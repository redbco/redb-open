package pinecone

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// PineconeReplicationSourceDetails contains information about a replication source
type PineconeReplicationSourceDetails struct {
	ReplicationID string `json:"replication_id"`
	IndexName     string `json:"index_name"`
	Namespace     string `json:"namespace"`
	DatabaseID    string `json:"database_id"`
	PollInterval  int    `json:"poll_interval"` // in seconds
}

// PineconeReplicationChange represents a change in the vector database
type PineconeReplicationChange struct {
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}

// replicationState tracks the state of replication for each source
var replicationState = struct {
	sync.RWMutex
	sources map[string]*replicationSourceState
}{
	sources: make(map[string]*replicationSourceState),
}

type replicationSourceState struct {
	client       *PineconeClient
	details      *PineconeReplicationSourceDetails
	eventHandler func(map[string]interface{})
	lastVectors  map[string]PineconeVector // Map of vector ID to vector data
	stopChan     chan struct{}
	running      bool
}

// CreateReplicationSource sets up a replication source for Pinecone
func CreateReplicationSource(client *PineconeClient, indexName string, namespace string, databaseID string, eventHandler func(map[string]interface{})) (*PineconeReplicationSourceDetails, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	// Verify that the index exists
	indexes, err := listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("error listing indexes: %v", err)
	}

	indexExists := false
	for _, idx := range indexes {
		if idx == indexName {
			indexExists = true
			break
		}
	}

	if !indexExists {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	// Generate a unique ID for this replication source
	replicationID := fmt.Sprintf("pinecone_repl_%s_%s", databaseID, common.GenerateUniqueID())

	details := &PineconeReplicationSourceDetails{
		ReplicationID: replicationID,
		IndexName:     indexName,
		Namespace:     namespace,
		DatabaseID:    databaseID,
		PollInterval:  10, // Default to 10 seconds
	}

	// Start listening for changes
	go startReplication(client, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(client *PineconeClient, details *PineconeReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the index still exists
	indexes, err := listIndexes(client)
	if err != nil {
		return fmt.Errorf("error listing indexes: %v", err)
	}

	indexExists := false
	for _, idx := range indexes {
		if idx == details.IndexName {
			indexExists = true
			break
		}
	}

	if !indexExists {
		return fmt.Errorf("index %s does not exist", details.IndexName)
	}

	// Start listening for changes
	go startReplication(client, details, eventHandler)

	return nil
}

// startReplication begins the replication process
func startReplication(client *PineconeClient, details *PineconeReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	replicationState.Lock()
	defer replicationState.Unlock()

	// Check if this source is already running
	if state, exists := replicationState.sources[details.ReplicationID]; exists && state.running {
		// Stop the existing replication
		state.stopChan <- struct{}{}
	}

	// Create a new state for this source
	state := &replicationSourceState{
		client:       client,
		details:      details,
		eventHandler: eventHandler,
		lastVectors:  make(map[string]PineconeVector),
		stopChan:     make(chan struct{}),
		running:      true,
	}

	replicationState.sources[details.ReplicationID] = state

	// Start the replication loop in a separate goroutine
	go replicationLoop(state)
}

// replicationLoop continuously polls for changes
func replicationLoop(state *replicationSourceState) {
	// Initial fetch to establish baseline
	initialVectors, err := fetchAllVectors(state.client, state.details.IndexName, state.details.Namespace)
	if err != nil {
		log.Printf("Error fetching initial vectors: %v", err)
	} else {
		// Store the initial state
		for _, vector := range initialVectors {
			state.lastVectors[vector.ID] = vector
		}
	}

	ticker := time.NewTicker(time.Duration(state.details.PollInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-state.stopChan:
			// Mark as not running
			replicationState.Lock()
			state.running = false
			replicationState.Unlock()
			return
		case <-ticker.C:
			// Fetch current vectors
			currentVectors, err := fetchAllVectors(state.client, state.details.IndexName, state.details.Namespace)
			if err != nil {
				log.Printf("Error fetching vectors: %v", err)
				continue
			}

			// Detect changes
			changes := detectChanges(state.lastVectors, currentVectors)

			// Process changes
			for _, change := range changes {
				event := map[string]interface{}{
					"table":     state.details.IndexName,
					"namespace": state.details.Namespace,
					"operation": change.Operation,
					"data":      change.Data,
				}
				if change.OldData != nil {
					event["old_data"] = change.OldData
				}
				state.eventHandler(event)
			}

			// Update the last known state
			state.lastVectors = make(map[string]PineconeVector)
			for _, vector := range currentVectors {
				state.lastVectors[vector.ID] = vector
			}
		}
	}
}

// fetchAllVectors retrieves all vectors from an index and namespace
func fetchAllVectors(client *PineconeClient, indexName string, namespace string) (map[string]PineconeVector, error) {
	// Construct the API URL for the specific index
	indexHost := fmt.Sprintf(pineconeAPIURL, indexName, client.ProjectID, client.Environment)

	// Get index stats to determine vector count
	stats, err := getIndexStats(client, indexName)
	if err != nil {
		return nil, fmt.Errorf("error getting index stats: %v", err)
	}

	// Determine the batch size and number of batches
	batchSize := 1000 // Pinecone's maximum fetch size
	totalVectors := stats.VectorCount

	// Create a map to store all vectors
	allVectors := make(map[string]PineconeVector)

	// If there are no vectors, return an empty map
	if totalVectors == 0 {
		return allVectors, nil
	}

	// We need to fetch vectors in batches using multiple queries
	// Since Pinecone doesn't have a direct "list all vectors" API,
	// we'll use a combination of techniques:

	// 1. First, try to get a list of vector IDs
	vectorIDs, err := fetchVectorIDs(client, indexName, namespace)
	if err != nil {
		// If we can't get IDs, we'll use a different approach
		log.Printf("Warning: couldn't fetch vector IDs: %v", err)
	} else {
		// 2. Fetch vectors by ID in batches
		for i := 0; i < len(vectorIDs); i += batchSize {
			end := i + batchSize
			if end > len(vectorIDs) {
				end = len(vectorIDs)
			}

			batchIDs := vectorIDs[i:end]
			vectors, err := fetchVectorsByIDs(client, indexHost, namespace, batchIDs)
			if err != nil {
				return nil, fmt.Errorf("error fetching vectors batch: %v", err)
			}

			// Add vectors to the result map
			for _, vector := range vectors {
				allVectors[vector.ID] = vector
			}
		}

		return allVectors, nil
	}

	// Fallback approach: use queries with filters to get all vectors
	// This is less efficient but works when we can't get vector IDs directly

	// We'll use a dummy vector for querying
	dummyVector := make([]float32, stats.Dimension)

	// Fetch vectors in batches
	var cursor string
	for {
		vectors, nextCursor, err := fetchVectorsBatch(client, indexHost, namespace, dummyVector, batchSize, cursor)
		if err != nil {
			return nil, fmt.Errorf("error fetching vectors batch: %v", err)
		}

		// Add vectors to the result map
		for _, vector := range vectors {
			allVectors[vector.ID] = vector
		}

		// If there's no next cursor, we're done
		if nextCursor == "" {
			break
		}

		cursor = nextCursor
	}

	return allVectors, nil
}

// fetchVectorIDs retrieves all vector IDs from an index and namespace
func fetchVectorIDs(client *PineconeClient, indexName string, namespace string) ([]string, error) {
	// This is a simplified implementation - Pinecone doesn't have a direct API for this
	// In a real implementation, you might need to use a custom solution or store IDs separately

	// For now, we'll return an error to force using the fallback approach
	return nil, fmt.Errorf("direct ID fetching not implemented")
}

// fetchVectorsByIDs retrieves vectors by their IDs
func fetchVectorsByIDs(client *PineconeClient, indexHost string, namespace string, ids []string) ([]PineconeVector, error) {
	// Create fetch request
	fetchReq := struct {
		IDs       []string `json:"ids"`
		Namespace string   `json:"namespace,omitempty"`
	}{
		IDs:       ids,
		Namespace: namespace,
	}

	// Convert request to JSON
	fetchJSON, err := json.Marshal(fetchReq)
	if err != nil {
		return nil, fmt.Errorf("error marshaling fetch request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/vectors/fetch", indexHost), bytes.NewBuffer(fetchJSON))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing fetch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetch failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var fetchResult struct {
		Vectors map[string]struct {
			ID       string                 `json:"id"`
			Values   []float32              `json:"values"`
			Metadata map[string]interface{} `json:"metadata,omitempty"`
		} `json:"vectors"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&fetchResult); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert to vector array
	vectors := make([]PineconeVector, 0, len(fetchResult.Vectors))
	for _, v := range fetchResult.Vectors {
		vectors = append(vectors, PineconeVector{
			ID:       v.ID,
			Values:   v.Values,
			Metadata: v.Metadata,
		})
	}

	return vectors, nil
}

// fetchVectorsBatch retrieves a batch of vectors using query
func fetchVectorsBatch(client *PineconeClient, indexHost string, namespace string, queryVector []float32, limit int, cursor string) ([]PineconeVector, string, error) {
	// Create query request
	queryReq := struct {
		Namespace       string    `json:"namespace,omitempty"`
		TopK            int       `json:"topK"`
		Vector          []float32 `json:"vector"`
		IncludeValues   bool      `json:"includeValues"`
		IncludeMetadata bool      `json:"includeMetadata"`
		Cursor          string    `json:"cursor,omitempty"`
	}{
		Namespace:       namespace,
		TopK:            limit,
		Vector:          queryVector,
		IncludeValues:   true,
		IncludeMetadata: true,
		Cursor:          cursor,
	}

	// Convert request to JSON
	queryJSON, err := json.Marshal(queryReq)
	if err != nil {
		return nil, "", fmt.Errorf("error marshaling query request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", indexHost), bytes.NewBuffer(queryJSON))
	if err != nil {
		return nil, "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("error executing query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var queryResult struct {
		Matches []struct {
			ID       string                 `json:"id"`
			Score    float32                `json:"score"`
			Values   []float32              `json:"values,omitempty"`
			Metadata map[string]interface{} `json:"metadata,omitempty"`
		} `json:"matches"`
		Namespace string `json:"namespace,omitempty"`
		Cursor    string `json:"cursor,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&queryResult); err != nil {
		return nil, "", fmt.Errorf("error decoding response: %v", err)
	}

	// Convert to vector array
	vectors := make([]PineconeVector, 0, len(queryResult.Matches))
	for _, match := range queryResult.Matches {
		vectors = append(vectors, PineconeVector{
			ID:       match.ID,
			Values:   match.Values,
			Metadata: match.Metadata,
		})
	}

	return vectors, queryResult.Cursor, nil
}

// detectChanges identifies differences between two sets of vectors
func detectChanges(oldVectors, newVectors map[string]PineconeVector) []PineconeReplicationChange {
	var changes []PineconeReplicationChange

	// Check for updates and deletions
	for id, oldVector := range oldVectors {
		if newVector, exists := newVectors[id]; exists {
			// Vector still exists, check if it was updated
			if !vectorsEqual(oldVector, newVector) {
				// Vector was updated
				changes = append(changes, PineconeReplicationChange{
					Operation: "UPDATE",
					Data:      vectorToMap(newVector),
					OldData:   vectorToMap(oldVector),
				})
			}
		} else {
			// Vector was deleted
			changes = append(changes, PineconeReplicationChange{
				Operation: "DELETE",
				OldData:   vectorToMap(oldVector),
			})
		}
	}

	// Check for insertions
	for id, newVector := range newVectors {
		if _, exists := oldVectors[id]; !exists {
			// Vector was inserted
			changes = append(changes, PineconeReplicationChange{
				Operation: "INSERT",
				Data:      vectorToMap(newVector),
			})
		}
	}

	return changes
}

// vectorsEqual checks if two vectors are identical
func vectorsEqual(a, b PineconeVector) bool {
	// Check ID
	if a.ID != b.ID {
		return false
	}

	// Check values
	if len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Values {
		if a.Values[i] != b.Values[i] {
			return false
		}
	}

	// Check metadata
	if len(a.Metadata) != len(b.Metadata) {
		return false
	}
	for k, v1 := range a.Metadata {
		if v2, ok := b.Metadata[k]; !ok || fmt.Sprintf("%v", v1) != fmt.Sprintf("%v", v2) {
			return false
		}
	}

	return true
}

// vectorToMap converts a PineconeVector to a map
func vectorToMap(vector PineconeVector) map[string]interface{} {
	result := map[string]interface{}{
		"id":     vector.ID,
		"values": vector.Values,
	}

	// Add metadata
	for k, v := range vector.Metadata {
		result[k] = v
	}

	return result
}

// StopReplication stops the replication process for a specific source
func StopReplication(replicationID string) error {
	replicationState.RLock()
	state, exists := replicationState.sources[replicationID]
	replicationState.RUnlock()

	if !exists || !state.running {
		return fmt.Errorf("replication %s is not running", replicationID)
	}

	// Signal the replication loop to stop
	state.stopChan <- struct{}{}

	return nil
}

// These functions are included for compatibility with the PostgreSQL interface

// For compatibility with the PostgreSQL version
func CreateReplicationSourceCompat(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*PineconeReplicationSourceDetails, error) {
	return nil, fmt.Errorf("not implemented for Pinecone - use CreateReplicationSource instead")
}

// For compatibility with the PostgreSQL version
func ReconnectToReplicationSourceCompat(db *sql.DB, details *PineconeReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	return fmt.Errorf("not implemented for Pinecone - use ReconnectToReplicationSource instead")
}
