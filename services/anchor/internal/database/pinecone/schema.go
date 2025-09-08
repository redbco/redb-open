package pinecone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of a Pinecone database and returns a UnifiedModel
func DiscoverSchema(client *PineconeClient) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Pinecone,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
		Namespaces:    make(map[string]unifiedmodel.Namespace),
	}

	// Get indexes
	indexNames, err := listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("error fetching indexes: %v", err)
	}

	// Get details for each index and convert to unified model
	for _, indexName := range indexNames {
		indexDetails, err := describeIndex(client, indexName)
		if err != nil {
			return nil, fmt.Errorf("error describing index %s: %v", indexName, err)
		}

		// Get statistics for the index
		indexStats, err := getIndexStats(client, indexName)
		if err != nil {
			// Continue even if we can't get stats
			indexStats = PineconeIndexInfo{
				Name:        indexName,
				VectorCount: 0,
				IndexSize:   0,
			}
		}

		// Combine details and stats
		indexInfo := PineconeIndexInfo{
			Name:           indexName,
			Dimension:      indexDetails.Dimension,
			Metric:         indexDetails.Metric,
			Pods:           indexDetails.Pods,
			Replicas:       indexDetails.Replicas,
			PodType:        indexDetails.PodType,
			Metadata:       indexDetails.Metadata,
			VectorCount:    indexStats.VectorCount,
			IndexSize:      indexStats.IndexSize,
			Namespaces:     indexStats.Namespaces,
			Shards:         indexDetails.Shards,
			Status:         indexDetails.Status,
			Environment:    client.Environment,
			Region:         indexDetails.Region,
			MetadataConfig: indexDetails.MetadataConfig,
		}

		// Convert to vector index directly
		vectorIndex := unifiedmodel.VectorIndex{
			Name:      indexInfo.Name,
			Dimension: indexInfo.Dimension,
			Metric:    indexInfo.Metric,
			Parameters: map[string]any{
				"pods":         indexInfo.Pods,
				"replicas":     indexInfo.Replicas,
				"pod_type":     indexInfo.PodType,
				"vector_count": indexInfo.VectorCount,
				"index_size":   indexInfo.IndexSize,
				"environment":  indexInfo.Environment,
				"region":       indexInfo.Region,
			},
		}
		um.VectorIndexes[indexName] = vectorIndex

		// Create namespaces for this index
		for _, namespace := range indexStats.Namespaces {
			um.Namespaces[namespace] = unifiedmodel.Namespace{
				Name: namespace,
			}
		}
	}

	// Get collections (if supported by the Pinecone version)
	collections, err := listCollections(client)
	if err == nil {
		for _, collName := range collections {
			_, err := describeCollection(client, collName)
			if err != nil {
				continue // Skip this collection if we can't get details
			}

			// Add collection to unified model
			um.Collections[collName] = unifiedmodel.Collection{
				Name: collName,
			}
		}
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(client *PineconeClient, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Create vector indexes from UnifiedModel
	for _, vectorIndex := range um.VectorIndexes {
		if err := createVectorIndexFromUnified(client, vectorIndex); err != nil {
			return fmt.Errorf("error creating vector index %s: %v", vectorIndex.Name, err)
		}
	}

	// Create collections from UnifiedModel (if any)
	for _, collection := range um.Collections {
		if err := createCollectionFromUnified(client, collection); err != nil {
			return fmt.Errorf("error creating collection %s: %v", collection.Name, err)
		}
	}

	return nil
}

// createIndex creates a Pinecone index with the specified parameters
func createIndex(client *PineconeClient, name string, dimension int, metric, podType string, pods int) error {
	// Create index request payload
	indexRequest := map[string]interface{}{
		"name":      name,
		"dimension": dimension,
		"metric":    metric,
		"pod_type":  podType,
		"pods":      pods,
	}

	// Convert to JSON
	jsonData, err := json.Marshal(indexRequest)
	if err != nil {
		return fmt.Errorf("error marshaling index request: %v", err)
	}

	// Make HTTP request to create index
	url := fmt.Sprintf("%s/databases", client.BaseURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", client.APIKey)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error creating index: %s (status: %d)", string(body), resp.StatusCode)
	}

	// Wait for index to be ready (Pinecone indexes take time to initialize)
	return waitForIndexReady(client, name)
}

// waitForIndexReady waits for a Pinecone index to become ready
func waitForIndexReady(client *PineconeClient, indexName string) error {
	maxAttempts := 30
	for i := 0; i < maxAttempts; i++ {
		status, err := getIndexStatus(client, indexName)
		if err != nil {
			return fmt.Errorf("error checking index status: %v", err)
		}

		if status == "Ready" {
			return nil
		}

		time.Sleep(10 * time.Second) // Wait 10 seconds between checks
	}

	return fmt.Errorf("index %s did not become ready within timeout", indexName)
}

// getIndexStatus gets the current status of a Pinecone index
func getIndexStatus(client *PineconeClient, indexName string) (string, error) {
	url := fmt.Sprintf("%s/databases/%s", client.BaseURL, indexName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error getting index status: %d", resp.StatusCode)
	}

	var indexInfo map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&indexInfo); err != nil {
		return "", fmt.Errorf("error decoding response: %v", err)
	}

	if status, ok := indexInfo["status"].(map[string]interface{}); ok {
		if state, ok := status["state"].(string); ok {
			return state, nil
		}
	}

	return "Unknown", nil
}

// createVectorIndexFromUnified creates a vector index from UnifiedModel VectorIndex
func createVectorIndexFromUnified(client *PineconeClient, vectorIndex unifiedmodel.VectorIndex) error {
	if vectorIndex.Name == "" {
		return fmt.Errorf("vector index name cannot be empty")
	}

	if vectorIndex.Dimension == 0 {
		return fmt.Errorf("vector index dimension must be specified")
	}

	// Extract Pinecone-specific parameters from Options
	metric := vectorIndex.Metric
	if metric == "" {
		metric = "cosine" // Default metric
	}

	podType := "p1.x1" // Default pod type
	pods := 1          // Default number of pods

	if vectorIndex.Parameters != nil {
		if pt, ok := vectorIndex.Parameters["pod_type"].(string); ok {
			podType = pt
		}
		if p, ok := vectorIndex.Parameters["pods"].(int); ok {
			pods = p
		}
	}

	// Create the index using Pinecone API
	return createIndex(client, vectorIndex.Name, vectorIndex.Dimension, metric, podType, pods)
}

// createCollectionFromUnified creates a collection from UnifiedModel Collection
func createCollectionFromUnified(client *PineconeClient, collection unifiedmodel.Collection) error {
	if collection.Name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	// For Pinecone, collections are typically created as indexes
	// We'll create a vector index with default parameters
	dimension := 1536 // Default dimension
	metric := "cosine"
	podType := "p1.x1"
	pods := 1

	// Extract parameters from collection options if available
	if collection.Options != nil {
		if d, ok := collection.Options["dimension"].(int); ok {
			dimension = d
		}
		if m, ok := collection.Options["metric"].(string); ok {
			metric = m
		}
		if pt, ok := collection.Options["pod_type"].(string); ok {
			podType = pt
		}
		if p, ok := collection.Options["pods"].(int); ok {
			pods = p
		}
	}

	// Create the index
	return createIndex(client, collection.Name, dimension, metric, podType, pods)
}
