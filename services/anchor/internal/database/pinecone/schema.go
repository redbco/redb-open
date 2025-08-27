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
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

		// Convert to vector index
		vectorIndex := ConvertPineconeIndex(indexInfo)
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

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(client *PineconeClient, params common.StructureParams) error {
	// For Pinecone, we'll create indexes based on the provided parameters
	// Extract Pinecone-specific parameters from the generic structure

	// Check if we have any indexes to create
	if len(params.Tables) == 0 {
		return fmt.Errorf("no indexes specified for creation")
	}

	// In Pinecone, we'll treat tables as indexes
	for _, table := range params.Tables {
		// Extract index parameters from table info
		dimension := 0
		metric := "cosine"
		podType := "p1.x1"
		pods := 1

		// Look for dimension in table metadata or columns
		for _, column := range table.Columns {
			if column.Name == "dimension" && column.ColumnDefault != nil {
				// Try to parse dimension from column default
				fmt.Sscanf(*column.ColumnDefault, "%d", &dimension)
			}
		}

		// If dimension is still 0, use a default value
		if dimension == 0 {
			dimension = 1536 // Common dimension for embeddings
		}

		// Create the index
		if err := createIndex(client, table.Name, dimension, metric, podType, pods); err != nil {
			return fmt.Errorf("error creating index %s: %v", table.Name, err)
		}
	}

	return nil
}

// createIndex creates a new Pinecone index
func createIndex(client *PineconeClient, name string, dimension int, metric string, podType string, pods int) error {
	// Check if index already exists
	indexes, err := listIndexes(client)
	if err != nil {
		return fmt.Errorf("error checking existing indexes: %v", err)
	}

	for _, idx := range indexes {
		if idx == name {
			return fmt.Errorf("index %s already exists", name)
		}
	}

	// Create index request
	createReq := struct {
		Name      string `json:"name"`
		Dimension int    `json:"dimension"`
		Metric    string `json:"metric"`
		PodType   string `json:"pod_type"`
		Pods      int    `json:"pods"`
	}{
		Name:      name,
		Dimension: dimension,
		Metric:    metric,
		PodType:   podType,
		Pods:      pods,
	}

	// Convert request to JSON
	createJSON, err := json.Marshal(createReq)
	if err != nil {
		return fmt.Errorf("error marshaling create request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/databases", client.BaseURL), bytes.NewBuffer(createJSON))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing create index: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create index failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Wait for index to be ready
	return waitForIndexReady(client, name)
}

// waitForIndexReady waits for an index to be ready
func waitForIndexReady(client *PineconeClient, indexName string) error {
	maxRetries := 30
	retryInterval := 10 * time.Second

	for i := 0; i < maxRetries; i++ {
		indexDetails, err := describeIndex(client, indexName)
		if err != nil {
			// If we can't describe the index, it might not be created yet
			time.Sleep(retryInterval)
			continue
		}

		if indexDetails.Status == "Ready" {
			return nil
		}

		time.Sleep(retryInterval)
	}

	return fmt.Errorf("timeout waiting for index %s to be ready", indexName)
}

// createCollection creates a new Pinecone collection
func CreateCollection(client *PineconeClient, name string, sourceIndex string) error {
	// Check if collection already exists
	collections, err := listCollections(client)
	if err == nil {
		for _, coll := range collections {
			if coll == name {
				return fmt.Errorf("collection %s already exists", name)
			}
		}
	}

	// Create collection request
	createReq := struct {
		Name        string `json:"name"`
		SourceIndex string `json:"source"`
	}{
		Name:        name,
		SourceIndex: sourceIndex,
	}

	// Convert request to JSON
	createJSON, err := json.Marshal(createReq)
	if err != nil {
		return fmt.Errorf("error marshaling create collection request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/collections", client.BaseURL), bytes.NewBuffer(createJSON))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing create collection: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create collection failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// deleteIndex deletes a Pinecone index
func deleteIndex(client *PineconeClient, indexName string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/databases/%s", client.BaseURL, indexName), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing delete index: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete index failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// deleteCollection deletes a Pinecone collection
func deleteCollection(client *PineconeClient, collectionName string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/collections/%s", client.BaseURL, collectionName), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing delete collection: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete collection failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// configureIndex updates the configuration of a Pinecone index
func configureIndex(client *PineconeClient, indexName string, pods int, replicas int, podType string) error {
	// Create configure request
	configReq := struct {
		Replicas int    `json:"replicas,omitempty"`
		PodType  string `json:"pod_type,omitempty"`
	}{
		Replicas: replicas,
		PodType:  podType,
	}

	// Convert request to JSON
	configJSON, err := json.Marshal(configReq)
	if err != nil {
		return fmt.Errorf("error marshaling configure request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("PATCH", fmt.Sprintf("%s/databases/%s", client.BaseURL, indexName), bytes.NewBuffer(configJSON))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing configure index: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("configure index failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// These functions are included for compatibility with the PostgreSQL interface
// but they're not fully implemented for Pinecone

// ConvertToCommonSchema converts a Pinecone UnifiedModel to a common.SchemaInfo
// This is useful for compatibility with systems that expect the common schema format
func ConvertToCommonSchema(um *unifiedmodel.UnifiedModel) *common.SchemaInfo {
	commonSchema := &common.SchemaInfo{
		SchemaType: "pinecone",
		Tables:     make([]common.TableInfo, 0, len(um.VectorIndexes)),
	}

	// Convert indexes to tables
	for _, index := range um.VectorIndexes {
		tableInfo := common.TableInfo{
			Name:      index.Name,
			Schema:    "default",
			TableType: "pinecone.index",
			Columns: []common.ColumnInfo{
				{
					Name:         "id",
					DataType:     "string",
					IsPrimaryKey: true,
					IsNullable:   false,
				},
				{
					Name:       "values",
					DataType:   "vector",
					IsNullable: false,
				},
				{
					Name:       "metadata",
					DataType:   "json",
					IsNullable: true,
				},
			},
			PrimaryKey: []string{"id"},
		}

		// Add dimension as a column
		dimensionColumn := common.ColumnInfo{
			Name:       "dimension",
			DataType:   "integer",
			IsNullable: false,
		}
		dimensionDefault := fmt.Sprintf("%d", index.Dimension)
		dimensionColumn.ColumnDefault = &dimensionDefault
		tableInfo.Columns = append(tableInfo.Columns, dimensionColumn)

		commonSchema.Tables = append(commonSchema.Tables, tableInfo)
	}

	return commonSchema
}
