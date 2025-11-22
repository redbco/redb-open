package milvus

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

// DiscoverSchema discovers the Milvus database schema and returns a UnifiedModel
func DiscoverSchema(client *MilvusClient) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Milvus,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
	}

	// Get all collections
	collections, err := listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	// Get details for each collection and convert to unified model
	for _, collectionName := range collections {
		details, err := describeCollection(client, collectionName)
		if err != nil {
			continue // Skip collections we can't describe
		}

		// Convert to Embedding (primary container for vector databases)
		var dimensions int
		for _, field := range details.Schema.Fields {
			if field.Type == "FloatVector" || field.Type == "BinaryVector" {
				if dim, ok := field.Params["dim"].(float64); ok {
					dimensions = int(dim)
				}
				break
			}
		}

		embedding := unifiedmodel.Embedding{
			Name:  details.Name,
			Model: "milvus", // Default model name
			Options: map[string]any{
				"shards_num": details.ShardsNum,
				"row_count":  details.RowCount,
				"size":       details.Size,
				"status":     details.Status,
				"dimension":  dimensions,
				"metric":     "L2", // Default metric for Milvus
			},
		}
		um.Embeddings[details.Name] = embedding

		// Keep VectorIndex for compatibility
		vectorIndex := unifiedmodel.VectorIndex{
			Name:      details.Name,
			Dimension: dimensions,
			Metric:    "L2", // Default metric for Milvus
			Parameters: map[string]any{
				"shards_num": details.ShardsNum,
				"row_count":  details.RowCount,
				"size":       details.Size,
				"status":     details.Status,
			},
		}
		um.VectorIndexes[details.Name] = vectorIndex

		// Also create a collection entry for compatibility
		um.Collections[details.Name] = unifiedmodel.Collection{
			Name: details.Name,
		}
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(client *MilvusClient, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Create collections from UnifiedModel
	for _, collection := range um.Collections {
		if err := createCollectionFromUnified(client, collection); err != nil {
			return fmt.Errorf("error creating collection %s: %v", collection.Name, err)
		}
	}

	// Create vector indexes from UnifiedModel
	for _, vectorIndex := range um.VectorIndexes {
		if err := createVectorIndexFromUnified(client, vectorIndex); err != nil {
			return fmt.Errorf("error creating vector index %s: %v", vectorIndex.Name, err)
		}
	}

	return nil
}

// CreateCollection creates a new collection in Milvus
func CreateCollection(client *MilvusClient, collectionName string, schema MilvusCollectionSchema) error {
	if collectionName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	// Check if collection already exists
	collections, err := listCollections(client)
	if err != nil {
		return fmt.Errorf("error listing collections: %v", err)
	}

	for _, existingCollection := range collections {
		if existingCollection == collectionName {
			return fmt.Errorf("collection %s already exists", collectionName)
		}
	}

	// Create collection
	url := fmt.Sprintf("%s/collections", client.BaseURL)

	requestBody := map[string]interface{}{
		"name":   collectionName,
		"schema": schema,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("error marshaling request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create collection failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DropCollection deletes a collection from Milvus
func DropCollection(client *MilvusClient, collectionName string) error {
	if collectionName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	url := fmt.Sprintf("%s/collections/%s", client.BaseURL, collectionName)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete collection failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// createCollectionFromUnified creates a collection from UnifiedModel Collection
func createCollectionFromUnified(client *MilvusClient, collection unifiedmodel.Collection) error {
	if collection.Name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	// Create a basic schema for the collection
	// Default to a simple vector collection with ID and vector fields
	dimension := 128 // Default dimension
	if collection.Options != nil {
		if d, ok := collection.Options["dimension"].(int); ok {
			dimension = d
		}
	}

	schema := MilvusCollectionSchema{
		Fields: []MilvusFieldInfo{
			{
				Name:        "id",
				Description: "Primary key field",
				Type:        "Int64",
				PrimaryKey:  true,
				AutoID:      true,
			},
			{
				Name:        "vector",
				Description: "Vector field",
				Type:        "FloatVector",
				Params: map[string]interface{}{
					"dim": dimension,
				},
			},
		},
	}

	// Add metadata fields if specified in collection options
	if collection.Options != nil {
		if fields, ok := collection.Options["fields"].([]interface{}); ok {
			for _, field := range fields {
				if fieldMap, ok := field.(map[string]interface{}); ok {
					if name, ok := fieldMap["name"].(string); ok {
						fieldSchema := MilvusFieldInfo{
							Name:        name,
							Description: fmt.Sprintf("Field %s", name),
							Type:        "VarChar",
							Params: map[string]interface{}{
								"max_length": 256,
							},
						}
						if fieldType, ok := fieldMap["type"].(string); ok {
							fieldSchema.Type = fieldType
						}
						schema.Fields = append(schema.Fields, fieldSchema)
					}
				}
			}
		}
	}

	return CreateCollection(client, collection.Name, schema)
}

// createVectorIndexFromUnified creates a vector index (collection) from UnifiedModel VectorIndex
func createVectorIndexFromUnified(client *MilvusClient, vectorIndex unifiedmodel.VectorIndex) error {
	if vectorIndex.Name == "" {
		return fmt.Errorf("vector index name cannot be empty")
	}

	if vectorIndex.Dimension == 0 {
		return fmt.Errorf("vector index dimension must be specified")
	}

	// Create a collection schema for the vector index
	schema := MilvusCollectionSchema{
		Fields: []MilvusFieldInfo{
			{
				Name:        "id",
				Description: "Primary key field",
				Type:        "Int64",
				PrimaryKey:  true,
				AutoID:      true,
			},
			{
				Name:        "vector",
				Description: "Vector field",
				Type:        "FloatVector",
				Params: map[string]interface{}{
					"dim": vectorIndex.Dimension,
				},
			},
		},
	}

	// Add metadata fields if specified in vector index parameters
	if vectorIndex.Parameters != nil {
		if fields, ok := vectorIndex.Parameters["fields"].([]interface{}); ok {
			for _, field := range fields {
				if fieldMap, ok := field.(map[string]interface{}); ok {
					if name, ok := fieldMap["name"].(string); ok {
						fieldSchema := MilvusFieldInfo{
							Name:        name,
							Description: fmt.Sprintf("Field %s", name),
							Type:        "VarChar",
							Params: map[string]interface{}{
								"max_length": 256,
							},
						}
						if fieldType, ok := fieldMap["type"].(string); ok {
							fieldSchema.Type = fieldType
						}
						schema.Fields = append(schema.Fields, fieldSchema)
					}
				}
			}
		}
	}

	return CreateCollection(client, vectorIndex.Name, schema)
}
