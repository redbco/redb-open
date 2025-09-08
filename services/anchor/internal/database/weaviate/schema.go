package weaviate

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

// DiscoverSchema discovers the Weaviate database schema and returns a UnifiedModel
func DiscoverSchema(client *WeaviateClient) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Weaviate,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
		Types:         make(map[string]unifiedmodel.Type),
	}

	// Get all classes
	classes, err := listClasses(client)
	if err != nil {
		return nil, fmt.Errorf("error listing classes: %v", err)
	}

	// Get details for each class and convert to unified model
	for _, className := range classes {
		details, err := describeClass(client, className)
		if err != nil {
			continue // Skip classes we can't describe
		}

		// Convert to vector index directly
		vectorIndex := unifiedmodel.VectorIndex{
			Name:      details.Class,
			Dimension: 0,        // Weaviate doesn't expose vector dimensions directly
			Metric:    "cosine", // Default metric for Weaviate
			Parameters: map[string]any{
				"vectorizer":            details.Vectorizer,
				"object_count":          details.ObjectCount,
				"module_config":         details.ModuleConfig,
				"sharding_config":       details.ShardingConfig,
				"replication_config":    details.ReplicationConfig,
				"multi_tenancy_config":  details.MultiTenancyConfig,
				"inverted_index_config": details.InvertedIndexConfig,
			},
		}
		um.VectorIndexes[className] = vectorIndex

		// Also create a collection entry for compatibility
		um.Collections[className] = unifiedmodel.Collection{
			Name: className,
		}

		// Create types for properties
		for _, prop := range details.Properties {
			for _, dataType := range prop.DataType {
				um.Types[dataType] = unifiedmodel.Type{
					Name:     dataType,
					Category: "property",
				}
			}
		}
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(client *WeaviateClient, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Create collections (classes) from UnifiedModel
	for _, collection := range um.Collections {
		if err := createCollectionFromUnified(client, collection); err != nil {
			return fmt.Errorf("error creating collection %s: %v", collection.Name, err)
		}
	}

	// Create vector indexes (classes) from UnifiedModel
	for _, vectorIndex := range um.VectorIndexes {
		if err := createVectorIndexFromUnified(client, vectorIndex); err != nil {
			return fmt.Errorf("error creating vector index %s: %v", vectorIndex.Name, err)
		}
	}

	return nil
}

// CreateClass creates a new class in Weaviate
func CreateClass(client *WeaviateClient, classInfo WeaviateClassInfo) error {
	if classInfo.Class == "" {
		return fmt.Errorf("class name cannot be empty")
	}

	// Check if class already exists
	classes, err := listClasses(client)
	if err != nil {
		return fmt.Errorf("error listing classes: %v", err)
	}

	for _, existingClass := range classes {
		if existingClass == classInfo.Class {
			return fmt.Errorf("class %s already exists", classInfo.Class)
		}
	}

	// Create class
	url := fmt.Sprintf("%s/schema", client.BaseURL)

	jsonBody, err := json.Marshal(classInfo)
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
		return fmt.Errorf("create class failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DropClass deletes a class from Weaviate
func DropClass(client *WeaviateClient, className string) error {
	if className == "" {
		return fmt.Errorf("class name cannot be empty")
	}

	url := fmt.Sprintf("%s/schema/%s", client.BaseURL, className)

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
		return fmt.Errorf("delete class failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// createCollectionFromUnified creates a collection (class) from UnifiedModel Collection
func createCollectionFromUnified(client *WeaviateClient, collection unifiedmodel.Collection) error {
	if collection.Name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	// Create a basic class schema for the collection
	classInfo := WeaviateClassInfo{
		Class:       collection.Name,
		Description: "Collection created from UnifiedModel",
		Properties:  []WeaviatePropertyInfo{},
		Vectorizer:  "none", // Default vectorizer
	}

	// Add properties if specified in collection options
	if collection.Options != nil {
		if properties, ok := collection.Options["properties"].([]interface{}); ok {
			for _, prop := range properties {
				if propMap, ok := prop.(map[string]interface{}); ok {
					if name, ok := propMap["name"].(string); ok {
						property := WeaviatePropertyInfo{
							Name:        name,
							DataType:    []string{"text"}, // Default data type
							Description: fmt.Sprintf("Property %s", name),
						}
						if dataType, ok := propMap["dataType"].(string); ok {
							property.DataType = []string{dataType}
						}
						classInfo.Properties = append(classInfo.Properties, property)
					}
				}
			}
		}
		if vectorizer, ok := collection.Options["vectorizer"].(string); ok {
			classInfo.Vectorizer = vectorizer
		}
	}

	return CreateClass(client, classInfo)
}

// createVectorIndexFromUnified creates a vector index (class) from UnifiedModel VectorIndex
func createVectorIndexFromUnified(client *WeaviateClient, vectorIndex unifiedmodel.VectorIndex) error {
	if vectorIndex.Name == "" {
		return fmt.Errorf("vector index name cannot be empty")
	}

	// Create a class schema for the vector index
	classInfo := WeaviateClassInfo{
		Class:       vectorIndex.Name,
		Description: "Vector index created from UnifiedModel",
		Properties: []WeaviatePropertyInfo{
			{
				Name:        "content",
				DataType:    []string{"text"},
				Description: "Content field for vector index",
			},
		},
		Vectorizer: "text2vec-contextionary", // Default vectorizer for vector indexes
	}

	// Add custom properties if specified in vector index parameters
	if vectorIndex.Parameters != nil {
		if properties, ok := vectorIndex.Parameters["properties"].([]interface{}); ok {
			classInfo.Properties = []WeaviatePropertyInfo{} // Reset default properties
			for _, prop := range properties {
				if propMap, ok := prop.(map[string]interface{}); ok {
					if name, ok := propMap["name"].(string); ok {
						property := WeaviatePropertyInfo{
							Name:        name,
							DataType:    []string{"text"}, // Default data type
							Description: fmt.Sprintf("Property %s", name),
						}
						if dataType, ok := propMap["dataType"].(string); ok {
							property.DataType = []string{dataType}
						}
						classInfo.Properties = append(classInfo.Properties, property)
					}
				}
			}
		}
		if vectorizer, ok := vectorIndex.Parameters["vectorizer"].(string); ok {
			classInfo.Vectorizer = vectorizer
		}
		if moduleConfig, ok := vectorIndex.Parameters["moduleConfig"].(map[string]interface{}); ok {
			classInfo.ModuleConfig = moduleConfig
		}
	}

	return CreateClass(client, classInfo)
}
