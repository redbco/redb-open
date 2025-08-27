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
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

		// Convert to vector index
		vectorIndex := ConvertWeaviateClass(*details)
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

// CreateStructure creates database structures based on parameters
func CreateStructure(client *WeaviateClient, params common.StructureParams) error {
	// Weaviate doesn't support creating traditional database structures
	// Instead, we can create classes if needed
	return fmt.Errorf("structure creation is not supported for Weaviate. Use class creation instead")
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
