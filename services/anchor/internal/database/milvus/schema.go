package milvus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema discovers the Milvus database schema
func DiscoverSchema(client *MilvusClient) (*MilvusSchema, error) {
	// Get all collections
	collections, err := listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	// Get details for each collection
	collectionDetails := make([]MilvusCollectionInfo, 0, len(collections))
	for _, collectionName := range collections {
		details, err := describeCollection(client, collectionName)
		if err != nil {
			continue // Skip collections we can't describe
		}
		collectionDetails = append(collectionDetails, *details)
	}

	return &MilvusSchema{
		Collections: collectionDetails,
	}, nil
}

// CreateStructure creates database structures based on parameters
func CreateStructure(client *MilvusClient, params common.StructureParams) error {
	// Milvus doesn't support creating traditional database structures
	// Instead, we can create collections if needed
	return fmt.Errorf("structure creation is not supported for Milvus. Use collection creation instead")
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
