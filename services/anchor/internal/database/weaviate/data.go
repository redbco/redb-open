package weaviate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// FetchData retrieves objects from a specified class
func FetchData(client *WeaviateClient, className string, limit int) ([]map[string]interface{}, error) {
	if className == "" {
		return nil, fmt.Errorf("class name cannot be empty")
	}

	// Create query request
	queryRequest := WeaviateQueryRequest{
		Class:      className,
		Properties: []string{"*"},
		Limit:      limit,
	}

	jsonBody, err := json.Marshal(queryRequest)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	// Make request
	url := fmt.Sprintf("%s/graphql", client.BaseURL)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response WeaviateQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert to map format
	result := make([]map[string]interface{}, 0)
	if objects, ok := response.Data.Get[className]; ok {
		for _, obj := range objects {
			objMap := map[string]interface{}{
				"id": obj.ID,
			}

			// Add properties
			for k, v := range obj.Properties {
				objMap[k] = v
			}

			// Add vector if available
			if obj.Vector != nil {
				objMap["vector"] = obj.Vector
			}

			// Add additional data if available
			if obj.Additional != nil {
				for k, v := range obj.Additional {
					objMap[k] = v
				}
			}

			result = append(result, objMap)
		}
	}

	return result, nil
}

// InsertData inserts objects into a specified class
func InsertData(client *WeaviateClient, className string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if className == "" {
		return 0, fmt.Errorf("class name cannot be empty")
	}

	// Prepare objects for insertion
	var objects []WeaviateObject
	for _, item := range data {
		obj := WeaviateObject{
			Class:      className,
			Properties: make(map[string]interface{}),
		}

		// Extract ID if provided
		if id, ok := item["id"].(string); ok {
			obj.ID = id
		}

		// Extract vector if provided
		if vector, ok := item["vector"].([]float32); ok {
			obj.Vector = vector
		}

		// Extract properties
		for key, value := range item {
			if key != "id" && key != "vector" && key != "class" {
				obj.Properties[key] = value
			}
		}

		objects = append(objects, obj)
	}

	// Insert objects one by one (Weaviate doesn't support batch insert via REST API)
	var insertedCount int64
	for _, obj := range objects {
		jsonBody, err := json.Marshal(obj)
		if err != nil {
			return insertedCount, fmt.Errorf("error marshaling object: %v", err)
		}

		url := fmt.Sprintf("%s/objects", client.BaseURL)

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
		if err != nil {
			return insertedCount, fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		// Add authentication if provided
		if client.Username != "" && client.Password != "" {
			req.SetBasicAuth(client.Username, client.Password)
		}

		httpClient := &http.Client{Timeout: 30 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			return insertedCount, fmt.Errorf("error executing request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			return insertedCount, fmt.Errorf("insert failed with status %d: %s", resp.StatusCode, string(body))
		}

		insertedCount++
	}

	return insertedCount, nil
}

// UpdateData updates existing objects in a specified class
func UpdateData(client *WeaviateClient, className string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if className == "" {
		return 0, fmt.Errorf("class name cannot be empty")
	}

	// Weaviate doesn't support traditional updates, so we'll delete and re-insert
	// First, extract IDs to delete
	var idsToDelete []string
	for _, item := range data {
		if id, ok := item["id"].(string); ok {
			idsToDelete = append(idsToDelete, id)
		}
	}

	// Delete existing objects
	var deletedCount int64
	for _, id := range idsToDelete {
		url := fmt.Sprintf("%s/objects/%s", client.BaseURL, id)

		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return deletedCount, fmt.Errorf("error creating delete request: %v", err)
		}

		// Add authentication if provided
		if client.Username != "" && client.Password != "" {
			req.SetBasicAuth(client.Username, client.Password)
		}

		httpClient := &http.Client{Timeout: 30 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			return deletedCount, fmt.Errorf("error executing delete request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return deletedCount, fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
		}

		deletedCount++
	}

	// Insert the updated objects
	insertedCount, err := InsertData(client, className, data)
	if err != nil {
		return deletedCount, err
	}

	return insertedCount, nil
}

// UpsertData inserts or updates objects based on unique constraints
func UpsertData(client *WeaviateClient, className string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For Weaviate, upsert is the same as insert since it will overwrite existing IDs
	return InsertData(client, className, data)
}

// WipeDatabase removes all data from the Weaviate database
func WipeDatabase(client *WeaviateClient) error {
	// Get all classes
	classes, err := listClasses(client)
	if err != nil {
		return fmt.Errorf("error listing classes: %v", err)
	}

	// Delete each class
	for _, className := range classes {
		url := fmt.Sprintf("%s/schema/%s", client.BaseURL, className)

		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return fmt.Errorf("error creating delete request: %v", err)
		}

		// Add authentication if provided
		if client.Username != "" && client.Password != "" {
			req.SetBasicAuth(client.Username, client.Password)
		}

		httpClient := &http.Client{Timeout: 30 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("error executing delete request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("delete class failed with status %d: %s", resp.StatusCode, string(body))
		}
	}

	return nil
}
