package milvus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// FetchData retrieves vectors from a specified collection
func FetchData(client *MilvusClient, collectionName string, limit int) ([]map[string]interface{}, error) {
	if collectionName == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	// Create query request
	queryRequest := MilvusQueryRequest{
		CollectionName: collectionName,
		OutputFields:   []string{"*"},
		Limit:          int64(limit),
	}

	jsonBody, err := json.Marshal(queryRequest)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	// Make request
	url := fmt.Sprintf("%s/query", client.BaseURL)

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
	var response MilvusQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert to map format
	result := make([]map[string]interface{}, 0, len(response.Data))
	result = append(result, response.Data...)

	return result, nil
}

// InsertData inserts vectors into a specified collection
func InsertData(client *MilvusClient, collectionName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if collectionName == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	// Prepare data for insertion
	insertData := make(map[string]interface{})
	insertData["collection_name"] = collectionName

	// Extract fields from data
	fields := make(map[string][]interface{})
	for _, item := range data {
		for key, value := range item {
			if fields[key] == nil {
				fields[key] = make([]interface{}, 0, len(data))
			}
			fields[key] = append(fields[key], value)
		}
	}

	insertData["fields_data"] = fields

	jsonBody, err := json.Marshal(insertData)
	if err != nil {
		return 0, fmt.Errorf("error marshaling request: %v", err)
	}

	// Make request
	url := fmt.Sprintf("%s/insert", client.BaseURL)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("insert failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		InsertCount int64 `json:"insert_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("error decoding response: %v", err)
	}

	return response.InsertCount, nil
}

// UpdateData updates existing vectors in a specified collection
func UpdateData(client *MilvusClient, collectionName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if collectionName == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	// Milvus doesn't support traditional updates, so we'll delete and re-insert
	// First, extract IDs to delete
	var idsToDelete []interface{}
	for _, item := range data {
		if id, ok := item["id"]; ok {
			idsToDelete = append(idsToDelete, id)
		}
	}

	// Delete existing vectors
	if len(idsToDelete) > 0 {
		deleteURL := fmt.Sprintf("%s/delete", client.BaseURL)
		deleteBody := map[string]interface{}{
			"collection_name": collectionName,
			"ids":             idsToDelete,
		}

		jsonBody, err := json.Marshal(deleteBody)
		if err != nil {
			return 0, fmt.Errorf("error marshaling delete request: %v", err)
		}

		req, err := http.NewRequest("POST", deleteURL, bytes.NewBuffer(jsonBody))
		if err != nil {
			return 0, fmt.Errorf("error creating delete request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		// Add authentication if provided
		if client.Username != "" && client.Password != "" {
			req.SetBasicAuth(client.Username, client.Password)
		}

		httpClient := &http.Client{Timeout: 30 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			return 0, fmt.Errorf("error executing delete request: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return 0, fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
		}
	}

	// Insert the updated vectors
	return InsertData(client, collectionName, data)
}

// UpsertData inserts or updates vectors based on unique constraints
func UpsertData(client *MilvusClient, collectionName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For Milvus, upsert is the same as insert since it will overwrite existing IDs
	return InsertData(client, collectionName, data)
}

// WipeDatabase removes all data from the Milvus database
func WipeDatabase(client *MilvusClient) error {
	// Get all collections
	collections, err := listCollections(client)
	if err != nil {
		return fmt.Errorf("error listing collections: %v", err)
	}

	// Delete each collection
	for _, collectionName := range collections {
		url := fmt.Sprintf("%s/collections/%s", client.BaseURL, collectionName)

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
			return fmt.Errorf("delete collection failed with status %d: %s", resp.StatusCode, string(body))
		}
	}

	return nil
}
