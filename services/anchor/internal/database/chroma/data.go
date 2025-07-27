package chroma

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// FetchData retrieves vectors from a specified collection
func FetchData(client *ChromaClient, collectionName string, limit int) ([]map[string]interface{}, error) {
	if collectionName == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	// Get all vectors from the collection
	url := fmt.Sprintf("%s/collections/%s/get", client.BaseURL, collectionName)

	// Create request body
	requestBody := map[string]interface{}{
		"include": []string{"embeddings", "metadatas", "documents"},
	}

	if limit > 0 {
		requestBody["limit"] = limit
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response struct {
		IDs        []string                 `json:"ids"`
		Embeddings [][]float32              `json:"embeddings"`
		Metadatas  []map[string]interface{} `json:"metadatas"`
		Documents  []string                 `json:"documents"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert to map format
	result := make([]map[string]interface{}, 0, len(response.IDs))
	for i, id := range response.IDs {
		vectorMap := map[string]interface{}{
			"id": id,
		}

		if i < len(response.Embeddings) {
			vectorMap["embedding"] = response.Embeddings[i]
		}

		if i < len(response.Metadatas) && response.Metadatas[i] != nil {
			for k, v := range response.Metadatas[i] {
				vectorMap[k] = v
			}
		}

		if i < len(response.Documents) {
			vectorMap["document"] = response.Documents[i]
		}

		result = append(result, vectorMap)
	}

	return result, nil
}

// InsertData inserts vectors into a specified collection
func InsertData(client *ChromaClient, collectionName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if collectionName == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	// Prepare vectors for insertion
	var ids []string
	var embeddings [][]float32
	var metadatas []map[string]interface{}
	var documents []string

	for _, item := range data {
		// Extract ID
		id, ok := item["id"].(string)
		if !ok {
			return 0, fmt.Errorf("each vector must have an 'id' field")
		}
		ids = append(ids, id)

		// Extract embedding
		if embedding, ok := item["embedding"].([]float32); ok {
			embeddings = append(embeddings, embedding)
		} else {
			return 0, fmt.Errorf("each vector must have an 'embedding' field")
		}

		// Extract metadata (optional)
		metadata := make(map[string]interface{})
		for k, v := range item {
			if k != "id" && k != "embedding" && k != "document" {
				metadata[k] = v
			}
		}
		if len(metadata) > 0 {
			metadatas = append(metadatas, metadata)
		} else {
			metadatas = append(metadatas, nil)
		}

		// Extract document (optional)
		if document, ok := item["document"].(string); ok {
			documents = append(documents, document)
		} else {
			documents = append(documents, "")
		}
	}

	// Create request body
	requestBody := map[string]interface{}{
		"ids":        ids,
		"embeddings": embeddings,
	}

	if len(metadatas) > 0 {
		requestBody["metadatas"] = metadatas
	}

	if len(documents) > 0 {
		requestBody["documents"] = documents
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return 0, fmt.Errorf("error marshaling request: %v", err)
	}

	// Make request
	url := fmt.Sprintf("%s/collections/%s/add", client.BaseURL, collectionName)

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

	return int64(len(ids)), nil
}

// UpdateData updates existing vectors in a specified collection
func UpdateData(client *ChromaClient, collectionName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if collectionName == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	// Chroma doesn't support traditional updates, so we'll delete and re-insert
	// First, extract IDs to delete
	var idsToDelete []string
	for _, item := range data {
		if id, ok := item["id"].(string); ok {
			idsToDelete = append(idsToDelete, id)
		}
	}

	// Delete existing vectors
	if len(idsToDelete) > 0 {
		deleteURL := fmt.Sprintf("%s/collections/%s/delete", client.BaseURL, collectionName)
		deleteBody := map[string]interface{}{
			"ids": idsToDelete,
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
func UpsertData(client *ChromaClient, collectionName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For Chroma, upsert is the same as insert since it will overwrite existing IDs
	return InsertData(client, collectionName, data)
}

// WipeDatabase removes all data from the Chroma database
func WipeDatabase(client *ChromaClient) error {
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
