package pinecone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FetchData retrieves vectors from a specified index and namespace
func FetchData(client *PineconeClient, indexName string, namespace string, limit int) ([]map[string]interface{}, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	// Construct the API URL for the specific index
	indexHost := fmt.Sprintf(pineconeAPIURL, indexName, client.ProjectID, client.Environment)

	// Create a query to fetch vectors
	// Since Pinecone doesn't have a direct "list all vectors" API,
	// we'll use a query with a high topK value to get as many vectors as possible
	queryReq := PineconeQueryRequest{
		TopK:            limit,
		IncludeValues:   true,
		IncludeMetadata: true,
	}

	if namespace != "" {
		queryReq.Namespace = namespace
	}

	// Convert query to JSON
	queryJSON, err := json.Marshal(queryReq)
	if err != nil {
		return nil, fmt.Errorf("error marshaling query: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/query", indexHost), bytes.NewBuffer(queryJSON))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var queryResult PineconeQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&queryResult); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Convert matches to map format
	result := make([]map[string]interface{}, 0, len(queryResult.Matches))
	for _, match := range queryResult.Matches {
		// Create a map with vector data
		vectorMap := map[string]interface{}{
			"id":     match.ID,
			"score":  match.Score,
			"values": match.Values,
		}

		// Add metadata if available
		if match.Metadata != nil {
			for k, v := range match.Metadata {
				vectorMap[k] = v
			}
		}

		result = append(result, vectorMap)
	}

	return result, nil
}

// InsertData inserts vectors into a specified index and namespace
func InsertData(client *PineconeClient, indexName string, namespace string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if indexName == "" {
		return 0, fmt.Errorf("index name cannot be empty")
	}

	// Construct the API URL for the specific index
	indexHost := fmt.Sprintf(pineconeAPIURL, indexName, client.ProjectID, client.Environment)

	// Convert data to Pinecone vectors
	vectors := make([]PineconeVector, 0, len(data))
	for _, item := range data {
		// Extract ID
		id, ok := item["id"].(string)
		if !ok {
			return 0, fmt.Errorf("vector ID must be a string")
		}

		// Extract values
		valuesRaw, ok := item["values"]
		if !ok {
			return 0, fmt.Errorf("vector values are required")
		}

		// Convert values to []float32
		var values []float32
		switch v := valuesRaw.(type) {
		case []float32:
			values = v
		case []float64:
			values = make([]float32, len(v))
			for i, val := range v {
				values[i] = float32(val)
			}
		case []interface{}:
			values = make([]float32, len(v))
			for i, val := range v {
				switch fv := val.(type) {
				case float64:
					values[i] = float32(fv)
				case float32:
					values[i] = fv
				default:
					return 0, fmt.Errorf("vector values must be numeric")
				}
			}
		default:
			return 0, fmt.Errorf("unsupported vector values format")
		}

		// Extract metadata (all fields except id and values)
		metadata := make(map[string]interface{})
		for k, v := range item {
			if k != "id" && k != "values" {
				metadata[k] = v
			}
		}

		vectors = append(vectors, PineconeVector{
			ID:       id,
			Values:   values,
			Metadata: metadata,
		})
	}

	// Create upsert request
	upsertReq := struct {
		Vectors   []PineconeVector `json:"vectors"`
		Namespace string           `json:"namespace,omitempty"`
	}{
		Vectors:   vectors,
		Namespace: namespace,
	}

	// Convert request to JSON
	upsertJSON, err := json.Marshal(upsertReq)
	if err != nil {
		return 0, fmt.Errorf("error marshaling upsert request: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/vectors/upsert", indexHost), bytes.NewBuffer(upsertJSON))
	if err != nil {
		return 0, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error executing upsert: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("upsert failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var upsertResult struct {
		UpsertedCount int64 `json:"upsertedCount"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&upsertResult); err != nil {
		return 0, fmt.Errorf("error decoding response: %v", err)
	}

	return upsertResult.UpsertedCount, nil
}

// WipeDatabase removes all data from the database
// Note: In Pinecone, this means deleting all vectors from all indexes
func WipeDatabase(client *PineconeClient) error {
	// Get all indexes
	indexes, err := listIndexes(client)
	if err != nil {
		return fmt.Errorf("error listing indexes: %v", err)
	}

	// Delete all vectors from each index
	for _, indexName := range indexes {
		// Construct the API URL for the specific index
		indexHost := fmt.Sprintf(pineconeAPIURL, indexName, client.ProjectID, client.Environment)

		// Create delete request (delete all vectors)
		deleteReq := struct {
			DeleteAll bool `json:"deleteAll"`
		}{
			DeleteAll: true,
		}

		// Convert request to JSON
		deleteJSON, err := json.Marshal(deleteReq)
		if err != nil {
			return fmt.Errorf("error marshaling delete request: %v", err)
		}

		// Create HTTP request
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/vectors/delete", indexHost), bytes.NewBuffer(deleteJSON))
		if err != nil {
			return fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Api-Key", client.APIKey)
		req.Header.Set("Content-Type", "application/json")

		// Execute request
		httpClient := &http.Client{Timeout: 30 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("error executing delete: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("delete failed for index %s with status %d: %s", indexName, resp.StatusCode, string(body))
		}
	}

	return nil
}

// Note: The pgxpool parameter is kept for compatibility with the PostgreSQL version,
// but it's not used in the Pinecone implementation
func FetchDataCompat(pool *pgxpool.Pool, indexName string, limit int) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("not implemented for Pinecone - use FetchData instead")
}
