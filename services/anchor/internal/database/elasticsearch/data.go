package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// FetchData retrieves data from a specified index
func FetchData(esClient *ElasticsearchClient, indexName string, limit int) ([]map[string]interface{}, error) {
	client := esClient.Client
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	// Build search query
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	if limit > 0 {
		query["size"] = limit
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %v", err)
	}

	// Perform the search request
	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex(indexName),
		client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("error searching index %s: %v", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Parse the response
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error parsing response: %v", err)
	}

	// Extract hits
	hits, found := result["hits"].(map[string]interface{})
	if !found {
		return nil, fmt.Errorf("hits not found in response")
	}

	hitsArray, found := hits["hits"].([]interface{})
	if !found {
		return nil, fmt.Errorf("hits array not found in response")
	}

	// Convert hits to result format
	var documents []map[string]interface{}
	for _, hit := range hitsArray {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		// Add document ID to the source
		if id, ok := hitMap["_id"].(string); ok {
			source["_id"] = id
		}

		documents = append(documents, source)
	}

	return documents, nil
}

// InsertData inserts data into a specified index
func InsertData(esClient *ElasticsearchClient, indexName string, data []map[string]interface{}) (int64, error) {
	client := esClient.Client
	if len(data) == 0 {
		return 0, nil
	}

	var totalRowsAffected int64

	// Prepare bulk request
	var buf bytes.Buffer
	for _, doc := range data {
		// Extract ID if present, otherwise Elasticsearch will generate one
		var id string
		if docID, ok := doc["_id"]; ok {
			id = fmt.Sprintf("%v", docID)
			delete(doc, "_id") // Remove ID from source document
		}

		// Create action line
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}

		// Add ID if present
		if id != "" {
			action["index"].(map[string]interface{})["_id"] = id
		}

		// Add action and document to bulk request
		if err := json.NewEncoder(&buf).Encode(action); err != nil {
			return totalRowsAffected, fmt.Errorf("error encoding action: %v", err)
		}
		if err := json.NewEncoder(&buf).Encode(doc); err != nil {
			return totalRowsAffected, fmt.Errorf("error encoding document: %v", err)
		}
	}

	// Execute bulk request
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(context.Background()),
		client.Bulk.WithIndex(indexName),
	)
	if err != nil {
		return totalRowsAffected, fmt.Errorf("error executing bulk request: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return totalRowsAffected, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Parse response
	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return totalRowsAffected, fmt.Errorf("error parsing bulk response: %v", err)
	}

	// Check for errors in items
	if items, ok := bulkResponse["items"].([]interface{}); ok {
		for _, item := range items {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if indexResp, ok := itemMap["index"].(map[string]interface{}); ok {
					if _, ok := indexResp["error"]; !ok {
						totalRowsAffected++
					}
				}
			}
		}
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all indices from the Elasticsearch cluster
func WipeDatabase(esClient *ElasticsearchClient) error {
	client := esClient.Client
	// Get all indices
	res, err := client.Indices.GetAlias(
		client.Indices.GetAlias.WithContext(context.Background()),
	)
	if err != nil {
		return fmt.Errorf("error getting indices: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// If 404, it means no indices exist, which is fine
		if res.StatusCode == 404 {
			return nil
		}
		return fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Parse response to get index names
	var aliasResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&aliasResponse); err != nil {
		return fmt.Errorf("error parsing alias response: %v", err)
	}

	// Extract index names
	var indices []string
	for index := range aliasResponse {
		// Skip system indices
		if !strings.HasPrefix(index, ".") {
			indices = append(indices, index)
		}
	}

	// Delete all indices
	if len(indices) > 0 {
		deleteRes, err := client.Indices.Delete(
			indices,
			client.Indices.Delete.WithContext(context.Background()),
		)
		if err != nil {
			return fmt.Errorf("error deleting indices: %v", err)
		}
		defer deleteRes.Body.Close()

		if deleteRes.IsError() {
			return fmt.Errorf("error response from Elasticsearch: %s", deleteRes.String())
		}
	}

	return nil
}
