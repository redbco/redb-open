package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ExecuteQuery executes a search query on Elasticsearch and returns results as a slice of maps
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return nil, fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	ctx := context.Background()

	// Parse query to extract index and search body
	// Expected format: {"index": "index_name", "body": {...}}
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse elasticsearch query: %w", err)
	}

	indexName, ok := queryReq["index"].(string)
	if !ok {
		return nil, fmt.Errorf("index name is required in elasticsearch query")
	}

	searchBody, ok := queryReq["body"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("search body is required in elasticsearch query")
	}

	// Convert search body to JSON
	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search body: %w", err)
	}

	// Execute search
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(indexName),
		client.Search.WithBody(strings.NewReader(string(bodyBytes))),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute elasticsearch search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch search error: %s", res.String())
	}

	// Parse response
	var searchResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode elasticsearch response: %w", err)
	}

	// Extract hits
	var results []interface{}
	if hits, ok := searchResp["hits"].(map[string]interface{}); ok {
		if hitsList, ok := hits["hits"].([]interface{}); ok {
			for _, hit := range hitsList {
				if hitMap, ok := hit.(map[string]interface{}); ok {
					// Include both _source and metadata
					result := make(map[string]interface{})
					if source, ok := hitMap["_source"]; ok {
						result["_source"] = source
					}
					if id, ok := hitMap["_id"]; ok {
						result["_id"] = id
					}
					if index, ok := hitMap["_index"]; ok {
						result["_index"] = index
					}
					if score, ok := hitMap["_score"]; ok {
						result["_score"] = score
					}
					results = append(results, result)
				}
			}
		}
	}

	return results, nil
}

// ExecuteCountQuery executes a count query on Elasticsearch and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return 0, fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	ctx := context.Background()

	// Parse query to extract index and search body
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse elasticsearch count query: %w", err)
	}

	indexName, ok := queryReq["index"].(string)
	if !ok {
		return 0, fmt.Errorf("index name is required in elasticsearch count query")
	}

	// Use count API for better performance
	var bodyReader *strings.Reader
	if searchBody, ok := queryReq["body"].(map[string]interface{}); ok {
		if queryPart, ok := searchBody["query"]; ok {
			countBody := map[string]interface{}{
				"query": queryPart,
			}
			bodyBytes, err := json.Marshal(countBody)
			if err != nil {
				return 0, fmt.Errorf("failed to marshal count body: %w", err)
			}
			bodyReader = strings.NewReader(string(bodyBytes))
		}
	}

	// Execute count
	res, err := client.Count(
		client.Count.WithContext(ctx),
		client.Count.WithIndex(indexName),
		client.Count.WithBody(bodyReader),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to execute elasticsearch count: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("elasticsearch count error: %s", res.String())
	}

	// Parse response
	var countResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&countResp); err != nil {
		return 0, fmt.Errorf("failed to decode elasticsearch count response: %w", err)
	}

	// Extract count
	if count, ok := countResp["count"].(float64); ok {
		return int64(count), nil
	}

	return 0, nil
}

// StreamTableData streams documents from an Elasticsearch index in batches for efficient data copying
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	ctx := context.Background()

	// Build search body with from/size pagination
	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"from": offset,
		"size": batchSize,
	}

	// Add source filtering for specific columns if requested
	if len(columns) > 0 {
		searchBody["_source"] = columns
	}

	// Convert to JSON
	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to marshal search body: %w", err)
	}

	// Execute search
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(tableName),
		client.Search.WithBody(strings.NewReader(string(bodyBytes))),
		client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute elasticsearch streaming search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, false, "", fmt.Errorf("elasticsearch streaming search error: %s", res.String())
	}

	// Parse response
	var searchResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, false, "", fmt.Errorf("failed to decode elasticsearch response: %w", err)
	}

	// Extract hits
	var results []map[string]interface{}
	if hits, ok := searchResp["hits"].(map[string]interface{}); ok {
		if hitsList, ok := hits["hits"].([]interface{}); ok {
			for _, hit := range hitsList {
				if hitMap, ok := hit.(map[string]interface{}); ok {
					// Create result with both _source and metadata
					result := make(map[string]interface{})
					if source, ok := hitMap["_source"].(map[string]interface{}); ok {
						// Flatten _source fields into the result
						for k, v := range source {
							result[k] = v
						}
					}
					// Add metadata fields
					if id, ok := hitMap["_id"]; ok {
						result["_id"] = id
					}
					if index, ok := hitMap["_index"]; ok {
						result["_index"] = index
					}
					if score, ok := hitMap["_score"]; ok {
						result["_score"] = score
					}
					results = append(results, result)
				}
			}
		}
	}

	rowCount := len(results)
	isComplete := rowCount < int(batchSize)

	// For simple offset-based pagination, we don't use cursor values
	nextCursorValue := ""

	return results, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of documents in an Elasticsearch index
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return 0, false, fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	ctx := context.Background()

	// Build count body
	var bodyReader *strings.Reader
	if whereClause != "" {
		// Parse whereClause as JSON query
		var queryClause map[string]interface{}
		if err := json.Unmarshal([]byte(whereClause), &queryClause); err != nil {
			return 0, false, fmt.Errorf("failed to parse where clause: %w", err)
		}

		countBody := map[string]interface{}{
			"query": queryClause,
		}
		bodyBytes, err := json.Marshal(countBody)
		if err != nil {
			return 0, false, fmt.Errorf("failed to marshal count body: %w", err)
		}
		bodyReader = strings.NewReader(string(bodyBytes))
	}

	// Execute count
	res, err := client.Count(
		client.Count.WithContext(ctx),
		client.Count.WithIndex(tableName),
		client.Count.WithBody(bodyReader),
	)
	if err != nil {
		return 0, false, fmt.Errorf("failed to execute elasticsearch count: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, false, fmt.Errorf("elasticsearch count error: %s", res.String())
	}

	// Parse response
	var countResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&countResp); err != nil {
		return 0, false, fmt.Errorf("failed to decode elasticsearch count response: %w", err)
	}

	// Extract count
	if count, ok := countResp["count"].(float64); ok {
		// Elasticsearch count is always exact, not an estimate
		return int64(count), false, nil
	}

	return 0, false, nil
}
