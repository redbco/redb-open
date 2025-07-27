package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource sets up a replication source for an Elasticsearch index
func CreateReplicationSource(esClient *ElasticsearchClient, indexName string, databaseID string, eventHandler func(map[string]interface{})) (*ElasticsearchReplicationSourceDetails, error) {
	client := esClient.Client
	// Generate a unique watch ID
	watchID := fmt.Sprintf("watch_%s_%s", databaseID, common.GenerateUniqueID())

	// Create a watcher to monitor changes in the index
	err := createWatcher(client, watchID, indexName)
	if err != nil {
		return nil, fmt.Errorf("error creating watcher: %v", err)
	}

	details := &ElasticsearchReplicationSourceDetails{
		WatchID:    watchID,
		IndexName:  indexName,
		DatabaseID: databaseID,
	}

	// Start listening for replication events
	go listenForReplicationEvents(client, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(esClient *ElasticsearchClient, details *ElasticsearchReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	client := esClient.Client
	// Verify that the watch still exists
	exists, err := watchExists(client, details.WatchID)
	if err != nil {
		return fmt.Errorf("error checking watch: %v", err)
	}

	if !exists {
		return fmt.Errorf("watch %s does not exist", details.WatchID)
	}

	// Start listening for replication events
	go listenForReplicationEvents(client, details, eventHandler)

	return nil
}

func createWatcher(client *elasticsearch.Client, watchID, indexName string) error {
	// Check if X-Pack Watcher is available
	infoRes, err := client.Info()
	if err != nil {
		return fmt.Errorf("error checking Elasticsearch info: %v", err)
	}
	defer infoRes.Body.Close()

	var infoResp map[string]interface{}
	if err := json.NewDecoder(infoRes.Body).Decode(&infoResp); err != nil {
		return fmt.Errorf("error parsing info response: %v", err)
	}

	// Check if X-Pack is available
	if features, ok := infoResp["features"].(map[string]interface{}); ok {
		if watcher, ok := features["watcher"].(map[string]interface{}); ok {
			if available, ok := watcher["available"].(bool); ok && !available {
				return fmt.Errorf("X-Pack Watcher is not available")
			}
		}
	}

	// Create a watch that monitors changes in the index
	watch := map[string]interface{}{
		"trigger": map[string]interface{}{
			"schedule": map[string]interface{}{
				"interval": "1m", // Check every minute
			},
		},
		"input": map[string]interface{}{
			"search": map[string]interface{}{
				"request": map[string]interface{}{
					"indices": []string{indexName},
					"body": map[string]interface{}{
						"query": map[string]interface{}{
							"bool": map[string]interface{}{
								"must": map[string]interface{}{
									"range": map[string]interface{}{
										"@timestamp": map[string]interface{}{
											"gte": "now-1m",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"condition": map[string]interface{}{
			"compare": map[string]interface{}{
				"ctx.payload.hits.total": map[string]interface{}{
					"gt": 0,
				},
			},
		},
		"actions": map[string]interface{}{
			"log_changes": map[string]interface{}{
				"logging": map[string]interface{}{
					"text": "Changes detected in index {{ctx.payload.hits.total}}",
				},
			},
		},
		"metadata": map[string]interface{}{
			"index_name":  indexName,
			"database_id": indexName,
		},
	}

	// Convert watch to JSON
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(watch); err != nil {
		return fmt.Errorf("error encoding watch: %v", err)
	}

	// Create the watch
	res, err := client.Watcher.PutWatch(
		watchID,
		client.Watcher.PutWatch.WithBody(&buf),
		client.Watcher.PutWatch.WithContext(context.Background()),
	)
	if err != nil {
		return fmt.Errorf("error creating watch: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	return nil
}

func watchExists(client *elasticsearch.Client, watchID string) (bool, error) {
	res, err := client.Watcher.GetWatch(
		watchID,
		client.Watcher.GetWatch.WithContext(context.Background()),
	)
	if err != nil {
		return false, fmt.Errorf("error getting watch: %v", err)
	}
	defer res.Body.Close()

	// If 404, the watch doesn't exist
	if res.StatusCode == 404 {
		return false, nil
	}

	if res.IsError() {
		return false, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	return true, nil
}

func listenForReplicationEvents(client *elasticsearch.Client, details *ElasticsearchReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	for {
		changes, err := getReplicationChanges(client, details.IndexName)
		if err != nil {
			log.Printf("Error getting replication changes: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, change := range changes {
			event := map[string]interface{}{
				"index":     details.IndexName,
				"operation": change.Operation,
				"data":      change.Data,
				"old_data":  change.OldData,
			}
			eventHandler(event)
		}

		time.Sleep(1 * time.Second)
	}
}

func getReplicationChanges(client *elasticsearch.Client, indexName string) ([]ElasticsearchReplicationChange, error) {
	// This is a simplified implementation that uses the _changes API
	// In a real implementation, you might use the _changes API with a sequence token
	// or implement a custom solution using timestamps or versions

	// Get recent documents
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"@timestamp": map[string]interface{}{
					"gte": "now-10s",
				},
			},
		},
		"sort": []map[string]interface{}{
			{
				"@timestamp": map[string]interface{}{
					"order": "desc",
				},
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %v", err)
	}

	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex(indexName),
		client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("error searching index: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var searchResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("error parsing search response: %v", err)
	}

	var changes []ElasticsearchReplicationChange

	// Extract hits
	if hits, ok := searchResponse["hits"].(map[string]interface{}); ok {
		if hitsArray, ok := hits["hits"].([]interface{}); ok {
			for _, hit := range hitsArray {
				hitMap, ok := hit.(map[string]interface{})
				if !ok {
					continue
				}

				source, ok := hitMap["_source"].(map[string]interface{})
				if !ok {
					continue
				}

				// Determine operation type (simplified)
				operation := "INDEX"
				if _, ok := source["_deleted"]; ok {
					operation = "DELETE"
				}

				change := ElasticsearchReplicationChange{
					Operation: operation,
					Data:      source,
				}

				changes = append(changes, change)
			}
		}
	}

	return changes, nil
}
