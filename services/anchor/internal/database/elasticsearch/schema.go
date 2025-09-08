package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of an Elasticsearch cluster and returns a UnifiedModel
func DiscoverSchema(esClient *ElasticsearchClient) (*unifiedmodel.UnifiedModel, error) {
	client := esClient.Client

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Elasticsearch,
		SearchIndexes: make(map[string]unifiedmodel.SearchIndex),
	}

	// Discover indices directly into unified model
	if err := discoverIndicesUnified(client, um); err != nil {
		return nil, fmt.Errorf("error discovering indices: %v", err)
	}

	// Note: Elasticsearch pipelines are not supported in the unified model yet

	// Note: Templates, aliases, and components are Elasticsearch-specific
	// and don't have direct UnifiedModel equivalents, so we skip them
	// for now but could be added as extensions or custom metadata

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(esClient *ElasticsearchClient, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	ctx := context.Background()

	// Create search indexes from UnifiedModel
	for _, searchIndex := range um.SearchIndexes {
		if err := createSearchIndexFromUnified(ctx, esClient.Client, searchIndex); err != nil {
			return fmt.Errorf("error creating search index %s: %v", searchIndex.Name, err)
		}
	}

	// Note: Elasticsearch pipelines are not supported in the unified model yet

	return nil
}

func discoverIndicesUnified(client *elasticsearch.Client, um *unifiedmodel.UnifiedModel) error {
	// Get all indices
	res, err := client.Indices.Get(
		[]string{"*"},
		client.Indices.Get.WithContext(context.Background()),
		client.Indices.Get.WithHuman(),
	)
	if err != nil {
		return fmt.Errorf("error getting indices: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			// No indices found, return empty
			return nil
		}
		return fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var indicesResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&indicesResponse); err != nil {
		return fmt.Errorf("error parsing indices response: %v", err)
	}

	// Convert indices to unified model search indexes
	for indexName, indexData := range indicesResponse {
		// Skip system indices
		if strings.HasPrefix(indexName, ".") {
			continue
		}

		_, ok := indexData.(map[string]interface{})
		if !ok {
			continue
		}

		// Create SearchIndex for the index
		searchIndex := unifiedmodel.SearchIndex{
			Name:   indexName,
			Fields: []string{},
		}

		// Get mappings to extract field information
		mappingRes, err := client.Indices.GetMapping(
			client.Indices.GetMapping.WithIndex(indexName),
			client.Indices.GetMapping.WithContext(context.Background()),
		)
		if err == nil {
			defer mappingRes.Body.Close()
			if !mappingRes.IsError() {
				var mappingResponse map[string]interface{}
				if err := json.NewDecoder(mappingRes.Body).Decode(&mappingResponse); err == nil {
					if indexMapping, ok := mappingResponse[indexName].(map[string]interface{}); ok {
						if mappings, ok := indexMapping["mappings"].(map[string]interface{}); ok {
							if properties, ok := mappings["properties"].(map[string]interface{}); ok {
								// Extract field names
								for fieldName := range properties {
									searchIndex.Fields = append(searchIndex.Fields, fieldName)
								}
							}
						}
					}
				}
			}
		}

		// Get settings to extract analyzer information
		settingsRes, err := client.Indices.GetSettings(
			client.Indices.GetSettings.WithIndex(indexName),
			client.Indices.GetSettings.WithContext(context.Background()),
		)
		if err == nil {
			defer settingsRes.Body.Close()
			if !settingsRes.IsError() {
				var settingsResponse map[string]interface{}
				if err := json.NewDecoder(settingsRes.Body).Decode(&settingsResponse); err == nil {
					if indexSettings, ok := settingsResponse[indexName].(map[string]interface{}); ok {
						if settings, ok := indexSettings["settings"].(map[string]interface{}); ok {
							if index, ok := settings["index"].(map[string]interface{}); ok {
								if analysis, ok := index["analysis"].(map[string]interface{}); ok {
									if analyzer, ok := analysis["analyzer"].(map[string]interface{}); ok {
										if defaultAnalyzer, ok := analyzer["default"].(map[string]interface{}); ok {
											if analyzerType, ok := defaultAnalyzer["type"].(string); ok {
												searchIndex.Analyzer = analyzerType
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		um.SearchIndexes[indexName] = searchIndex
	}

	return nil
}

// createSearchIndexFromUnified creates a search index from UnifiedModel SearchIndex
func createSearchIndexFromUnified(ctx context.Context, client *elasticsearch.Client, searchIndex unifiedmodel.SearchIndex) error {
	if searchIndex.Name == "" {
		return fmt.Errorf("search index name cannot be empty")
	}

	// Create basic mapping for the index
	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{},
		},
	}

	// Add field mappings if specified
	if len(searchIndex.Fields) > 0 {
		properties := mapping["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
		for _, field := range searchIndex.Fields {
			properties[field] = map[string]interface{}{
				"type": "text", // Default field type
			}
		}
	}

	// Add analyzer if specified
	if searchIndex.Analyzer != "" {
		if settings, ok := mapping["settings"].(map[string]interface{}); ok {
			settings["analysis"] = map[string]interface{}{
				"analyzer": map[string]interface{}{
					"default": map[string]interface{}{
						"type": searchIndex.Analyzer,
					},
				},
			}
		} else {
			mapping["settings"] = map[string]interface{}{
				"analysis": map[string]interface{}{
					"analyzer": map[string]interface{}{
						"default": map[string]interface{}{
							"type": searchIndex.Analyzer,
						},
					},
				},
			}
		}
	}

	// Create the index
	body, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("error marshaling index mapping: %v", err)
	}

	res, err := client.Indices.Create(
		searchIndex.Name,
		client.Indices.Create.WithContext(ctx),
		client.Indices.Create.WithBody(strings.NewReader(string(body))),
	)
	if err != nil {
		return fmt.Errorf("error creating index: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response creating index: %s", res.String())
	}

	return nil
}
