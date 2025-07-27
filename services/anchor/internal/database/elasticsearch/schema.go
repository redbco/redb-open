package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of an Elasticsearch cluster
func DiscoverSchema(esClient *ElasticsearchClient) (*ElasticsearchSchema, error) {
	client := esClient.Client
	schema := &ElasticsearchSchema{}
	var err error

	// Get indices
	schema.Indices, err = discoverIndices(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering indices: %v", err)
	}

	// Get index templates
	schema.Templates, err = discoverTemplates(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering templates: %v", err)
	}

	// Get ingest pipelines
	schema.Pipelines, err = discoverPipelines(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering pipelines: %v", err)
	}

	// Get aliases
	schema.Aliases, err = discoverAliases(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering aliases: %v", err)
	}

	// Get component templates
	schema.Components, err = discoverComponentTemplates(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering component templates: %v", err)
	}

	return schema, nil
}

// CreateStructure creates Elasticsearch indices and other objects based on the provided parameters
func CreateStructure(esClient *ElasticsearchClient, params common.StructureParams) error {
	ctx := context.Background()

	// Create indices
	for _, index := range params.Tables {
		if err := createIndex(ctx, esClient.Client, index); err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

func discoverIndices(client *elasticsearch.Client) ([]common.TableInfo, error) {
	// Get all indices
	res, err := client.Indices.Get(
		[]string{"*"},
		client.Indices.Get.WithContext(context.Background()),
		client.Indices.Get.WithHuman(),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting indices: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// If 404, it means no indices exist
		if res.StatusCode == 404 {
			return []common.TableInfo{}, nil
		}
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var indicesResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&indicesResponse); err != nil {
		return nil, fmt.Errorf("error parsing indices response: %v", err)
	}

	var indices []common.TableInfo
	for indexName, indexData := range indicesResponse {
		// Skip system indices
		if strings.HasPrefix(indexName, ".") {
			continue
		}

		indexInfo, ok := indexData.(map[string]interface{})
		if !ok {
			continue
		}

		// Create TableInfo for the index
		tableInfo := common.TableInfo{
			Name:      indexName,
			Schema:    "default", // Elasticsearch doesn't have schemas like SQL databases
			TableType: "elasticsearch.index",
		}

		// Get mappings using the utility function
		mapping, err := getMapping(client, indexName)
		if err != nil {
			// Log the error but continue with other indices
			fmt.Printf("Warning: Error getting mapping for index %s: %v\n", indexName, err)
		} else if mapping != nil {
			// Extract properties from mappings
			if mappings, ok := mapping["mappings"].(map[string]interface{}); ok {
				properties, ok := mappings["properties"].(map[string]interface{})
				if ok {
					// Convert properties to columns
					for propName, propData := range properties {
						propInfo, ok := propData.(map[string]interface{})
						if !ok {
							continue
						}

						dataType, _ := propInfo["type"].(string)
						column := common.ColumnInfo{
							Name:       propName,
							DataType:   dataType,
							IsNullable: true, // Elasticsearch fields are nullable by default
						}

						tableInfo.Columns = append(tableInfo.Columns, column)
					}
				}
			}
		}

		// Get settings
		settings, ok := indexInfo["settings"].(map[string]interface{})
		if ok {
			// Extract index settings
			if index, ok := settings["index"].(map[string]interface{}); ok {
				// Create an index for the primary key if specified
				if id, ok := index["routing"].(map[string]interface{}); ok {
					if path, ok := id["allocation"].(map[string]interface{}); ok {
						if require, ok := path["require"].(map[string]interface{}); ok {
							if idPath, ok := require["_id"].(string); ok {
								tableInfo.PrimaryKey = []string{idPath}
							}
						}
					}
				}

				// Extract other settings for index info
				if numberOfShards, ok := index["number_of_shards"].(string); ok {
					if numberOfReplicas, ok := index["number_of_replicas"].(string); ok {
						indexInfo := common.IndexInfo{
							Name:     indexName + "_settings",
							IsUnique: false,
							Settings: common.SettingsInfo{
								NumberOfShards:   numberOfShards,
								NumberOfReplicas: numberOfReplicas,
							},
						}
						tableInfo.Indexes = append(tableInfo.Indexes, indexInfo)
					}
				}
			}
		}

		indices = append(indices, tableInfo)
	}

	return indices, nil
}

func discoverTemplates(client *elasticsearch.Client) ([]TemplateInfo, error) {
	// Get all index templates
	res, err := client.Indices.GetIndexTemplate(
		client.Indices.GetIndexTemplate.WithContext(context.Background()),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting index templates: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// If 404, it means no templates exist
		if res.StatusCode == 404 {
			return []TemplateInfo{}, nil
		}
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var templatesResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&templatesResponse); err != nil {
		return nil, fmt.Errorf("error parsing templates response: %v", err)
	}

	var templates []TemplateInfo
	if templatesList, ok := templatesResponse["index_templates"].([]interface{}); ok {
		for _, templateData := range templatesList {
			template, ok := templateData.(map[string]interface{})
			if !ok {
				continue
			}

			name, ok := template["name"].(string)
			if !ok {
				continue
			}

			templateInfo := TemplateInfo{
				Name: name,
			}

			// Extract index patterns
			if indexTemplate, ok := template["index_template"].(map[string]interface{}); ok {
				if patterns, ok := indexTemplate["index_patterns"].([]interface{}); ok {
					for _, pattern := range patterns {
						if patternStr, ok := pattern.(string); ok {
							templateInfo.IndexPatterns = append(templateInfo.IndexPatterns, patternStr)
						}
					}
				}

				// Extract priority
				if priority, ok := indexTemplate["priority"].(float64); ok {
					templateInfo.Priority = int(priority)
				}

				// Extract version
				if version, ok := indexTemplate["version"].(float64); ok {
					templateInfo.Version = int(version)
				}

				// Extract template settings
				if template, ok := indexTemplate["template"].(map[string]interface{}); ok {
					templateInfo.Settings = template["settings"].(map[string]interface{})
					templateInfo.Mappings = template["mappings"].(map[string]interface{})
				}
			}

			templates = append(templates, templateInfo)
		}
	}

	return templates, nil
}

func discoverPipelines(client *elasticsearch.Client) ([]PipelineInfo, error) {
	// Get all ingest pipelines
	res, err := client.Ingest.GetPipeline(
		client.Ingest.GetPipeline.WithContext(context.Background()),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting pipelines: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// If 404, it means no pipelines exist
		if res.StatusCode == 404 {
			return []PipelineInfo{}, nil
		}
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var pipelinesResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&pipelinesResponse); err != nil {
		return nil, fmt.Errorf("error parsing pipelines response: %v", err)
	}

	var pipelines []PipelineInfo
	for pipelineName, pipelineData := range pipelinesResponse {
		pipeline, ok := pipelineData.(map[string]interface{})
		if !ok {
			continue
		}

		pipelineInfo := PipelineInfo{
			Name: pipelineName,
		}

		// Extract description
		if description, ok := pipeline["description"].(string); ok {
			pipelineInfo.Description = description
		}

		// Extract processors
		if processors, ok := pipeline["processors"].([]interface{}); ok {
			for _, processor := range processors {
				if processorMap, ok := processor.(map[string]interface{}); ok {
					pipelineInfo.Processors = append(pipelineInfo.Processors, processorMap)
				}
			}
		}

		pipelines = append(pipelines, pipelineInfo)
	}

	return pipelines, nil
}

func discoverAliases(client *elasticsearch.Client) ([]AliasInfo, error) {
	// Get all aliases
	res, err := client.Indices.GetAlias(
		client.Indices.GetAlias.WithContext(context.Background()),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting aliases: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// If 404, it means no aliases exist
		if res.StatusCode == 404 {
			return []AliasInfo{}, nil
		}
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var aliasesResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&aliasesResponse); err != nil {
		return nil, fmt.Errorf("error parsing aliases response: %v", err)
	}

	// Map to track aliases and their indices
	aliasMap := make(map[string]*AliasInfo)

	// Process each index and its aliases
	for indexName, indexData := range aliasesResponse {
		// Skip system indices
		if strings.HasPrefix(indexName, ".") {
			continue
		}

		indexInfo, ok := indexData.(map[string]interface{})
		if !ok {
			continue
		}

		aliases, ok := indexInfo["aliases"].(map[string]interface{})
		if !ok {
			continue
		}

		// Process each alias for this index
		for aliasName, aliasData := range aliases {
			// Get or create alias info
			aliasInfo, exists := aliasMap[aliasName]
			if !exists {
				aliasInfo = &AliasInfo{
					Name:    aliasName,
					Indices: []string{},
				}
				aliasMap[aliasName] = aliasInfo
			}

			// Add this index to the alias
			aliasInfo.Indices = append(aliasInfo.Indices, indexName)

			// Extract alias properties
			if aliasProps, ok := aliasData.(map[string]interface{}); ok {
				if isWriteIndex, ok := aliasProps["is_write_index"].(bool); ok && isWriteIndex {
					aliasInfo.IsWriteIndex = true
				}
				if filter, ok := aliasProps["filter"].(map[string]interface{}); ok {
					aliasInfo.Filter = filter
				}
			}
		}
	}

	// Convert map to slice
	var aliases []AliasInfo
	for _, aliasInfo := range aliasMap {
		aliases = append(aliases, *aliasInfo)
	}

	return aliases, nil
}

func discoverComponentTemplates(client *elasticsearch.Client) ([]ComponentInfo, error) {
	// Get all component templates
	res, err := client.Cluster.GetComponentTemplate(
		client.Cluster.GetComponentTemplate.WithContext(context.Background()),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting component templates: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		// If 404, it means no component templates exist
		if res.StatusCode == 404 {
			return []ComponentInfo{}, nil
		}
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var componentsResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&componentsResponse); err != nil {
		return nil, fmt.Errorf("error parsing component templates response: %v", err)
	}

	var components []ComponentInfo
	if componentsList, ok := componentsResponse["component_templates"].([]interface{}); ok {
		for _, componentData := range componentsList {
			component, ok := componentData.(map[string]interface{})
			if !ok {
				continue
			}

			name, ok := component["name"].(string)
			if !ok {
				continue
			}

			componentInfo := ComponentInfo{
				Name: name,
			}

			// Extract component template details
			if componentTemplate, ok := component["component_template"].(map[string]interface{}); ok {
				// Extract version
				if version, ok := componentTemplate["version"].(float64); ok {
					componentInfo.Version = int(version)
				}

				// Extract template settings and mappings
				if template, ok := componentTemplate["template"].(map[string]interface{}); ok {
					if settings, ok := template["settings"].(map[string]interface{}); ok {
						componentInfo.Settings = settings
					}
					if mappings, ok := template["mappings"].(map[string]interface{}); ok {
						componentInfo.Mappings = mappings
					}
				}
			}

			components = append(components, componentInfo)
		}
	}

	return components, nil
}

func createIndex(ctx context.Context, client *elasticsearch.Client, indexInfo common.TableInfo) error {
	// Check if index already exists
	existsRes, err := client.Indices.Exists([]string{indexInfo.Name})
	if err != nil {
		return fmt.Errorf("error checking if index exists: %v", err)
	}

	if existsRes.StatusCode == 200 {
		return fmt.Errorf("index '%s' already exists", indexInfo.Name)
	}

	// Prepare index creation request
	var settings map[string]interface{}
	var mappings map[string]interface{}

	// Extract settings from index info
	if len(indexInfo.Indexes) > 0 {
		for _, idx := range indexInfo.Indexes {
			if idx.Settings.NumberOfShards != "" && idx.Settings.NumberOfReplicas != "" {
				settings = map[string]interface{}{
					"number_of_shards":   idx.Settings.NumberOfShards,
					"number_of_replicas": idx.Settings.NumberOfReplicas,
				}
				break
			}
		}
	}

	// Create mappings from columns
	properties := make(map[string]interface{})
	for _, column := range indexInfo.Columns {
		fieldMapping := map[string]interface{}{
			"type": getElasticsearchType(column.DataType),
		}
		properties[column.Name] = fieldMapping
	}

	mappings = map[string]interface{}{
		"properties": properties,
	}

	// Create index request body
	indexRequest := map[string]interface{}{}

	// Add settings if available
	if settings != nil {
		indexRequest["settings"] = map[string]interface{}{
			"index": settings,
		}
	}

	// Add mappings (no need to check if nil since we always create it)
	indexRequest["mappings"] = mappings

	// Convert request to JSON
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(indexRequest); err != nil {
		return fmt.Errorf("error encoding index request: %v", err)
	}

	// Create the index
	res, err := client.Indices.Create(
		indexInfo.Name,
		client.Indices.Create.WithContext(ctx),
		client.Indices.Create.WithBody(&buf),
	)
	if err != nil {
		return fmt.Errorf("error creating index: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	return nil
}

// Helper function to map SQL/common types to Elasticsearch types
func getElasticsearchType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "integer", "int", "smallint", "bigint":
		return "integer"
	case "float", "real", "double precision", "double":
		return "float"
	case "boolean", "bool":
		return "boolean"
	case "date", "timestamp", "timestamp with time zone", "timestamp without time zone":
		return "date"
	case "text", "character varying", "varchar", "char", "character":
		return "text"
	case "keyword":
		return "keyword"
	case "object", "json", "jsonb":
		return "object"
	case "nested":
		return "nested"
	case "geo_point":
		return "geo_point"
	case "geo_shape":
		return "geo_shape"
	default:
		return "keyword" // Default to keyword for unknown types
	}
}

// getMapping retrieves the mapping for a specified index
func getMapping(client *elasticsearch.Client, indexName string) (map[string]interface{}, error) {
	res, err := client.Indices.GetMapping(
		client.Indices.GetMapping.WithContext(context.Background()),
		client.Indices.GetMapping.WithIndex(indexName),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting mapping: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var mappingResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&mappingResponse); err != nil {
		return nil, fmt.Errorf("error parsing mapping response: %v", err)
	}

	// Extract mapping for the specified index
	indexMapping, ok := mappingResponse[indexName].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("mapping for index %s not found", indexName)
	}

	// Extract the mappings object which contains the properties
	mappings, ok := indexMapping["mappings"].(map[string]interface{})
	if !ok {
		// Return empty mappings if not found
		return map[string]interface{}{"mappings": map[string]interface{}{}}, nil
	}

	return map[string]interface{}{"mappings": mappings}, nil
}
