package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type ElasticsearchIngester struct{}

func (e *ElasticsearchIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var esSchema models.ElasticsearchSchema
	if err := json.Unmarshal(rawSchema, &esSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal Elasticsearch schema: %w", err)
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert indices to tables
	for _, index := range esSchema.Indices {
		unifiedTable := models.Table{
			Name:      index.Name,
			Schema:    "default",
			TableType: "standard",
			Comment:   index.Comment,
		}

		// Convert mappings to columns
		if mappings, ok := index.Mappings["properties"].(map[string]interface{}); ok {
			for fieldName, fieldData := range mappings {
				if field, ok := fieldData.(map[string]interface{}); ok {
					unifiedCol := models.Column{
						Name: fieldName,
						DataType: models.DataType{
							Name: getFieldType(field),
						},
						IsNullable: true, // Elasticsearch fields are nullable by default
						Comment:    getFieldComment(field),
					}
					unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
				}
			}
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	return unifiedModel, warnings, nil
}

type ElasticsearchExporter struct{}

func (e *ElasticsearchExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	esSchema := models.ElasticsearchSchema{
		SchemaType: "elasticsearch",
	}
	warnings := []string{}

	// Convert tables to indices
	for _, table := range model.Tables {
		esIndex := models.ElasticsearchIndex{
			Name:             table.Name,
			NumberOfShards:   1, // Default value
			NumberOfReplicas: 1, // Default value
			Comment:          table.Comment,
			Mappings:         make(map[string]interface{}),
		}

		// Convert columns to mappings
		properties := make(map[string]interface{})
		for _, col := range table.Columns {
			field := models.ElasticsearchField{
				Name:      col.Name,
				Type:      convertDataTypeToESType(col.DataType.Name),
				Comment:   col.Comment,
				Enabled:   true,
				Index:     true,
				DocValues: true,
				Store:     true,
			}
			properties[col.Name] = field
		}

		esIndex.Mappings["properties"] = properties
		esSchema.Indices = append(esSchema.Indices, esIndex)
	}

	return esSchema, warnings, nil
}

// Helper functions

func getFieldType(field map[string]interface{}) string {
	if fieldType, ok := field["type"].(string); ok {
		return fieldType
	}
	return "string" // Default type
}

func getFieldComment(field map[string]interface{}) string {
	if comment, ok := field["comment"].(string); ok {
		return comment
	}
	return ""
}

func convertDataTypeToESType(dataType string) string {
	switch dataType {
	case "integer", "int", "bigint":
		return "long"
	case "float", "double":
		return "double"
	case "boolean", "bool":
		return "boolean"
	case "date", "datetime", "timestamp":
		return "date"
	case "text", "varchar", "char":
		return "text"
	case "binary", "blob":
		return "binary"
	default:
		return "keyword"
	}
}
