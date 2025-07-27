package generators

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type ElasticsearchGenerator struct{}

func (g *ElasticsearchGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	var sb strings.Builder

	// Create index with settings
	sb.WriteString(fmt.Sprintf("PUT /%s\n", table.Name))
	sb.WriteString("{\n")
	sb.WriteString("  \"settings\": {\n")
	sb.WriteString("    \"number_of_shards\": 1,\n")
	sb.WriteString("    \"number_of_replicas\": 1\n")
	sb.WriteString("  },\n")

	// Add mappings
	sb.WriteString("  \"mappings\": {\n")
	sb.WriteString("    \"properties\": {\n")

	// Add field mappings
	for _, col := range table.Columns {
		fieldDef := g.generateFieldMapping(col.Name, models.ElasticsearchField{
			Type: convertToElasticsearchType(col.DataType),
		})
		sb.WriteString(fieldDef + ",\n")
	}

	sb.WriteString("    }\n  }\n}")
	return sb.String(), nil
}

func (g *ElasticsearchGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	var sql strings.Builder

	// Add comment for the function
	sql.WriteString(fmt.Sprintf("-- Function: %s\n", fn.Name))
	sql.WriteString(fmt.Sprintf("-- Arguments: %v\n", fn.Arguments))
	sql.WriteString(fmt.Sprintf("-- Return type: %s\n", fn.ReturnType))

	// Note: Elasticsearch doesn't support functions directly
	sql.WriteString("-- Note: Elasticsearch doesn't support functions directly\n")
	sql.WriteString("-- Consider using scripted fields or runtime fields instead\n")

	return sql.String(), nil
}

func (g *ElasticsearchGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	var sql strings.Builder

	// Add comment for the trigger
	sql.WriteString(fmt.Sprintf("-- Trigger: %s\n", trigger.Name))
	sql.WriteString(fmt.Sprintf("-- Table: %s.%s\n", trigger.Schema, trigger.Table))
	sql.WriteString(fmt.Sprintf("-- Event: %s\n", trigger.Event))

	// Note: Elasticsearch doesn't support triggers directly
	sql.WriteString("-- Note: Elasticsearch doesn't support triggers directly\n")
	sql.WriteString("-- Consider using watchers or transforms instead\n")

	return sql.String(), nil
}

func (g *ElasticsearchGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	var sql strings.Builder

	// Add comment for the sequence
	sql.WriteString(fmt.Sprintf("-- Sequence: %s\n", seq.Name))

	// Note: Elasticsearch doesn't support sequences directly
	sql.WriteString("-- Note: Elasticsearch doesn't support sequences directly\n")
	sql.WriteString("-- Consider using auto-incrementing IDs or UUIDs instead\n")

	return sql.String(), nil
}

func (g *ElasticsearchGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sql strings.Builder
	warnings := []string{}

	// Add header comment
	sql.WriteString("-- Elasticsearch Schema Generated from UnifiedModel\n\n")

	// Process tables
	for _, table := range model.Tables {
		tableSQL, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating table %s.%s: %v", table.Schema, table.Name, err))
			continue
		}
		sql.WriteString(tableSQL)
		sql.WriteString("\n\n")
	}

	// Process functions
	for _, fn := range model.Functions {
		fnSQL, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating function %s.%s: %v", fn.Schema, fn.Name, err))
			continue
		}
		sql.WriteString(fnSQL)
		sql.WriteString("\n\n")
	}

	// Process triggers
	for _, trigger := range model.Triggers {
		triggerSQL, err := g.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating trigger %s.%s: %v", trigger.Schema, trigger.Name, err))
			continue
		}
		sql.WriteString(triggerSQL)
		sql.WriteString("\n\n")
	}

	// Process sequences
	for _, seq := range model.Sequences {
		seqSQL, err := g.GenerateCreateSequenceSQL(seq)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Error generating sequence %s.%s: %v", seq.Schema, seq.Name, err))
			continue
		}
		sql.WriteString(seqSQL)
		sql.WriteString("\n\n")
	}

	return sql.String(), warnings, nil
}

func (g *ElasticsearchGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	// This method is kept for backward compatibility
	return nil, nil
}

func (g *ElasticsearchGenerator) generateFieldMapping(fieldName string, field models.ElasticsearchField) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("      \"%s\": {\n", fieldName))
	sb.WriteString(fmt.Sprintf("        \"type\": \"%s\"", field.Type))

	if field.Format != "" {
		sb.WriteString(fmt.Sprintf(",\n        \"format\": \"%s\"", field.Format))
	}

	if field.Analyzer != "" {
		sb.WriteString(fmt.Sprintf(",\n        \"analyzer\": \"%s\"", field.Analyzer))
	}

	if field.SearchAnalyzer != "" {
		sb.WriteString(fmt.Sprintf(",\n        \"search_analyzer\": \"%s\"", field.SearchAnalyzer))
	}

	if field.Normalizer != "" {
		sb.WriteString(fmt.Sprintf(",\n        \"normalizer\": \"%s\"", field.Normalizer))
	}

	if !field.Enabled {
		sb.WriteString(",\n        \"enabled\": false")
	}

	if !field.Index {
		sb.WriteString(",\n        \"index\": false")
	}

	if !field.DocValues {
		sb.WriteString(",\n        \"doc_values\": false")
	}

	if !field.Store {
		sb.WriteString(",\n        \"store\": false")
	}

	if field.Boost != 0 {
		sb.WriteString(fmt.Sprintf(",\n        \"boost\": %f", field.Boost))
	}

	if field.NullValue != nil {
		nullValue, _ := json.Marshal(field.NullValue)
		sb.WriteString(fmt.Sprintf(",\n        \"null_value\": %s", string(nullValue)))
	}

	if len(field.CopyTo) > 0 {
		copyTo, _ := json.Marshal(field.CopyTo)
		sb.WriteString(fmt.Sprintf(",\n        \"copy_to\": %s", string(copyTo)))
	}

	if field.IgnoreAbove > 0 {
		sb.WriteString(fmt.Sprintf(",\n        \"ignore_above\": %d", field.IgnoreAbove))
	}

	if field.IgnoreMalformed {
		sb.WriteString(",\n        \"ignore_malformed\": true")
	}

	if !field.Coerce {
		sb.WriteString(",\n        \"coerce\": false")
	}

	if field.Comment != "" {
		sb.WriteString(fmt.Sprintf(",\n        \"comment\": \"%s\"", field.Comment))
	}

	sb.WriteString("\n      }")
	return sb.String()
}

func convertToElasticsearchType(dt models.DataType) string {
	switch strings.ToLower(dt.BaseType) {
	case "varchar", "text", "char":
		return "keyword"
	case "integer", "int", "bigint", "smallint":
		return "long"
	case "decimal", "numeric":
		return "double"
	case "boolean", "bool":
		return "boolean"
	case "date":
		return "date"
	case "timestamp":
		return "date"
	case "json", "jsonb":
		return "object"
	default:
		return "keyword"
	}
}

func (g *ElasticsearchGenerator) GenerateCreateIndex(index models.ElasticsearchIndex) string {
	var sb strings.Builder

	// Create index with settings
	sb.WriteString(fmt.Sprintf("PUT /%s\n", index.Name))
	sb.WriteString("{\n")
	sb.WriteString("  \"settings\": {\n")
	sb.WriteString(fmt.Sprintf("    \"number_of_shards\": %d,\n", index.NumberOfShards))
	sb.WriteString(fmt.Sprintf("    \"number_of_replicas\": %d\n", index.NumberOfReplicas))
	sb.WriteString("  },\n")

	// Add mappings
	sb.WriteString("  \"mappings\": {\n")
	sb.WriteString("    \"properties\": {\n")

	// Add field mappings
	if mappings, ok := index.Mappings["properties"].(map[string]interface{}); ok {
		fieldDefs := make([]string, 0)
		for fieldName, fieldData := range mappings {
			if field, ok := fieldData.(models.ElasticsearchField); ok {
				fieldDef := g.generateFieldMapping(fieldName, field)
				fieldDefs = append(fieldDefs, fieldDef)
			}
		}
		sb.WriteString(strings.Join(fieldDefs, ",\n"))
	}

	sb.WriteString("\n    }\n  }\n}")
	return sb.String()
}

func (g *ElasticsearchGenerator) GenerateCreateAlias(alias models.Alias) string {
	var sb strings.Builder
	sb.WriteString("POST /_aliases\n{\n  \"actions\": [\n")

	actions := make([]string, 0)
	for _, index := range alias.Indices {
		action := fmt.Sprintf("    {\n      \"add\": {\n        \"index\": \"%s\",\n        \"alias\": \"%s\"", index, alias.Name)

		if alias.Filter != nil {
			filter, _ := json.Marshal(alias.Filter)
			action += fmt.Sprintf(",\n        \"filter\": %s", string(filter))
		}

		if alias.Routing != "" {
			action += fmt.Sprintf(",\n        \"routing\": \"%s\"", alias.Routing)
		}

		if alias.IsWriteIndex {
			action += ",\n        \"is_write_index\": true"
		}

		action += "\n      }\n    }"
		actions = append(actions, action)
	}

	sb.WriteString(strings.Join(actions, ",\n"))
	sb.WriteString("\n  ]\n}")
	return sb.String()
}

func (g *ElasticsearchGenerator) GenerateDropIndex(index models.ElasticsearchIndex) string {
	return fmt.Sprintf("DELETE /%s", index.Name)
}

func (g *ElasticsearchGenerator) GenerateDropAlias(alias models.Alias) string {
	var sb strings.Builder
	sb.WriteString("POST /_aliases\n{\n  \"actions\": [\n")

	actions := make([]string, 0)
	for _, index := range alias.Indices {
		action := fmt.Sprintf("    {\n      \"remove\": {\n        \"index\": \"%s\",\n        \"alias\": \"%s\"\n      }\n    }", index, alias.Name)
		actions = append(actions, action)
	}

	sb.WriteString(strings.Join(actions, ",\n"))
	sb.WriteString("\n  ]\n}")
	return sb.String()
}
