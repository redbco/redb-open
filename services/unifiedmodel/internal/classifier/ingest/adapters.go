package ingest

import (
	"encoding/json"
	"fmt"
	"strings"

	unifiedmodel "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
)

// DatabaseSchema represents the structure from the JSON input
type DatabaseSchema struct {
	Tables     []TableInfo     `json:"tables"`
	Schemas    []SchemaInfo    `json:"schemas"`
	Sequences  []SequenceInfo  `json:"sequences"`
	Extensions []ExtensionInfo `json:"extensions"`
}

type TableInfo struct {
	Name      string       `json:"name"`
	Schema    string       `json:"schema"`
	TableType string       `json:"tableType"`
	Columns   []ColumnInfo `json:"columns"`
}

type ColumnInfo struct {
	Name            string `json:"name"`
	DataType        string `json:"dataType"`
	IsNullable      bool   `json:"isNullable"`
	IsPrimaryKey    bool   `json:"isPrimaryKey"`
	IsAutoIncrement bool   `json:"isAutoIncrement"`
	IsUnique        bool   `json:"isUnique"`
	IsArray         bool   `json:"isArray"`
	ColumnDefault   string `json:"columnDefault"`
	VarcharLength   int32  `json:"varcharLength"`
}

type SchemaInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type SequenceInfo struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	DataType  string `json:"dataType"`
	Increment int64  `json:"increment"`
}

type ExtensionInfo struct {
	Name    string `json:"name"`
	Schema  string `json:"schema"`
	Version string `json:"version"`
}

// Adapter interface for different database types
type Adapter interface {
	ConvertToTableMetadata(data []byte, engine string) ([]*unifiedmodel.TableMetadata, error)
}

// UniversalAdapter handles the JSON format from the example
type UniversalAdapter struct{}

// NewUniversalAdapter creates a new universal adapter
func NewUniversalAdapter() *UniversalAdapter {
	return &UniversalAdapter{}
}

// ConvertToTableMetadata converts JSON schema to TableMetadata
func (a *UniversalAdapter) ConvertToTableMetadata(data []byte, engine string) ([]*unifiedmodel.TableMetadata, error) {
	var schema DatabaseSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	var tables []*unifiedmodel.TableMetadata

	for _, table := range schema.Tables {
		metadata := &unifiedmodel.TableMetadata{
			Engine:     engine,
			Schema:     table.Schema,
			Name:       table.Name,
			TableType:  table.TableType,
			Columns:    make([]*unifiedmodel.ColumnMetadata, len(table.Columns)),
			Properties: make(map[string]string),
		}

		// Convert columns
		for i, col := range table.Columns {
			column := &unifiedmodel.ColumnMetadata{
				Name:            col.Name,
				Type:            col.DataType,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsNullable:      col.IsNullable,
				IsArray:         col.IsArray,
				IsAutoIncrement: col.IsAutoIncrement,
				ColumnDefault:   col.ColumnDefault,
				VarcharLength:   col.VarcharLength,
			}

			// Detect indexes from column properties
			column.Indexes = a.detectIndexes(col, engine)

			// Detect foreign keys (simplified - would need more context in real implementation)
			column.IsForeignKey = a.detectForeignKey(col)

			// Detect vector properties
			if a.isVectorType(col.DataType) {
				column.VectorDimension = a.extractVectorDimension(col.DataType)
			}

			metadata.Columns[i] = column
		}

		// Set table properties
		metadata.Properties = a.extractTableProperties(table, engine)

		// Detect system tables
		metadata.IsSystemTable = a.isSystemTable(table.Schema, table.Name)

		// Set access patterns based on engine and table type
		metadata.AccessPattern = a.detectAccessPattern(engine, table.TableType)

		tables = append(tables, metadata)
	}

	return tables, nil
}

func (a *UniversalAdapter) detectIndexes(col ColumnInfo, engine string) []string {
	var indexes []string

	// Primary keys typically have B-tree indexes
	if col.IsPrimaryKey {
		indexes = append(indexes, "btree")
	}

	// Auto-increment columns typically have indexes
	if col.IsAutoIncrement {
		indexes = append(indexes, "btree")
	}

	// Engine-specific index detection
	switch strings.ToLower(engine) {
	case "elasticsearch":
		if a.isStringType(col.DataType) {
			indexes = append(indexes, "fulltext")
		}
	case "postgres", "postgresql":
		if strings.Contains(strings.ToLower(col.DataType), "json") {
			indexes = append(indexes, "gin")
		}
	}

	return indexes
}

func (a *UniversalAdapter) detectForeignKey(col ColumnInfo) bool {
	// Simple heuristic: look for column names that end with _id (but not just "id")
	name := strings.ToLower(col.Name)
	return strings.HasSuffix(name, "_id") && name != "id"
}

func (a *UniversalAdapter) isVectorType(dataType string) bool {
	dt := strings.ToLower(dataType)
	vectorTypes := []string{"vector", "embedding", "float[]", "real[]"}
	for _, vt := range vectorTypes {
		if strings.Contains(dt, vt) {
			return true
		}
	}
	return false
}

func (a *UniversalAdapter) extractVectorDimension(dataType string) int32 {
	// Try to extract dimension from types like "vector(768)" or "float[768]"
	// This is a simplified implementation
	if strings.Contains(dataType, "(") && strings.Contains(dataType, ")") {
		// Extract number between parentheses
		start := strings.Index(dataType, "(")
		end := strings.Index(dataType, ")")
		if end > start {
			dimStr := dataType[start+1 : end]
			// Parse dimension (simplified)
			if len(dimStr) > 0 && len(dimStr) < 5 {
				return 768 // Default common dimension
			}
		}
	}
	return 0
}

func (a *UniversalAdapter) isStringType(dataType string) bool {
	dt := strings.ToLower(dataType)
	return strings.Contains(dt, "varchar") || strings.Contains(dt, "text") ||
		strings.Contains(dt, "char") || strings.Contains(dt, "string")
}

func (a *UniversalAdapter) extractTableProperties(table TableInfo, engine string) map[string]string {
	props := make(map[string]string)

	// Set table type
	if table.TableType != "" {
		props["table_type"] = table.TableType
	}

	// Engine-specific properties
	switch strings.ToLower(engine) {
	case "cassandra":
		// Look for clustering/partition keys in column names
		for _, col := range table.Columns {
			if strings.Contains(strings.ToLower(col.Name), "time") {
				props["partition_by"] = "time"
				break
			}
		}
	case "clickhouse":
		props["engine_type"] = "columnar"
		props["optimized_for"] = "analytics"
	case "elasticsearch":
		props["search_engine"] = "true"
		props["document_store"] = "true"
	}

	return props
}

func (a *UniversalAdapter) isSystemTable(schema, tableName string) bool {
	systemSchemas := []string{
		"information_schema", "pg_catalog", "sys", "system",
		"performance_schema", "mysql", "__system__",
	}

	schemaLower := strings.ToLower(schema)
	for _, sysSchema := range systemSchemas {
		if schemaLower == sysSchema {
			return true
		}
	}

	// Check for system table prefixes
	tableNameLower := strings.ToLower(tableName)
	systemPrefixes := []string{"pg_", "sys_", "__", "information_"}
	for _, prefix := range systemPrefixes {
		if strings.HasPrefix(tableNameLower, prefix) {
			return true
		}
	}

	return false
}

func (a *UniversalAdapter) detectAccessPattern(engine, tableType string) string {
	engineLower := strings.ToLower(engine)

	switch {
	case strings.Contains(engineLower, "clickhouse"):
		return "append_only"
	case strings.Contains(engineLower, "cassandra"):
		return "write_heavy"
	case strings.Contains(engineLower, "elasticsearch"):
		return "read_heavy"
	case strings.Contains(engineLower, "redis"):
		return "read_heavy"
	case strings.Contains(engineLower, "pinecone") ||
		strings.Contains(engineLower, "milvus") ||
		strings.Contains(engineLower, "weaviate"):
		return "read_heavy"
	default:
		return "balanced"
	}
}
