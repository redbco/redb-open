package clickhouse

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateClickHouseUnifiedModel creates a UnifiedModel for ClickHouse with database details
func CreateClickHouseUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.ClickHouse,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Views:        make(map[string]unifiedmodel.View),
	}
	return um
}

// ConvertClickHouseTable converts common.TableInfo to unifiedmodel.Table for ClickHouse
func ConvertClickHouseTable(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Comment:     tableInfo.Schema, // Store database name in comment
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns
	for _, col := range tableInfo.Columns {
		var defaultValue string
		if col.ColumnDefault != nil {
			defaultValue = *col.ColumnDefault
		}
		table.Columns[col.Name] = unifiedmodel.Column{
			Name:         col.Name,
			DataType:     col.DataType,
			Nullable:     col.IsNullable,
			Default:      defaultValue,
			IsPrimaryKey: col.IsPrimaryKey,
		}
	}

	// Convert indexes
	for _, idx := range tableInfo.Indexes {
		table.Indexes[idx.Name] = unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
	}

	return table
}

// ConvertClickHouseDictionary converts ClickhouseDictionaryInfo to unifiedmodel.View (as dictionaries are similar to materialized views)
func ConvertClickHouseDictionary(dictInfo ClickhouseDictionaryInfo) unifiedmodel.View {
	return unifiedmodel.View{
		Name:       dictInfo.Name,
		Definition: dictInfo.Definition,
		Comment:    dictInfo.Description,
	}
}

// ClickhouseDictionaryInfo represents a Clickhouse dictionary
type ClickhouseDictionaryInfo struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	Source      string `json:"source"`
	Layout      string `json:"layout"`
	Definition  string `json:"definition"`
	Description string `json:"description,omitempty"`
}

// ClickhouseEngineInfo represents information about a Clickhouse table engine
type ClickhouseEngineInfo struct {
	Name       string                 `json:"name"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// ClickhouseClusterInfo represents information about a Clickhouse cluster
type ClickhouseClusterInfo struct {
	Name  string `json:"name"`
	Nodes int    `json:"nodes"`
}

// ClickhouseReplicationSourceDetails contains details for replication
type ClickhouseReplicationSourceDetails struct {
	TableName  string `json:"table_name"`
	DatabaseID string `json:"database_id"`
}

// ClickhouseReplicationChange represents a change in replication
type ClickhouseReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
