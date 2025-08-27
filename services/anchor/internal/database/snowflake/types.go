package snowflake

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateSnowflakeUnifiedModel creates a UnifiedModel for Snowflake with database details
func CreateSnowflakeUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.Snowflake,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Views:        make(map[string]unifiedmodel.View),
	}
	return um
}

// ConvertSnowflakeTable converts common.TableInfo to unifiedmodel.Table for Snowflake
func ConvertSnowflakeTable(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Comment:     tableInfo.Schema, // Store schema name in comment
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

// ConvertSnowflakeStage converts SnowflakeStageInfo to unifiedmodel.ExternalTable (stages are similar to external tables)
func ConvertSnowflakeStage(stageInfo SnowflakeStageInfo) unifiedmodel.ExternalTable {
	return unifiedmodel.ExternalTable{
		Name:     stageInfo.Name,
		Location: stageInfo.URL,
		Format:   stageInfo.StageType,
	}
}

// ConvertSnowflakePipe converts SnowflakePipeInfo to unifiedmodel.Function (pipes are data loading functions)
func ConvertSnowflakePipe(pipeInfo SnowflakePipeInfo) unifiedmodel.Function {
	return unifiedmodel.Function{
		Name:       pipeInfo.Name,
		Language:   "sql", // Snowflake uses SQL
		Definition: pipeInfo.Definition,
	}
}

// SnowflakeStageInfo represents a Snowflake stage
type SnowflakeStageInfo struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	Database    string `json:"database"`
	URL         string `json:"url,omitempty"`
	StageType   string `json:"stageType"` // INTERNAL, EXTERNAL, etc.
	Credentials string `json:"credentials,omitempty"`
}

// SnowflakeWarehouseInfo represents a Snowflake warehouse
type SnowflakeWarehouseInfo struct {
	Name            string `json:"name"`
	Size            string `json:"size"` // X-Small, Small, Medium, Large, etc.
	MinClusterCount int    `json:"minClusterCount"`
	MaxClusterCount int    `json:"maxClusterCount"`
	AutoSuspend     int    `json:"autoSuspend"` // seconds
	AutoResume      bool   `json:"autoResume"`
	State           string `json:"state"` // STARTED, SUSPENDED, etc.
}

// SnowflakePipeInfo represents a Snowflake pipe
type SnowflakePipeInfo struct {
	Name                string `json:"name"`
	Schema              string `json:"schema"`
	Database            string `json:"database"`
	Definition          string `json:"definition"`
	PipeType            string `json:"pipeType"` // STANDARD, SNOWPIPE_STREAMING
	Owner               string `json:"owner"`
	NotificationChannel string `json:"notificationChannel,omitempty"`
}

// SnowflakeStreamChange represents a change captured by Snowflake streams
type SnowflakeStreamChange struct {
	Operation string
	Data      map[string]interface{}
	Metadata  map[string]interface{}
}

// SnowflakeStreamInfo represents a Snowflake stream
type SnowflakeStreamInfo struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Database  string `json:"database"`
	TableName string `json:"tableName"`
	Mode      string `json:"mode"` // APPEND_ONLY, INSERT_ONLY, etc.
	Owner     string `json:"owner"`
}
