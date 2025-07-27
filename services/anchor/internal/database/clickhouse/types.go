package clickhouse

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// ClickhouseDetails contains information about a Clickhouse database
type ClickhouseDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// ClickhouseSchema represents the schema of a Clickhouse database
type ClickhouseSchema struct {
	Tables       []common.TableInfo          `json:"tables"`
	Schemas      []common.DatabaseSchemaInfo `json:"schemas"`
	Functions    []common.FunctionInfo       `json:"functions"`
	Views        []common.ViewInfo           `json:"views"`
	Dictionaries []ClickhouseDictionaryInfo  `json:"dictionaries"`
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
