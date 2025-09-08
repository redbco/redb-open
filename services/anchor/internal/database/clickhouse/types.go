package clickhouse

import (
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

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
