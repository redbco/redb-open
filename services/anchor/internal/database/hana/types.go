//go:build enterprise
// +build enterprise

package hana

// HanaReplicationSourceDetails holds information about a SAP HANA replication source
type HanaReplicationSourceDetails struct {
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	DatabaseID   string `json:"database_id"`
	LastCommitID int64  `json:"last_commit_id"` // For positioning in change log
}

// HanaReplicationChange represents a change in the SAP HANA database
type HanaReplicationChange struct {
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}
