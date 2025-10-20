//go:build enterprise
// +build enterprise

package db2

// Db2ReplicationSourceDetails contains information about a DB2 replication source
type Db2ReplicationSourceDetails struct {
	SourceSchema string `json:"source_schema"`
	SourceTable  string `json:"source_table"`
	DatabaseID   string `json:"database_id"`
	LastChangeID int    `json:"last_change_id,omitempty"` // Track position in change log
}

// Db2ReplicationChange represents a change in a DB2 replication stream
type Db2ReplicationChange struct {
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}
