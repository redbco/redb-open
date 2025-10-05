package db2

// Db2ReplicationSourceDetails contains information about a Db2 replication source
type Db2ReplicationSourceDetails struct {
	SourceSchema string `json:"source_schema"`
	SourceTable  string `json:"source_table"`
	DatabaseID   string `json:"database_id"`
}

// Db2ReplicationChange represents a change in a Db2 replication stream
type Db2ReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
