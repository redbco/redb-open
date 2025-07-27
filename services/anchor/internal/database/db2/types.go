package db2

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// Db2Details contains information about an IBM Db2 database
type Db2Details struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// Db2Schema represents the schema of an IBM Db2 database
type Db2Schema struct {
	Tables     []common.TableInfo          `json:"tables"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Procedures []common.ProcedureInfo      `json:"procedures"`
}

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
