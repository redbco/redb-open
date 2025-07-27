package mysql

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// MySQLDetails contains information about a MySQL database
type MySQLDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// MySQLSchema represents the schema of a MySQL database
type MySQLSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	EnumTypes  []common.EnumInfo           `json:"enumTypes"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Extensions []common.ExtensionInfo      `json:"extensions"`
}

// MySQLReplicationSourceDetails contains details about a MySQL replication source
type MySQLReplicationSourceDetails struct {
	BinlogFile     string `json:"binlog_file"`
	BinlogPosition uint32 `json:"binlog_position"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
}

// MySQLReplicationChange represents a change in MySQL replication
type MySQLReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
