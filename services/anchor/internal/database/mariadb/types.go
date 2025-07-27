package mariadb

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// MariaDBDetails contains information about a MariaDB database
type MariaDBDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// MariaDBSchema represents the schema of a MariaDB database
type MariaDBSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	EnumTypes  []common.EnumInfo           `json:"enumTypes"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Extensions []common.ExtensionInfo      `json:"extensions"`
}

// MariaDBReplicationSourceDetails contains details about a MariaDB replication source
type MariaDBReplicationSourceDetails struct {
	BinlogFile     string `json:"binlog_file"`
	BinlogPosition uint32 `json:"binlog_position"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
}

// MariaDBReplicationChange represents a change in MariaDB replication
type MariaDBReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
