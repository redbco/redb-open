package oracle

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// OracleDetails contains information about an Oracle database
type OracleDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// OracleSchema represents the schema of an Oracle database
type OracleSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	Types      []common.TypeInfo           `json:"types"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Procedures []common.ProcedureInfo      `json:"procedures"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Packages   []common.PackageInfo        `json:"packages"`
}

// OracleReplicationSourceDetails holds information about a replication source
type OracleReplicationSourceDetails struct {
	LogMinerSessionID string `json:"logminer_session_id"`
	TableName         string `json:"table_name"`
	DatabaseID        string `json:"database_id"`
	LastSCN           int64  `json:"last_scn"`
}

// OracleReplicationChange represents a change in the database
type OracleReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
