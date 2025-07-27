package mssql

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// MSSQLDetails contains information about a Microsoft SQL Server database
type MSSQLDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// MSSQLSchema represents the schema of a Microsoft SQL Server database
type MSSQLSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Procedures []common.ProcedureInfo      `json:"procedures"`
	Views      []common.ViewInfo           `json:"views"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
}

type MSSQLReplicationSourceDetails struct {
	PublicationName string `json:"publication_name"`
	TableName       string `json:"table_name"`
	DatabaseID      string `json:"database_id"`
	SubscriptionID  string `json:"subscription_id"`
}

type MSSQLReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
