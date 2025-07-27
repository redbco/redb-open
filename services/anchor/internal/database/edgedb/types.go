package edgedb

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// EdgeDBDetails contains information about an EdgeDB database
type EdgeDBDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
}

// EdgeDBSchema represents the schema of an EdgeDB database
type EdgeDBSchema struct {
	Types       []common.TypeInfo      `json:"types"`
	Modules     []common.ModuleInfo    `json:"modules"`
	Functions   []common.FunctionInfo  `json:"functions"`
	Extensions  []common.ExtensionInfo `json:"extensions"`
	Scalars     []EdgeDBScalarInfo     `json:"scalars"`
	Aliases     []EdgeDBAliasInfo      `json:"aliases"`
	Constraints []EdgeDBConstraintInfo `json:"constraints"`
}

// EdgeDBScalarInfo represents a scalar type in EdgeDB
type EdgeDBScalarInfo struct {
	Module      string                        `json:"module"`
	Name        string                        `json:"name"`
	BaseType    string                        `json:"baseType"`
	Constraints []common.EdgeDBConstraintInfo `json:"constraints"`
}

// EdgeDBAliasInfo represents an alias type in EdgeDB
type EdgeDBAliasInfo struct {
	Module string `json:"module"`
	Name   string `json:"name"`
	Type   string `json:"type"`
}

// EdgeDBConstraintInfo represents a constraint in EdgeDB
type EdgeDBConstraintInfo struct {
	Module      string      `json:"module"`
	Name        string      `json:"name"`
	ParamTypes  []string    `json:"paramTypes,omitempty"`
	ReturnType  string      `json:"returnType"`
	Description string      `json:"description,omitempty"`
	Args        interface{} `json:"args,omitempty"`
}

// EdgeDBReplicationSourceDetails contains information about a replication source
type EdgeDBReplicationSourceDetails struct {
	SourceID   string `json:"source_id"`
	ModuleName string `json:"module_name"`
	TypeName   string `json:"type_name"`
	DatabaseID string `json:"database_id"`
}

// EdgeDBReplicationChange represents a change in the database
type EdgeDBReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
