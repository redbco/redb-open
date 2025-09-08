package edgedb

import (
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// ConvertEdgeDBScalar converts EdgeDBScalarInfo to unifiedmodel.Type
func ConvertEdgeDBScalar(scalarInfo EdgeDBScalarInfo) unifiedmodel.Type {
	return unifiedmodel.Type{
		Name:     scalarInfo.Name,
		Category: "scalar",
		Definition: map[string]any{
			"module":      scalarInfo.Module,
			"baseType":    scalarInfo.BaseType,
			"constraints": scalarInfo.Constraints,
		},
	}
}

// ConvertEdgeDBAlias converts EdgeDBAliasInfo to unifiedmodel.Type
func ConvertEdgeDBAlias(aliasInfo EdgeDBAliasInfo) unifiedmodel.Type {
	return unifiedmodel.Type{
		Name:     aliasInfo.Name,
		Category: "alias",
		Definition: map[string]any{
			"module":     aliasInfo.Module,
			"targetType": aliasInfo.Type,
		},
	}
}

// ConvertEdgeDBConstraint converts EdgeDBConstraintInfo to unifiedmodel.Constraint
func ConvertEdgeDBConstraint(constraintInfo EdgeDBConstraintInfo) unifiedmodel.Constraint {
	return unifiedmodel.Constraint{
		Name:    constraintInfo.Name,
		Type:    unifiedmodel.ConstraintTypeCheck, // EdgeDB constraints are typically check constraints
		Columns: []string{},                       // EdgeDB constraints are more complex and don't map directly to columns
	}
}

// EdgeDBScalarInfo represents a scalar type in EdgeDB
type EdgeDBScalarInfo struct {
	Module      string                 `json:"module"`
	Name        string                 `json:"name"`
	BaseType    string                 `json:"baseType"`
	Constraints []EdgeDBConstraintInfo `json:"constraints"`
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
