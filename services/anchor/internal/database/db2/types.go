package db2

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateDb2UnifiedModel creates a UnifiedModel for DB2 with database details
func CreateDb2UnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.DB2,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Indexes:      make(map[string]unifiedmodel.Index),
	}
	return um
}

// ConvertDb2Table converts common.TableInfo to unifiedmodel.Table for DB2
func ConvertDb2Table(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Comment:     tableInfo.Schema, // Store schema in comment for reference
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns
	for _, col := range tableInfo.Columns {
		var defaultValue string
		if col.ColumnDefault != nil {
			defaultValue = *col.ColumnDefault
		}
		table.Columns[col.Name] = unifiedmodel.Column{
			Name:         col.Name,
			DataType:     col.DataType,
			Nullable:     col.IsNullable,
			Default:      defaultValue,
			IsPrimaryKey: col.IsPrimaryKey,
		}
	}

	// Convert indexes
	for _, idx := range tableInfo.Indexes {
		table.Indexes[idx.Name] = unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
	}

	// Convert constraints
	for _, constraint := range tableInfo.Constraints {
		// Map string constraint type to ConstraintType enum
		var constraintType unifiedmodel.ConstraintType
		switch constraint.Type {
		case "PRIMARY KEY":
			constraintType = unifiedmodel.ConstraintTypePrimaryKey
		case "FOREIGN KEY":
			constraintType = unifiedmodel.ConstraintTypeForeignKey
		case "UNIQUE":
			constraintType = unifiedmodel.ConstraintTypeUnique
		case "CHECK":
			constraintType = unifiedmodel.ConstraintTypeCheck
		default:
			constraintType = unifiedmodel.ConstraintTypeCheck // Default fallback
		}

		table.Constraints[constraint.Name] = unifiedmodel.Constraint{
			Name:       constraint.Name,
			Type:       constraintType,
			Expression: constraint.Definition,
		}
	}

	return table
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
