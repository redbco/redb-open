package mssql

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateMSSQLUnifiedModel creates a UnifiedModel for Microsoft SQL Server with database details
func CreateMSSQLUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.SQLServer,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Views:        make(map[string]unifiedmodel.View),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Indexes:      make(map[string]unifiedmodel.Index),
	}
	return um
}

// ConvertMSSQLTable converts common.TableInfo to unifiedmodel.Table for Microsoft SQL Server
func ConvertMSSQLTable(tableInfo common.TableInfo) unifiedmodel.Table {
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
