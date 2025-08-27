package mariadb

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateMariaDBUnifiedModel creates a UnifiedModel for MariaDB with database details
func CreateMariaDBUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MariaDB,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Types:        make(map[string]unifiedmodel.Type),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}
	return um
}

// ConvertMariaDBTableToUnified converts common.TableInfo to unifiedmodel.Table for MariaDB
func ConvertMariaDBTableToUnified(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns
	for _, col := range tableInfo.Columns {
		column := unifiedmodel.Column{
			Name:          col.Name,
			DataType:      col.DataType,
			Nullable:      col.IsNullable,
			IsPrimaryKey:  col.IsPrimaryKey,
			AutoIncrement: col.IsAutoIncrement,
		}
		if col.ColumnDefault != nil {
			column.Default = *col.ColumnDefault
		}
		if col.GenerationExpression != nil {
			column.GeneratedExpression = *col.GenerationExpression
		}
		table.Columns[col.Name] = column
	}

	// Convert indexes
	for _, idx := range tableInfo.Indexes {
		index := unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
		table.Indexes[idx.Name] = index
	}

	// Convert constraints
	for _, constraint := range tableInfo.Constraints {
		constraintType := unifiedmodel.ConstraintTypePrimaryKey
		switch constraint.Type {
		case "PRIMARY KEY":
			constraintType = unifiedmodel.ConstraintTypePrimaryKey
		case "FOREIGN KEY":
			constraintType = unifiedmodel.ConstraintTypeForeignKey
		case "UNIQUE":
			constraintType = unifiedmodel.ConstraintTypeUnique
		case "CHECK":
			constraintType = unifiedmodel.ConstraintTypeCheck
		case "NOT NULL":
			constraintType = unifiedmodel.ConstraintTypeNotNull
		}

		unifiedConstraint := unifiedmodel.Constraint{
			Name:    constraint.Name,
			Type:    constraintType,
			Columns: []string{constraint.Column},
		}

		if constraint.ForeignTable != "" {
			unifiedConstraint.Reference = unifiedmodel.Reference{
				Table:    constraint.ForeignTable,
				Columns:  []string{constraint.ForeignColumn},
				OnUpdate: constraint.OnUpdate,
				OnDelete: constraint.OnDelete,
			}
		}

		table.Constraints[constraint.Name] = unifiedConstraint
	}

	return table
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
