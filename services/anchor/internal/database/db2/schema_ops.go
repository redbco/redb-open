//go:build enterprise
// +build enterprise

package db2

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for IBM DB2.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the complete schema of the IBM DB2 database.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Use existing DiscoverSchema function from schema.go
	um, err := DiscoverSchema(s.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "discover_schema", err)
	}

	// Set database type
	um.DatabaseType = dbcapabilities.DB2

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// Use existing CreateStructure function from schema.go
	err := CreateStructure(s.conn.db, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DB2, "create_structure", err)
	}
	return nil
}

// ListTables returns the names of all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT TABNAME 
		FROM SYSCAT.TABLES 
		WHERE TYPE = 'T' 
		AND TABSCHEMA NOT LIKE 'SYS%'
		ORDER BY TABNAME
	`

	rows, err := s.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "list_tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.DB2, "list_tables", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "list_tables", err)
	}

	return tables, nil
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	// Discover full schema and extract specific table
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	table, exists := um.Tables[tableName]
	if !exists {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.DB2,
			"get_table_schema",
			adapter.ErrTableNotFound,
		).WithContext("table", tableName)
	}

	return &table, nil
}
