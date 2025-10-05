package mysql

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for MySQL.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the complete schema of the database as a UnifiedModel.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Use existing DiscoverSchema function
	um, err := DiscoverSchema(s.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "discover_schema", err)
	}
	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// Use existing CreateStructure function
	err := CreateStructure(s.conn.db, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "create_structure", err)
	}
	return nil
}

// ListTables returns the names of all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := "SHOW TABLES"

	rows, err := s.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "list_tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.MySQL, "list_tables", err)
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	// Discover full schema and extract the requested table
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	// Find the table in the unified model
	for _, table := range um.Tables {
		if table.Name == tableName {
			return &table, nil
		}
	}

	return nil, adapter.NewNotFoundError(dbcapabilities.MySQL, "table", tableName)
}
