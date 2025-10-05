package postgres

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for PostgreSQL.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the complete schema of the PostgreSQL database.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Use existing DiscoverSchema function
	um, err := DiscoverSchema(s.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "discover_schema", err)
	}
	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	// Use existing CreateStructure function
	err := CreateStructure(s.conn.pool, model)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "create_structure", err)
	}
	return nil
}

// ListTables returns the names of all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := s.conn.pool.Query(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_tables", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_tables", err)
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
			dbcapabilities.PostgreSQL,
			"get_table_schema",
			adapter.ErrTableNotFound,
		).WithContext("table", tableName)
	}

	return &table, nil
}
