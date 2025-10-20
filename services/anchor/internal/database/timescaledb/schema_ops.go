package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for TimescaleDB.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of the TimescaleDB database.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	tables, err := s.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	tablesMap := make(map[string]unifiedmodel.Table)

	for _, tableName := range tables {
		table, err := s.GetTableSchema(ctx, tableName)
		if err != nil {
			// Log error but continue with other tables
			continue
		}
		tablesMap[tableName] = *table
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
	}

	return model, nil
}

// CreateStructure creates the database structure from a unified model.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	for tableName, table := range model.Tables {
		if err := s.createTable(ctx, tableName, table); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}
	return nil
}

// ListTables lists all tables in the database (including hypertables).
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public'
		ORDER BY tablename
	`

	rows, err := s.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	query := `
		SELECT 
			column_name, 
			data_type, 
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := s.conn.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	columnsMap := make(map[string]unifiedmodel.Column)

	for rows.Next() {
		var columnName, dataType, isNullable string
		var columnDefault sql.NullString

		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault); err != nil {
			return nil, err
		}

		column := unifiedmodel.Column{
			Name:     columnName,
			DataType: dataType,
			Nullable: isNullable == "YES",
		}

		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		columnsMap[columnName] = column
	}

	// Check if this is a hypertable
	isHypertable, timeColumn := s.isHypertable(ctx, tableName)

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
	}

	// Add metadata for hypertables
	if isHypertable {
		if table.Options == nil {
			table.Options = make(map[string]any)
		}
		table.Options["is_hypertable"] = true
		table.Options["time_column"] = timeColumn
	}

	return table, rows.Err()
}

// isHypertable checks if a table is a TimescaleDB hypertable.
func (s *SchemaOps) isHypertable(ctx context.Context, tableName string) (bool, string) {
	query := `
		SELECT 
			h.table_name,
			d.column_name
		FROM _timescaledb_catalog.hypertable h
		JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
		WHERE h.table_name = $1 AND d.column_type = 'time'
		LIMIT 1
	`

	var table, timeColumn string
	err := s.conn.db.QueryRowContext(ctx, query, tableName).Scan(&table, &timeColumn)
	if err != nil {
		return false, ""
	}

	return true, timeColumn
}

// createTable creates a table from the unified model.
func (s *SchemaOps) createTable(ctx context.Context, tableName string, table unifiedmodel.Table) error {
	var columnDefs []string

	for _, column := range table.Columns {
		def := fmt.Sprintf("%s %s", column.Name, column.DataType)

		if !column.Nullable {
			def += " NOT NULL"
		}

		if column.Default != "" {
			def += fmt.Sprintf(" DEFAULT %s", column.Default)
		}

		columnDefs = append(columnDefs, def)
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		tableName,
		strings.Join(columnDefs, ", "),
	)

	_, err := s.conn.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Convert to hypertable if metadata indicates it should be
	if table.Options != nil {
		if isHypertable, ok := table.Options["is_hypertable"].(bool); ok && isHypertable {
			if timeColumn, ok := table.Options["time_column"].(string); ok {
				if err := s.createHypertable(ctx, tableName, timeColumn); err != nil {
					return fmt.Errorf("failed to create hypertable: %w", err)
				}
			}
		}
	}

	return nil
}

// createHypertable converts a regular table to a TimescaleDB hypertable.
func (s *SchemaOps) createHypertable(ctx context.Context, tableName, timeColumn string) error {
	query := fmt.Sprintf("SELECT create_hypertable('%s', '%s')", tableName, timeColumn)
	_, err := s.conn.db.ExecContext(ctx, query)
	return err
}

// ListHypertables returns all hypertables in the database.
func (s *SchemaOps) ListHypertables(ctx context.Context) ([]string, error) {
	query := `
		SELECT table_name 
		FROM _timescaledb_catalog.hypertable
		WHERE schema_name = 'public'
		ORDER BY table_name
	`

	rows, err := s.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list hypertables: %w", err)
	}
	defer rows.Close()

	var hypertables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		hypertables = append(hypertables, tableName)
	}

	return hypertables, rows.Err()
}
