package redshift

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Redshift.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of Redshift database (tables and their schemas).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Query to list all tables in the current schema
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := s.conn.client.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	tablesMap := make(map[string]unifiedmodel.Table)

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		// Get table schema
		table, err := s.GetTableSchema(ctx, tableName)
		if err != nil {
			continue
		}

		tablesMap[tableName] = *table
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	model := &unifiedmodel.UnifiedModel{
		DatabaseType: s.conn.Type(),
		Tables:       tablesMap,
	}

	return model, nil
}

// CreateStructure creates Redshift structure from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	for tableName, table := range model.Tables {
		// Build CREATE TABLE statement
		query := fmt.Sprintf("CREATE TABLE %s (", tableName)

		first := true
		for colName, col := range table.Columns {
			if !first {
				query += ", "
			}
			first = false

			query += fmt.Sprintf("%s %s", colName, s.mapUnifiedTypeToRedshift(col.DataType))

			if !col.Nullable {
				query += " NOT NULL"
			}
		}

		query += ")"

		_, err := s.conn.client.DB().ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	return nil
}

// mapUnifiedTypeToRedshift maps unified data types to Redshift data types.
func (s *SchemaOps) mapUnifiedTypeToRedshift(dataType string) string {
	switch dataType {
	case "string", "text":
		return "VARCHAR(65535)"
	case "integer", "int":
		return "INTEGER"
	case "bigint":
		return "BIGINT"
	case "float", "double":
		return "DOUBLE PRECISION"
	case "boolean", "bool":
		return "BOOLEAN"
	case "timestamp":
		return "TIMESTAMP"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "bytes", "binary":
		return "VARBYTE"
	case "numeric", "decimal":
		return "DECIMAL(18,2)"
	case "json":
		return "VARCHAR(65535)" // Redshift doesn't have native JSON type
	default:
		return "VARCHAR(65535)"
	}
}

// ListTables lists all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := s.conn.client.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := s.conn.client.DB().QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	columnsMap := make(map[string]unifiedmodel.Column)

	for rows.Next() {
		var columnName, dataType, isNullable string
		if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
			continue
		}

		column := unifiedmodel.Column{
			Name:     columnName,
			DataType: s.mapRedshiftTypeToUnified(dataType),
			Nullable: isNullable == "YES",
		}

		columnsMap[columnName] = column
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	table := &unifiedmodel.Table{
		Name:    tableName,
		Columns: columnsMap,
	}

	return table, nil
}

// mapRedshiftTypeToUnified maps Redshift data types to unified data types.
func (s *SchemaOps) mapRedshiftTypeToUnified(dataType string) string {
	switch dataType {
	case "character varying", "varchar", "character", "char", "text":
		return "string"
	case "integer", "int", "int4":
		return "integer"
	case "bigint", "int8":
		return "bigint"
	case "smallint", "int2":
		return "integer"
	case "double precision", "float8":
		return "float"
	case "real", "float4":
		return "float"
	case "boolean", "bool":
		return "boolean"
	case "timestamp without time zone", "timestamp":
		return "timestamp"
	case "timestamp with time zone", "timestamptz":
		return "timestamp"
	case "date":
		return "date"
	case "time without time zone", "time":
		return "time"
	case "numeric", "decimal":
		return "numeric"
	case "varbyte", "bytea":
		return "bytes"
	default:
		return "string"
	}
}
