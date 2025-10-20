package databricks

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Databricks.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of Databricks database (Delta tables).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Query to list all tables in the current database
	query := "SHOW TABLES"

	rows, err := s.conn.client.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	tablesMap := make(map[string]unifiedmodel.Table)

	for rows.Next() {
		var database, tableName, isTemporary string
		if err := rows.Scan(&database, &tableName, &isTemporary); err != nil {
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

// CreateStructure creates Databricks structure from a UnifiedModel.
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

			query += fmt.Sprintf("%s %s", colName, s.mapUnifiedTypeToDatabricks(col.DataType))

			if !col.Nullable {
				query += " NOT NULL"
			}
		}

		query += ") USING DELTA" // Use Delta Lake format

		_, err := s.conn.client.DB().ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}
	}

	return nil
}

// mapUnifiedTypeToDatabricks maps unified data types to Databricks data types.
func (s *SchemaOps) mapUnifiedTypeToDatabricks(dataType string) string {
	switch dataType {
	case "string", "text":
		return "STRING"
	case "integer", "int":
		return "INT"
	case "bigint":
		return "BIGINT"
	case "float", "double":
		return "DOUBLE"
	case "boolean", "bool":
		return "BOOLEAN"
	case "timestamp":
		return "TIMESTAMP"
	case "date":
		return "DATE"
	case "bytes", "binary":
		return "BINARY"
	case "numeric", "decimal":
		return "DECIMAL(18,2)"
	case "json":
		return "STRING" // Store JSON as string, can also use STRUCT
	default:
		return "STRING"
	}
}

// ListTables lists all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := "SHOW TABLES"

	rows, err := s.conn.client.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var database, tableName, isTemporary string
		if err := rows.Scan(&database, &tableName, &isTemporary); err != nil {
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
	query := fmt.Sprintf("DESCRIBE TABLE %s", tableName)

	rows, err := s.conn.client.DB().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	columnsMap := make(map[string]unifiedmodel.Column)

	for rows.Next() {
		var columnName, dataType, comment sql.NullString
		if err := rows.Scan(&columnName, &dataType, &comment); err != nil {
			continue
		}

		if !columnName.Valid || !dataType.Valid {
			continue
		}

		column := unifiedmodel.Column{
			Name:     columnName.String,
			DataType: s.mapDatabricksTypeToUnified(dataType.String),
			Nullable: true, // Databricks doesn't easily expose nullability in DESCRIBE
		}

		columnsMap[columnName.String] = column
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

// mapDatabricksTypeToUnified maps Databricks data types to unified data types.
func (s *SchemaOps) mapDatabricksTypeToUnified(dataType string) string {
	switch dataType {
	case "STRING", "VARCHAR", "CHAR":
		return "string"
	case "INT", "INTEGER", "SMALLINT", "TINYINT":
		return "integer"
	case "BIGINT", "LONG":
		return "bigint"
	case "FLOAT", "REAL", "DOUBLE":
		return "float"
	case "BOOLEAN":
		return "boolean"
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		return "timestamp"
	case "DATE":
		return "date"
	case "DECIMAL", "NUMERIC":
		return "numeric"
	case "BINARY":
		return "bytes"
	default:
		return "string"
	}
}
