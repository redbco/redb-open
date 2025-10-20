package synapse

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements schema operations for Synapse.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the schema of Synapse database (tables and their schemas).
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	// Query to list all tables in the current database
	query := `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
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

// CreateStructure creates Synapse structure from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	for tableName, table := range model.Tables {
		// Build CREATE TABLE statement
		query := fmt.Sprintf("CREATE TABLE [%s] (", tableName)

		first := true
		for colName, col := range table.Columns {
			if !first {
				query += ", "
			}
			first = false

			query += fmt.Sprintf("[%s] %s", colName, s.mapUnifiedTypeToSynapse(col.DataType))

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

// mapUnifiedTypeToSynapse maps unified data types to Synapse data types.
func (s *SchemaOps) mapUnifiedTypeToSynapse(dataType string) string {
	switch dataType {
	case "string", "text":
		return "NVARCHAR(MAX)"
	case "integer", "int":
		return "INT"
	case "bigint":
		return "BIGINT"
	case "float", "double":
		return "FLOAT"
	case "boolean", "bool":
		return "BIT"
	case "timestamp":
		return "DATETIME2"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "bytes", "binary":
		return "VARBINARY(MAX)"
	case "numeric", "decimal":
		return "DECIMAL(18,2)"
	case "json":
		return "NVARCHAR(MAX)" // Store JSON as string
	default:
		return "NVARCHAR(MAX)"
	}
}

// ListTables lists all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
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
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = @p1
		ORDER BY ORDINAL_POSITION
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
			DataType: s.mapSynapseTypeToUnified(dataType),
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

// mapSynapseTypeToUnified maps Synapse data types to unified data types.
func (s *SchemaOps) mapSynapseTypeToUnified(dataType string) string {
	switch dataType {
	case "nvarchar", "varchar", "nchar", "char", "text", "ntext":
		return "string"
	case "int", "smallint", "tinyint":
		return "integer"
	case "bigint":
		return "bigint"
	case "float", "real":
		return "float"
	case "bit":
		return "boolean"
	case "datetime", "datetime2", "smalldatetime":
		return "timestamp"
	case "date":
		return "date"
	case "time":
		return "time"
	case "decimal", "numeric", "money", "smallmoney":
		return "numeric"
	case "binary", "varbinary", "image":
		return "bytes"
	case "uniqueidentifier":
		return "string"
	default:
		return "string"
	}
}
