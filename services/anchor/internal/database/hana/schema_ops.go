//go:build enterprise
// +build enterprise

package hana

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for SAP HANA.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema retrieves the complete schema of the SAP HANA database.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.HANA,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	// Discover tables
	if err := discoverTables(ctx, s.conn.db, um); err != nil {
		return nil, adapter.WrapError(dbcapabilities.HANA, "discover_schema", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	if model == nil {
		return adapter.NewDatabaseError(
			dbcapabilities.HANA,
			"create_structure",
			adapter.ErrInvalidData,
		).WithContext("error", "unified model cannot be nil")
	}

	// Create tables
	for _, table := range model.Tables {
		if err := createTable(ctx, s.conn.db, table); err != nil {
			return adapter.WrapError(dbcapabilities.HANA, "create_structure", err)
		}
	}

	return nil
}

// ListTables returns the names of all tables in the database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT TABLE_NAME 
		FROM SYS.TABLES 
		WHERE SCHEMA_NAME = CURRENT_SCHEMA
		ORDER BY TABLE_NAME
	`

	rows, err := s.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.HANA, "list_tables", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.HANA, "list_tables", err)
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// GetTableSchema retrieves the schema for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
	um, err := s.DiscoverSchema(ctx)
	if err != nil {
		return nil, err
	}

	table, exists := um.Tables[tableName]
	if !exists {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.HANA,
			"get_table_schema",
			adapter.ErrTableNotFound,
		).WithContext("table", tableName)
	}

	return &table, nil
}

func discoverTables(ctx context.Context, db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	// Query to get tables and columns from SAP HANA
	query := `
		SELECT 
			t.TABLE_NAME,
			c.COLUMN_NAME,
			c.DATA_TYPE_NAME,
			c.LENGTH,
			c.SCALE,
			CASE WHEN c.IS_NULLABLE = 'TRUE' THEN 1 ELSE 0 END AS IS_NULLABLE,
			COALESCE(c.DEFAULT_VALUE, '') AS DEFAULT_VALUE,
			c.POSITION,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
		FROM SYS.TABLES t
		INNER JOIN SYS.TABLE_COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.SCHEMA_NAME = c.SCHEMA_NAME
		LEFT JOIN (
			SELECT DISTINCT tc.TABLE_NAME, tc.SCHEMA_NAME, tc.COLUMN_NAME
			FROM SYS.CONSTRAINTS cs
			INNER JOIN SYS.CONSTRAINT_COLUMNS tc ON cs.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND cs.SCHEMA_NAME = tc.SCHEMA_NAME
			WHERE cs.IS_PRIMARY_KEY = 'TRUE'
		) pk ON t.TABLE_NAME = pk.TABLE_NAME AND t.SCHEMA_NAME = pk.SCHEMA_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
		WHERE t.SCHEMA_NAME = CURRENT_SCHEMA
		ORDER BY t.TABLE_NAME, c.POSITION
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error querying tables: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]*unifiedmodel.Table)

	for rows.Next() {
		var tableName, columnName, dataType, defaultValue string
		var length, position int
		var scale sql.NullInt64
		var isNullable, isPrimaryKey bool

		err := rows.Scan(&tableName, &columnName, &dataType, &length, &scale,
			&isNullable, &defaultValue, &position, &isPrimaryKey)
		if err != nil {
			return fmt.Errorf("error scanning table row: %w", err)
		}

		if tables[tableName] == nil {
			tables[tableName] = &unifiedmodel.Table{
				Name:        tableName,
				Columns:     make(map[string]unifiedmodel.Column),
				Indexes:     make(map[string]unifiedmodel.Index),
				Constraints: make(map[string]unifiedmodel.Constraint),
			}
		}

		column := unifiedmodel.Column{
			Name:         columnName,
			DataType:     dataType,
			Nullable:     isNullable,
			Default:      strings.TrimSpace(defaultValue),
			IsPrimaryKey: isPrimaryKey,
			Options: map[string]interface{}{
				"length":   length,
				"position": position,
			},
		}

		if scale.Valid {
			column.Options["scale"] = scale.Int64
		}

		tables[tableName].Columns[columnName] = column
	}

	for _, table := range tables {
		um.Tables[table.Name] = *table
	}

	return nil
}

func createTable(ctx context.Context, db *sql.DB, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	var columns []string
	for _, column := range table.Columns {
		columnDef := QuoteIdentifier(column.Name) + " " + column.DataType

		if !column.Nullable {
			columnDef += " NOT NULL"
		}

		if column.Default != "" {
			columnDef += " DEFAULT " + column.Default
		}

		columns = append(columns, columnDef)
	}

	// Add primary key if exists
	var pkColumns []string
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			pkColumns = append(pkColumns, QuoteIdentifier(column.Name))
		}
	}

	if len(pkColumns) > 0 {
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkColumns, ", ")))
	}

	query := fmt.Sprintf("CREATE TABLE %s (%s)",
		QuoteIdentifier(table.Name),
		strings.Join(columns, ", "))

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	return nil
}

// QuoteIdentifier quotes a SAP HANA identifier
func QuoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}
