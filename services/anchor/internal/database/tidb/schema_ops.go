package tidb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator for TiDB.
type SchemaOps struct {
	conn *Connection
}

// DiscoverSchema discovers the unified schema for the TiDB database.
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
	if !s.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	model := &unifiedmodel.UnifiedModel{
		Tables: make(map[string]unifiedmodel.Table),
	}

	// Query tables from information_schema
	query := `
		SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`

	rows, err := s.conn.db.QueryContext(ctx, query, s.conn.config.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, tableType string
		var tableComment sql.NullString

		if err := rows.Scan(&tableName, &tableType, &tableComment); err != nil {
			return nil, err
		}

		// Discover columns for this table
		columns, err := s.discoverColumns(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to discover columns for table %s: %w", tableName, err)
		}

		// Discover indexes
		indexes, err := s.discoverIndexes(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to discover indexes for table %s: %w", tableName, err)
		}

		table := unifiedmodel.Table{
			Name:    tableName,
			Columns: columns,
			Indexes: indexes,
			Options: map[string]interface{}{},
		}

		if tableComment.Valid {
			table.Options["comment"] = tableComment.String
		}

		model.Tables[tableName] = table
	}

	return model, rows.Err()
}

// discoverColumns discovers columns for a specific table.
func (s *SchemaOps) discoverColumns(ctx context.Context, tableName string) (map[string]unifiedmodel.Column, error) {
	query := `
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_KEY,
			EXTRA,
			COLUMN_COMMENT
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := s.conn.db.QueryContext(ctx, query, s.conn.config.DatabaseName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]unifiedmodel.Column)
	for rows.Next() {
		var colName, dataType, isNullable, columnKey, extra string
		var columnDefault, columnComment sql.NullString

		if err := rows.Scan(&colName, &dataType, &isNullable, &columnDefault, &columnKey, &extra, &columnComment); err != nil {
			return nil, err
		}

		column := unifiedmodel.Column{
			Name:     colName,
			DataType: dataType,
			Nullable: isNullable == "YES",
		}

		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		// Set primary key flag
		if columnKey == "PRI" {
			column.IsPrimaryKey = true
		}

		// Set auto-increment flag
		if extra == "auto_increment" {
			column.AutoIncrement = true
		}

		// Store comment in options
		if columnComment.Valid && columnComment.String != "" {
			if column.Options == nil {
				column.Options = make(map[string]interface{})
			}
			column.Options["comment"] = columnComment.String
		}

		columns[colName] = column
	}

	return columns, rows.Err()
}

// discoverIndexes discovers indexes for a specific table.
func (s *SchemaOps) discoverIndexes(ctx context.Context, tableName string) (map[string]unifiedmodel.Index, error) {
	query := `
		SELECT 
			INDEX_NAME,
			NON_UNIQUE,
			COLUMN_NAME,
			SEQ_IN_INDEX
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`

	rows, err := s.conn.db.QueryContext(ctx, query, s.conn.config.DatabaseName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*unifiedmodel.Index)

	for rows.Next() {
		var indexName, columnName string
		var nonUnique, seqInIndex int

		if err := rows.Scan(&indexName, &nonUnique, &columnName, &seqInIndex); err != nil {
			return nil, err
		}

		idx, exists := indexMap[indexName]
		if !exists {
			idx = &unifiedmodel.Index{
				Name:    indexName,
				Columns: []string{},
				Unique:  nonUnique == 0,
			}
			indexMap[indexName] = idx
		}

		idx.Columns = append(idx.Columns, columnName)
	}

	// Convert to final map
	indexes := make(map[string]unifiedmodel.Index)
	for name, idx := range indexMap {
		indexes[name] = *idx
	}

	return indexes, rows.Err()
}

// GetTableSchema retrieves schema information for a specific table.
func (s *SchemaOps) GetTableSchema(ctx context.Context, name string) (*unifiedmodel.Table, error) {
	if !s.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	// Query table information
	query := `
		SELECT TABLE_TYPE, TABLE_COMMENT
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`

	var tableType string
	var tableComment sql.NullString

	err := s.conn.db.QueryRowContext(ctx, query, s.conn.config.DatabaseName, name).Scan(&tableType, &tableComment)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table not found: %s", name)
		}
		return nil, err
	}

	// Discover columns
	columns, err := s.discoverColumns(ctx, name)
	if err != nil {
		return nil, err
	}

	// Discover indexes
	indexes, err := s.discoverIndexes(ctx, name)
	if err != nil {
		return nil, err
	}

	table := &unifiedmodel.Table{
		Name:    name,
		Columns: columns,
		Indexes: indexes,
		Options: map[string]interface{}{},
	}

	if tableComment.Valid {
		table.Options["comment"] = tableComment.String
	}

	return table, nil
}

// ListTables lists all tables in the TiDB database.
func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
	if !s.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	query := `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`

	rows, err := s.conn.db.QueryContext(ctx, query, s.conn.config.DatabaseName)
	if err != nil {
		return nil, err
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

// CreateTable creates a new table in TiDB.
func (s *SchemaOps) CreateTable(ctx context.Context, table *unifiedmodel.Table) error {
	if !s.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	// Build CREATE TABLE statement
	query := fmt.Sprintf("CREATE TABLE `%s` (", table.Name)

	// Add columns
	colIndex := 0
	for colName, col := range table.Columns {
		if colIndex > 0 {
			query += ", "
		}
		query += fmt.Sprintf("`%s` %s", colName, col.DataType)

		if !col.Nullable {
			query += " NOT NULL"
		}

		if col.Default != "" {
			query += fmt.Sprintf(" DEFAULT %s", col.Default)
		}

		if col.AutoIncrement {
			query += " AUTO_INCREMENT"
		}

		// Add comment from options if present
		if col.Options != nil {
			if comment, ok := col.Options["comment"].(string); ok && comment != "" {
				query += fmt.Sprintf(" COMMENT '%s'", comment)
			}
		}

		colIndex++
	}

	// Add primary key
	var pkCols []string
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			pkCols = append(pkCols, fmt.Sprintf("`%s`", col.Name))
		}
	}
	if len(pkCols) > 0 {
		query += fmt.Sprintf(", PRIMARY KEY (%s)", joinStrings(pkCols, ", "))
	}

	query += ")"

	// Execute query
	_, err := s.conn.db.ExecContext(ctx, query)
	return err
}

// CreateStructure creates database objects from a UnifiedModel.
func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
	if !s.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	// Create tables from model
	for _, table := range model.Tables {
		if err := s.CreateTable(ctx, &table); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.Name, err)
		}
	}

	return nil
}

// DropTable drops a table from TiDB.
func (s *SchemaOps) DropTable(ctx context.Context, name string) error {
	if !s.conn.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	query := fmt.Sprintf("DROP TABLE `%s`", name)
	_, err := s.conn.db.ExecContext(ctx, query)
	return err
}

// Helper function to join strings
func joinStrings(strs []string, sep string) string {
	result := ""
	for i, s := range strs {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
