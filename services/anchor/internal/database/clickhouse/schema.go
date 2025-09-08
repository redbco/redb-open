package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of a Clickhouse database and returns a UnifiedModel
func DiscoverSchema(conn ClickhouseConn) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.ClickHouse,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Views:        make(map[string]unifiedmodel.View),
	}

	var err error

	// Get tables directly as unifiedmodel types
	err = discoverTablesUnified(conn, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get schemas directly as unifiedmodel types
	err = discoverSchemasUnified(conn, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering schemas: %v", err)
	}

	// Get functions directly as unifiedmodel types
	err = discoverFunctionsUnified(conn, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get views directly as unifiedmodel types
	err = discoverViewsUnified(conn, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering views: %v", err)
	}

	// Get dictionaries directly as unifiedmodel types
	err = discoverDictionariesUnified(conn, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering dictionaries: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(conn ClickhouseConn, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Create schemas from UnifiedModel
	for _, schema := range um.Schemas {
		if err := createSchemaFromUnified(conn, schema); err != nil {
			return fmt.Errorf("error creating schema %s: %v", schema.Name, err)
		}
	}

	// Create tables from UnifiedModel
	for _, table := range um.Tables {
		if err := createTableFromUnified(conn, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Create views from UnifiedModel
	for _, view := range um.Views {
		if err := createViewFromUnified(conn, view); err != nil {
			return fmt.Errorf("error creating view %s: %v", view.Name, err)
		}
	}

	// Create functions from UnifiedModel
	for _, function := range um.Functions {
		if err := createFunctionFromUnified(conn, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	return nil
}

// discoverTablesUnified discovers tables directly into UnifiedModel
func discoverTablesUnified(conn ClickhouseConn, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			database AS table_schema,
			name AS table_name,
			engine AS table_type,
			comment AS table_comment
		FROM system.tables 
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, tableName, tableType, comment string
		if err := rows.Scan(&schema, &tableName, &tableType, &comment); err != nil {
			return fmt.Errorf("error scanning table row: %v", err)
		}

		table := unifiedmodel.Table{
			Name:        tableName,
			Comment:     comment,
			Columns:     make(map[string]unifiedmodel.Column),
			Indexes:     make(map[string]unifiedmodel.Index),
			Constraints: make(map[string]unifiedmodel.Constraint),
		}

		// Get columns for this table
		err := getTableColumnsUnified(conn, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting columns for table %s.%s: %v", schema, tableName, err)
		}

		um.Tables[fmt.Sprintf("%s.%s", schema, tableName)] = table
	}

	return rows.Err()
}

// getTableColumnsUnified gets columns for a table directly into UnifiedModel
func getTableColumnsUnified(conn ClickhouseConn, database, tableName string, tableModel *unifiedmodel.Table) error {
	query := `
		SELECT 
			name AS column_name,
			type AS data_type,
			default_expression AS column_default,
			is_in_primary_key AS is_primary_key,
			comment AS column_comment
		FROM system.columns 
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	rows, err := conn.Query(context.Background(), query, database, tableName)
	if err != nil {
		return fmt.Errorf("error querying columns: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType, columnDefault, comment string
		var isPrimaryKey bool
		if err := rows.Scan(&columnName, &dataType, &columnDefault, &isPrimaryKey, &comment); err != nil {
			return fmt.Errorf("error scanning column row: %v", err)
		}

		column := unifiedmodel.Column{
			Name:         columnName,
			DataType:     dataType,
			Default:      columnDefault,
			IsPrimaryKey: isPrimaryKey,
			Nullable:     !strings.Contains(strings.ToLower(dataType), "not null"),
			Options:      map[string]any{"comment": comment},
		}

		tableModel.Columns[columnName] = column
	}

	return rows.Err()
}

// discoverSchemasUnified discovers schemas directly into UnifiedModel
func discoverSchemasUnified(conn ClickhouseConn, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			name AS database_name,
			comment AS database_comment
		FROM system.databases 
		WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY name
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			return fmt.Errorf("error scanning schema row: %v", err)
		}

		schema := unifiedmodel.Schema{
			Name:    name,
			Comment: comment,
		}

		um.Schemas[name] = schema
	}

	return rows.Err()
}

// discoverFunctionsUnified discovers functions directly into UnifiedModel
func discoverFunctionsUnified(conn ClickhouseConn, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			name AS function_name,
			'sql' AS language,
			'' AS return_type,
			'' AS function_body
		FROM system.functions 
		WHERE origin = 'System'
		ORDER BY name
		LIMIT 100
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, language, returnType, body string
		if err := rows.Scan(&name, &language, &returnType, &body); err != nil {
			return fmt.Errorf("error scanning function row: %v", err)
		}

		function := unifiedmodel.Function{
			Name:       name,
			Language:   language,
			Returns:    returnType,
			Definition: body,
		}

		um.Functions[name] = function
	}

	return rows.Err()
}

// discoverViewsUnified discovers views directly into UnifiedModel
func discoverViewsUnified(conn ClickhouseConn, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			database AS view_schema,
			name AS view_name,
			as_select AS view_definition
		FROM system.tables 
		WHERE engine = 'View' 
		AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, name, definition string
		if err := rows.Scan(&schema, &name, &definition); err != nil {
			return fmt.Errorf("error scanning view row: %v", err)
		}

		view := unifiedmodel.View{
			Name:       name,
			Definition: definition,
		}

		um.Views[fmt.Sprintf("%s.%s", schema, name)] = view
	}

	return rows.Err()
}

// discoverDictionariesUnified discovers dictionaries directly into UnifiedModel
func discoverDictionariesUnified(conn ClickhouseConn, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			database AS dict_database,
			name AS dict_name,
			type AS dict_type,
			source AS dict_source
		FROM system.dictionaries 
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying dictionaries: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var database, name, dictType, source string
		if err := rows.Scan(&database, &name, &dictType, &source); err != nil {
			return fmt.Errorf("error scanning dictionary row: %v", err)
		}

		// Represent dictionaries as views since they're similar to materialized views
		view := unifiedmodel.View{
			Name:       name,
			Definition: fmt.Sprintf("DICTIONARY %s TYPE %s SOURCE %s", name, dictType, source),
		}

		um.Views[fmt.Sprintf("%s.%s", database, name)] = view
	}

	return rows.Err()
}

// createSchemaFromUnified creates a schema from UnifiedModel Schema
func createSchemaFromUnified(conn ClickhouseConn, schema unifiedmodel.Schema) error {
	if schema.Name == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", QuoteIdentifier(schema.Name))
	if schema.Comment != "" {
		query += fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(schema.Comment, "'", "''"))
	}

	return conn.Exec(context.Background(), query)
}

// createTableFromUnified creates a table from UnifiedModel Table
func createTableFromUnified(conn ClickhouseConn, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Extract database name from table name if it contains a dot
	parts := strings.Split(table.Name, ".")
	var database, tableName string
	if len(parts) == 2 {
		database = parts[0]
		tableName = parts[1]
	} else {
		database = "default"
		tableName = table.Name
	}

	var columnDefs []string
	var primaryKeys []string

	for _, column := range table.Columns {
		columnDef := fmt.Sprintf("%s %s", QuoteIdentifier(column.Name), column.DataType)

		if column.Default != "" {
			columnDef += fmt.Sprintf(" DEFAULT %s", column.Default)
		}

		if column.Options != nil {
			if comment, ok := column.Options["comment"].(string); ok && comment != "" {
				columnDef += fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(comment, "'", "''"))
			}
		}

		columnDefs = append(columnDefs, columnDef)

		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, QuoteIdentifier(column.Name))
		}
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n\t%s\n)",
		QuoteIdentifier(database), QuoteIdentifier(tableName), strings.Join(columnDefs, ",\n\t"))

	if len(primaryKeys) > 0 {
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n\t%s,\n\tPRIMARY KEY (%s)\n)",
			QuoteIdentifier(database), QuoteIdentifier(tableName),
			strings.Join(columnDefs, ",\n\t"), strings.Join(primaryKeys, ", "))
	}

	query += " ENGINE = MergeTree()"

	if table.Comment != "" {
		query += fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(table.Comment, "'", "''"))
	}

	return conn.Exec(context.Background(), query)
}

// createViewFromUnified creates a view from UnifiedModel View
func createViewFromUnified(conn ClickhouseConn, view unifiedmodel.View) error {
	if view.Name == "" {
		return fmt.Errorf("view name cannot be empty")
	}

	// Extract database name from view name if it contains a dot
	parts := strings.Split(view.Name, ".")
	var database, viewName string
	if len(parts) == 2 {
		database = parts[0]
		viewName = parts[1]
	} else {
		database = "default"
		viewName = view.Name
	}

	query := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS %s",
		QuoteIdentifier(database), QuoteIdentifier(viewName), view.Definition)

	return conn.Exec(context.Background(), query)
}

// createFunctionFromUnified creates a function from UnifiedModel Function
func createFunctionFromUnified(conn ClickhouseConn, function unifiedmodel.Function) error {
	if function.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	// ClickHouse doesn't support user-defined functions in the same way as other databases
	// This is a placeholder implementation
	return fmt.Errorf("user-defined functions are not supported in ClickHouse")
}

// QuoteIdentifier quotes a ClickHouse identifier
func QuoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
}
