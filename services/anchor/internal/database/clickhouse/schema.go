package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

	// Get tables and their columns, convert to unified model format
	tablesMap, _, err := discoverTablesAndColumns(conn)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}
	for _, table := range tablesMap {
		unifiedTable := ConvertClickHouseTable(*table)
		um.Tables[table.Name] = unifiedTable
	}

	// Get schemas (databases in ClickHouse) and convert to unified model format
	schemas, err := getSchemas(conn)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}
	for _, schema := range schemas {
		um.Schemas[schema.Name] = unifiedmodel.Schema{
			Name:    schema.Name,
			Comment: schema.Description,
		}
	}

	// Get functions and convert to unified model format
	functions, err := getFunctions(conn)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}
	for _, function := range functions {
		um.Functions[function.Name] = unifiedmodel.Function{
			Name:       function.Name,
			Language:   "sql", // ClickHouse uses SQL-like syntax
			Returns:    function.ReturnType,
			Definition: function.Body,
		}
	}

	// Get views and convert to unified model format
	views, err := getViews(conn)
	if err != nil {
		return nil, fmt.Errorf("error getting views: %v", err)
	}
	for _, view := range views {
		um.Views[view.Name] = unifiedmodel.View{
			Name:       view.Name,
			Definition: view.Definition,
		}
	}

	// Get dictionaries and convert to unified views (as they're similar to materialized views)
	dictionaries, err := getDictionaries(conn)
	if err != nil {
		return nil, fmt.Errorf("error getting dictionaries: %v", err)
	}
	for _, dict := range dictionaries {
		unifiedDict := ConvertClickHouseDictionary(dict)
		um.Views[dict.Name] = unifiedDict
	}

	return um, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(conn ClickhouseConn, params common.StructureParams) error {
	// Sort tables based on dependencies
	sortedTables, err := common.TopologicalSort(params.Tables)
	if err != nil {
		return fmt.Errorf("error sorting tables: %v", err)
	}

	// Create tables
	for _, table := range sortedTables {
		if err := CreateTable(conn, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	return nil
}

func discoverTablesAndColumns(conn ClickhouseConn) (map[string]*common.TableInfo, []string, error) {
	query := `
		SELECT 
			database AS table_schema,
			table AS table_name,
			name AS column_name,
			type AS data_type,
			is_nullable,
			default_expression AS column_default,
			is_in_primary_key,
			0 AS is_array,
			0 AS is_unique,
			0 AS is_auto_increment,
			engine AS table_type,
			'' AS parent_table,
			'' AS partition_value
		FROM system.columns
		WHERE database = currentDatabase()
		ORDER BY table, position
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching table and column information: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*common.TableInfo)
	tableNames := make([]string, 0)

	for rows.Next() {
		var schemaName, tableName, columnName, dataType string
		var isNullable, isPrimaryKey, isArray, isUnique, isAutoIncrement sql.NullBool
		var columnDefault, parentTable, partitionValue sql.NullString
		var tableType string

		if err := rows.Scan(
			&schemaName, &tableName, &columnName, &dataType, &isNullable, &columnDefault,
			&isPrimaryKey, &isArray, &isUnique, &isAutoIncrement, &tableType, &parentTable, &partitionValue,
		); err != nil {
			return nil, nil, fmt.Errorf("error scanning table and column row: %v", err)
		}

		if _, exists := tables[tableName]; !exists {
			tables[tableName] = &common.TableInfo{
				Name:           tableName,
				Schema:         schemaName,
				TableType:      "clickhouse." + tableType,
				ParentTable:    parentTable.String,
				PartitionValue: partitionValue.String,
			}
			tableNames = append(tableNames, tableName)
		}

		var defaultValue *string
		if columnDefault.Valid {
			defaultValue = &columnDefault.String
		}

		columnInfo := common.ColumnInfo{
			Name:            columnName,
			DataType:        dataType,
			IsNullable:      isNullable.Bool,
			ColumnDefault:   defaultValue,
			IsPrimaryKey:    isPrimaryKey.Bool,
			IsArray:         isArray.Bool,
			IsUnique:        isUnique.Bool,
			IsAutoIncrement: isAutoIncrement.Bool,
		}

		// Handle array types in Clickhouse
		if strings.Contains(dataType, "Array") {
			columnInfo.IsArray = true
			elementType := strings.TrimPrefix(strings.TrimSuffix(dataType, ")"), "Array(")
			columnInfo.ArrayElementType = &elementType
		}

		tables[tableName].Columns = append(tables[tableName].Columns, columnInfo)
	}

	// Get primary keys for each table
	for tableName, table := range tables {
		primaryKeys, err := getPrimaryKeys(conn, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting primary keys for table %s: %v", tableName, err)
		}
		table.PrimaryKey = primaryKeys
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func getPrimaryKeys(conn ClickhouseConn, tableName string) ([]string, error) {
	query := `
		SELECT name
		FROM system.columns
		WHERE database = currentDatabase()
		AND table = ?
		AND is_in_primary_key = 1
		ORDER BY position
	`

	rows, err := conn.Query(context.Background(), query, tableName)
	if err != nil {
		return nil, fmt.Errorf("error fetching primary keys: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("error scanning primary key: %v", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	return primaryKeys, nil
}

func getSchemas(conn ClickhouseConn) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT 
			name as schema_name,
			'' as description
		FROM system.databases
		ORDER BY name
	`

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	var schemas []common.DatabaseSchemaInfo
	for rows.Next() {
		var schema common.DatabaseSchemaInfo
		var description sql.NullString
		if err := rows.Scan(&schema.Name, &description); err != nil {
			return nil, fmt.Errorf("error scanning schema: %v", err)
		}
		if description.Valid {
			schema.Description = description.String
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func getFunctions(conn ClickhouseConn) ([]common.FunctionInfo, error) {
	query := `
		SELECT 
			name AS function_name,
			'' AS schema_name,
			'' AS argument_data_types,
			'' AS return_type,
			'' AS function_body
		FROM system.functions
		ORDER BY name
	`
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []common.FunctionInfo

	for rows.Next() {
		var function common.FunctionInfo
		if err := rows.Scan(&function.Name, &function.Schema, &function.Arguments, &function.ReturnType, &function.Body); err != nil {
			return nil, err
		}
		functions = append(functions, function)
	}
	return functions, rows.Err()
}

func getViews(conn ClickhouseConn) ([]common.ViewInfo, error) {
	query := `
		SELECT 
			name AS view_name,
			database AS schema_name,
			create_table_query AS definition
		FROM system.tables
		WHERE engine = 'View'
		AND database = currentDatabase()
		ORDER BY name
	`
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []common.ViewInfo

	for rows.Next() {
		var view common.ViewInfo
		if err := rows.Scan(&view.Name, &view.Schema, &view.Definition); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, rows.Err()
}

func getDictionaries(conn ClickhouseConn) ([]ClickhouseDictionaryInfo, error) {
	query := `
		SELECT 
			name,
			database AS schema,
			source,
			layout,
			'' AS definition,
			'' AS description
		FROM system.dictionaries
		WHERE database = currentDatabase()
		ORDER BY name
	`
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dictionaries []ClickhouseDictionaryInfo

	for rows.Next() {
		var dict ClickhouseDictionaryInfo
		if err := rows.Scan(&dict.Name, &dict.Schema, &dict.Source, &dict.Layout, &dict.Definition, &dict.Description); err != nil {
			return nil, err
		}
		dictionaries = append(dictionaries, dict)
	}
	return dictionaries, rows.Err()
}

func CreateTable(conn ClickhouseConn, tableInfo common.TableInfo) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	if tableInfo.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Check if the table already exists
	var exists int
	err := conn.QueryRow(context.Background(),
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?",
		tableInfo.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists > 0 {
		return fmt.Errorf("table '%s' already exists", tableInfo.Name)
	}

	// Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", tableInfo.Name)
	for i, column := range tableInfo.Columns {
		if i > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s ", column.Name)

		// Handle Clickhouse data types
		if column.IsArray {
			if column.ArrayElementType != nil {
				createTableSQL += fmt.Sprintf("Array(%s)", *column.ArrayElementType)
			} else {
				createTableSQL += "Array(String)"
			}
		} else {
			// Map common data types to Clickhouse types
			switch strings.ToUpper(column.DataType) {
			case "INTEGER", "INT":
				createTableSQL += "Int32"
			case "BIGINT":
				createTableSQL += "Int64"
			case "FLOAT":
				createTableSQL += "Float32"
			case "DOUBLE":
				createTableSQL += "Float64"
			case "VARCHAR", "CHARACTER VARYING":
				createTableSQL += "String"
			case "DATE":
				createTableSQL += "Date"
			case "TIMESTAMP":
				createTableSQL += "DateTime"
			case "BOOLEAN":
				createTableSQL += "UInt8"
			default:
				createTableSQL += column.DataType
			}
		}

		if !column.IsNullable {
			createTableSQL += " NOT NULL"
		}
		if column.ColumnDefault != nil {
			createTableSQL += fmt.Sprintf(" DEFAULT %s", *column.ColumnDefault)
		}
	}

	// Add primary key
	if len(tableInfo.PrimaryKey) > 0 {
		createTableSQL += fmt.Sprintf(") ENGINE = MergeTree() PRIMARY KEY (%s)",
			strings.Join(tableInfo.PrimaryKey, ", "))
	} else {
		// Default to MergeTree engine with order by tuple()
		createTableSQL += ") ENGINE = MergeTree() ORDER BY tuple()"
	}

	// Print the SQL statement
	fmt.Printf("Creating table %s with SQL: %s\n", tableInfo.Name, createTableSQL)

	err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	return nil
}
