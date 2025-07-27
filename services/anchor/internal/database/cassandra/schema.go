package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of a Cassandra database
func DiscoverSchema(session *gocql.Session) (*CassandraSchema, error) {
	schema := &CassandraSchema{}
	var err error

	// Get keyspaces
	schema.Keyspaces, err = discoverKeyspaces(session)
	if err != nil {
		return nil, fmt.Errorf("error discovering keyspaces: %v", err)
	}

	// Get tables
	schema.Tables, err = discoverTables(session)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get user-defined types
	schema.Types, err = discoverTypes(session)
	if err != nil {
		return nil, fmt.Errorf("error discovering types: %v", err)
	}

	// Get functions
	schema.Functions, err = discoverFunctions(session)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get aggregates
	schema.Aggregates, err = discoverAggregates(session)
	if err != nil {
		return nil, fmt.Errorf("error discovering aggregates: %v", err)
	}

	// Get materialized views
	schema.MaterializedViews, err = discoverMaterializedViews(session)
	if err != nil {
		return nil, fmt.Errorf("error discovering materialized views: %v", err)
	}

	return schema, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(session *gocql.Session, params common.StructureParams) error {
	// Create keyspaces first
	for _, keyspace := range params.Schemas {
		if err := createKeyspace(session, keyspace.Name); err != nil {
			return fmt.Errorf("error creating keyspace %s: %v", keyspace.Name, err)
		}
	}

	// Create user-defined types
	for _, udt := range params.Types {
		if err := createUserDefinedType(session, udt); err != nil {
			return fmt.Errorf("error creating user-defined type %s: %v", udt.Name, err)
		}
	}

	// Create tables
	for _, table := range params.Tables {
		if err := createTable(session, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Create materialized views
	for _, view := range params.MaterializedViews {
		if err := createMaterializedView(session, view); err != nil {
			return fmt.Errorf("error creating materialized view %s: %v", view.Name, err)
		}
	}

	// Create functions
	for _, function := range params.Functions {
		if err := createFunction(session, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	return nil
}

func discoverKeyspaces(session *gocql.Session) ([]KeyspaceInfo, error) {
	var keyspaces []KeyspaceInfo

	iter := session.Query("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces").Iter()

	var keyspaceName string
	var durableWrites bool
	var replication map[string]string

	for iter.Scan(&keyspaceName, &durableWrites, &replication) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		strategy := replication["class"]
		// Remove the full class name prefix
		strategy = strategy[strings.LastIndex(strategy, ".")+1:]

		// Remove strategy from the map
		delete(replication, "class")

		keyspaces = append(keyspaces, KeyspaceInfo{
			Name:                keyspaceName,
			ReplicationStrategy: strategy,
			ReplicationOptions:  replication,
			DurableWrites:       durableWrites,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching keyspaces: %v", err)
	}

	return keyspaces, nil
}

func discoverTables(session *gocql.Session) ([]common.TableInfo, error) {
	var tables []common.TableInfo

	// Get all tables
	iter := session.Query(`
		SELECT keyspace_name, table_name 
		FROM system_schema.tables 
		WHERE keyspace_name NOT LIKE 'system%'
	`).Iter()

	var keyspaceName, tableName string

	for iter.Scan(&keyspaceName, &tableName) {
		table := common.TableInfo{
			Schema:    keyspaceName,
			Name:      tableName,
			TableType: "cassandra.table",
		}

		// Get columns for this table
		columns, err := getTableColumns(session, keyspaceName, tableName)
		if err != nil {
			return nil, err
		}
		table.Columns = columns

		// Get primary key
		primaryKey, err := getTablePrimaryKey(session, keyspaceName, tableName)
		if err != nil {
			return nil, err
		}
		table.PrimaryKey = primaryKey

		tables = append(tables, table)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching tables: %v", err)
	}

	return tables, nil
}

func getTableColumns(session *gocql.Session, keyspace, table string) ([]common.ColumnInfo, error) {
	var columns []common.ColumnInfo

	iter := session.Query(`
		SELECT column_name, type, kind
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?
	`, keyspace, table).Iter()

	var columnName, dataType, kind string

	for iter.Scan(&columnName, &dataType, &kind) {
		column := common.ColumnInfo{
			Name:       columnName,
			DataType:   dataType,
			IsNullable: true, // Cassandra columns are nullable by default
		}

		// Check if this column is part of the primary key
		if kind == "partition_key" || kind == "clustering" {
			column.IsPrimaryKey = true
			column.IsNullable = false // Primary key columns cannot be null
		}

		// Handle collection types
		if strings.HasPrefix(dataType, "list<") ||
			strings.HasPrefix(dataType, "set<") ||
			strings.HasPrefix(dataType, "map<") {
			column.IsArray = true
			elementType := extractElementType(dataType)
			column.ArrayElementType = &elementType
		}

		// Handle user-defined types
		if strings.HasPrefix(dataType, "frozen<") {
			typeName := extractUserDefinedType(dataType)
			column.CustomTypeName = &typeName
		}

		columns = append(columns, column)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching columns for table %s.%s: %v", keyspace, table, err)
	}

	return columns, nil
}

func getTablePrimaryKey(session *gocql.Session, keyspace, table string) ([]string, error) {
	var primaryKey []string

	// Get partition key columns
	iter := session.Query(`
		SELECT column_name
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'
		ORDER BY position
	`, keyspace, table).Iter()

	var columnName string
	for iter.Scan(&columnName) {
		primaryKey = append(primaryKey, columnName)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching partition key for table %s.%s: %v", keyspace, table, err)
	}

	// Get clustering columns
	iter = session.Query(`
		SELECT column_name
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'clustering'
		ORDER BY position
	`, keyspace, table).Iter()

	for iter.Scan(&columnName) {
		primaryKey = append(primaryKey, columnName)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching clustering columns for table %s.%s: %v", keyspace, table, err)
	}

	return primaryKey, nil
}

func discoverTypes(session *gocql.Session) ([]CassandraType, error) {
	var types []CassandraType

	// Get all user-defined types
	iter := session.Query(`
		SELECT keyspace_name, type_name
		FROM system_schema.types
		WHERE keyspace_name NOT LIKE 'system%'
	`).Iter()

	var keyspaceName, typeName string

	for iter.Scan(&keyspaceName, &typeName) {
		udt := CassandraType{
			Keyspace: keyspaceName,
			Name:     typeName,
		}

		// Get fields for this type
		fields, err := getTypeFields(session, keyspaceName, typeName)
		if err != nil {
			return nil, err
		}
		udt.Fields = fields

		types = append(types, udt)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching user-defined types: %v", err)
	}

	return types, nil
}

func getTypeFields(session *gocql.Session, keyspace, typeName string) ([]CassandraTypeField, error) {
	var fields []CassandraTypeField

	iter := session.Query(`
		SELECT field_name, field_type
		FROM system_schema.types
		WHERE keyspace_name = ? AND type_name = ?
	`, keyspace, typeName).Iter()

	var fieldNames []string
	var fieldTypes []string

	// In Cassandra, field names and types are stored as collections
	if iter.Scan(&fieldNames, &fieldTypes) {
		for i := 0; i < len(fieldNames); i++ {
			fields = append(fields, CassandraTypeField{
				Name:     fieldNames[i],
				DataType: fieldTypes[i],
			})
		}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching fields for type %s.%s: %v", keyspace, typeName, err)
	}

	return fields, nil
}

func discoverFunctions(session *gocql.Session) ([]common.FunctionInfo, error) {
	var functions []common.FunctionInfo

	iter := session.Query(`
		SELECT keyspace_name, function_name, argument_types, return_type, language, body
		FROM system_schema.functions
		WHERE keyspace_name NOT LIKE 'system%'
	`).Iter()

	var keyspaceName, functionName, language, body, returnType string
	var argumentTypes []string

	for iter.Scan(&keyspaceName, &functionName, &argumentTypes, &returnType, &language, &body) {
		// Format argument types as a string
		args := strings.Join(argumentTypes, ", ")

		functions = append(functions, common.FunctionInfo{
			Schema:     keyspaceName,
			Name:       functionName,
			Arguments:  args,
			ReturnType: returnType,
			Body:       body,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching functions: %v", err)
	}

	return functions, nil
}

func discoverAggregates(session *gocql.Session) ([]AggregateInfo, error) {
	var aggregates []AggregateInfo

	iter := session.Query(`
		SELECT keyspace_name, aggregate_name, argument_types, final_func, initcond, return_type, state_func, state_type
		FROM system_schema.aggregates
		WHERE keyspace_name NOT LIKE 'system%'
	`).Iter()

	var keyspaceName, aggregateName, finalFunc, initCond, returnType, stateFunc, stateType string
	var argumentTypes []string

	for iter.Scan(&keyspaceName, &aggregateName, &argumentTypes, &finalFunc, &initCond, &returnType, &stateFunc, &stateType) {
		aggregates = append(aggregates, AggregateInfo{
			Keyspace:      keyspaceName,
			Name:          aggregateName,
			ArgumentTypes: argumentTypes,
			FinalFunc:     finalFunc,
			InitCond:      initCond,
			ReturnType:    returnType,
			StateFunc:     stateFunc,
			StateType:     stateType,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching aggregates: %v", err)
	}

	return aggregates, nil
}

func discoverMaterializedViews(session *gocql.Session) ([]common.MaterializedViewInfo, error) {
	var views []common.MaterializedViewInfo

	iter := session.Query(`
		SELECT keyspace_name, view_name, base_table_name, include_all_columns
		FROM system_schema.views
		WHERE keyspace_name NOT LIKE 'system%'
	`).Iter()

	var keyspaceName, viewName, baseTableName string
	var includeAllColumns bool

	for iter.Scan(&keyspaceName, &viewName, &baseTableName, &includeAllColumns) {
		view := common.MaterializedViewInfo{
			Keyspace:   keyspaceName,
			Name:       viewName,
			BaseTable:  baseTableName,
			IncludeAll: includeAllColumns,
		}

		// Get columns for this view
		columns, err := getViewColumns(session, keyspaceName, viewName)
		if err != nil {
			return nil, err
		}
		view.Columns = columns

		// Get primary key
		primaryKey, err := getTablePrimaryKey(session, keyspaceName, viewName)
		if err != nil {
			return nil, err
		}
		view.PrimaryKey = primaryKey

		// Get clustering order
		clusteringOrder, err := getViewClusteringOrder(session, keyspaceName, viewName)
		if err != nil {
			return nil, err
		}
		view.ClusteringOrder = clusteringOrder

		// Get where clause (not directly available in system tables)
		view.WhereClause = ""

		views = append(views, view)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching materialized views: %v", err)
	}

	return views, nil
}

func getViewColumns(session *gocql.Session, keyspace, view string) ([]string, error) {
	var columns []string

	iter := session.Query(`
		SELECT column_name
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?
	`, keyspace, view).Iter()

	var columnName string
	for iter.Scan(&columnName) {
		columns = append(columns, columnName)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching columns for view %s.%s: %v", keyspace, view, err)
	}

	return columns, nil
}

func getViewClusteringOrder(session *gocql.Session, keyspace, view string) (map[string]string, error) {
	clusteringOrder := make(map[string]string)

	iter := session.Query(`
		SELECT column_name, clustering_order
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'clustering'
	`, keyspace, view).Iter()

	var columnName, order string
	for iter.Scan(&columnName, &order) {
		clusteringOrder[columnName] = order
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching clustering order for view %s.%s: %v", keyspace, view, err)
	}

	return clusteringOrder, nil
}

func extractElementType(dataType string) string {
	// Extract the element type from collection types like list<text>, set<int>, map<text, int>
	start := strings.Index(dataType, "<")
	end := strings.LastIndex(dataType, ">")

	if start != -1 && end != -1 && end > start {
		return dataType[start+1 : end]
	}

	return dataType
}

func extractUserDefinedType(dataType string) string {
	// Extract the UDT name from frozen<mytype>
	if strings.HasPrefix(dataType, "frozen<") {
		return extractElementType(dataType)
	}
	return dataType
}

func createKeyspace(session *gocql.Session, keyspaceName string) error {
	// Check if keyspace already exists
	var count int
	if err := session.Query("SELECT COUNT(*) FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspaceName).Scan(&count); err != nil {
		return fmt.Errorf("error checking if keyspace exists: %v", err)
	}

	if count > 0 {
		// Keyspace already exists
		return nil
	}

	// Create keyspace with simple strategy and replication factor 1 (for development)
	query := fmt.Sprintf(`
		CREATE KEYSPACE %s
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
		AND durable_writes = true
	`, common.QuoteIdentifier(keyspaceName))

	if err := session.Query(query).Exec(); err != nil {
		return fmt.Errorf("error creating keyspace: %v", err)
	}

	return nil
}

func createUserDefinedType(session *gocql.Session, typeInfo common.TypeInfo) error {
	// Extract keyspace from module if available
	keyspace := "default"
	if typeInfo.Module != "" {
		keyspace = typeInfo.Module
	}

	// Check if type already exists
	var count int
	if err := session.Query("SELECT COUNT(*) FROM system_schema.types WHERE keyspace_name = ? AND type_name = ?",
		keyspace, typeInfo.Name).Scan(&count); err != nil {
		return fmt.Errorf("error checking if type exists: %v", err)
	}

	if count > 0 {
		// Type already exists
		return nil
	}

	// Build field definitions
	var fields []string
	for _, prop := range typeInfo.Properties {
		// Convert property type to Cassandra type
		cassandraType := convertToCassandraType(prop.Type)
		fields = append(fields, fmt.Sprintf("%s %s", prop.Name, cassandraType))
	}

	// Create the type
	query := fmt.Sprintf(`
		CREATE TYPE %s.%s (
			%s
		)
	`, common.QuoteIdentifier(keyspace), common.QuoteIdentifier(typeInfo.Name), strings.Join(fields, ",\n\t\t\t"))

	if err := session.Query(query).Exec(); err != nil {
		return fmt.Errorf("error creating user-defined type: %v", err)
	}

	return nil
}

func createTable(session *gocql.Session, tableInfo common.TableInfo) error {
	// Extract keyspace from schema
	keyspace := tableInfo.Schema
	if keyspace == "" {
		keyspace = GetKeyspace(session)
	}

	// Check if table already exists
	var count int
	if err := session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		keyspace, tableInfo.Name).Scan(&count); err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}

	if count > 0 {
		// Table already exists
		return nil
	}

	// Build column definitions
	var columnDefs []string
	for _, column := range tableInfo.Columns {
		def := fmt.Sprintf("%s %s", column.Name, convertColumnType(column))
		columnDefs = append(columnDefs, def)
	}

	// Build primary key clause
	var primaryKeyClause string
	if len(tableInfo.PrimaryKey) > 0 {
		// In Cassandra, we need to distinguish between partition key and clustering columns
		// For simplicity, we'll assume the first column is the partition key
		// and the rest are clustering columns
		if len(tableInfo.PrimaryKey) == 1 {
			primaryKeyClause = fmt.Sprintf("PRIMARY KEY (%s)", tableInfo.PrimaryKey[0])
		} else {
			// First column is partition key, rest are clustering columns
			primaryKeyClause = fmt.Sprintf("PRIMARY KEY ((%s), %s)",
				tableInfo.PrimaryKey[0],
				strings.Join(tableInfo.PrimaryKey[1:], ", "))
		}
	}

	// Create the table
	query := fmt.Sprintf(`
		CREATE TABLE %s.%s (
			%s,
			%s
		)
	`, common.QuoteIdentifier(keyspace), common.QuoteIdentifier(tableInfo.Name),
		strings.Join(columnDefs, ",\n\t\t\t"), primaryKeyClause)

	if err := session.Query(query).Exec(); err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	return nil
}

func createMaterializedView(session *gocql.Session, viewInfo common.MaterializedViewInfo) error {
	// Check if view already exists
	var count int
	if err := session.Query("SELECT COUNT(*) FROM system_schema.views WHERE keyspace_name = ? AND view_name = ?",
		viewInfo.Keyspace, viewInfo.Name).Scan(&count); err != nil {
		return fmt.Errorf("error checking if view exists: %v", err)
	}

	if count > 0 {
		// View already exists
		return nil
	}

	// Build column selection
	columnSelection := "*"
	if !viewInfo.IncludeAll && len(viewInfo.Columns) > 0 {
		columnSelection = strings.Join(viewInfo.Columns, ", ")
	}

	// Build primary key clause
	var primaryKeyClause string
	if len(viewInfo.PrimaryKey) > 0 {
		// In Cassandra, we need to distinguish between partition key and clustering columns
		// For simplicity, we'll assume the first column is the partition key
		// and the rest are clustering columns
		if len(viewInfo.PrimaryKey) == 1 {
			primaryKeyClause = fmt.Sprintf("PRIMARY KEY (%s)", viewInfo.PrimaryKey[0])
		} else {
			// First column is partition key, rest are clustering columns
			primaryKeyClause = fmt.Sprintf("PRIMARY KEY ((%s), %s)",
				viewInfo.PrimaryKey[0],
				strings.Join(viewInfo.PrimaryKey[1:], ", "))
		}
	}

	// Build where clause
	whereClause := "1=1" // Default where clause that's always true
	if viewInfo.WhereClause != "" {
		whereClause = viewInfo.WhereClause
	}

	// Create the materialized view
	query := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW %s.%s AS
		SELECT %s
		FROM %s.%s
		WHERE %s
		PRIMARY KEY %s
	`, common.QuoteIdentifier(viewInfo.Keyspace), common.QuoteIdentifier(viewInfo.Name),
		columnSelection,
		common.QuoteIdentifier(viewInfo.Keyspace), common.QuoteIdentifier(viewInfo.BaseTable),
		whereClause,
		primaryKeyClause)

	// Add clustering order if specified
	if len(viewInfo.ClusteringOrder) > 0 {
		var orderClauses []string
		for column, order := range viewInfo.ClusteringOrder {
			orderClauses = append(orderClauses, fmt.Sprintf("%s %s", column, order))
		}
		query += fmt.Sprintf(" WITH CLUSTERING ORDER BY (%s)", strings.Join(orderClauses, ", "))
	}

	if err := session.Query(query).Exec(); err != nil {
		return fmt.Errorf("error creating materialized view: %v", err)
	}

	return nil
}

func createFunction(session *gocql.Session, functionInfo common.FunctionInfo) error {
	// Check if function already exists
	var count int
	if err := session.Query("SELECT COUNT(*) FROM system_schema.functions WHERE keyspace_name = ? AND function_name = ?",
		functionInfo.Schema, functionInfo.Name).Scan(&count); err != nil {
		return fmt.Errorf("error checking if function exists: %v", err)
	}

	if count > 0 {
		// Function already exists
		return nil
	}

	// Create the function
	query := fmt.Sprintf(`
		CREATE FUNCTION %s.%s(%s)
		RETURNS %s
		LANGUAGE javascript
		AS '%s'
	`, common.QuoteIdentifier(functionInfo.Schema), common.QuoteIdentifier(functionInfo.Name),
		functionInfo.Arguments,
		functionInfo.ReturnType,
		escapeJavaScriptBody(functionInfo.Body))

	if err := session.Query(query).Exec(); err != nil {
		return fmt.Errorf("error creating function: %v", err)
	}

	return nil
}

func convertToCassandraType(dataType string) string {
	// Convert common data types to Cassandra types
	switch strings.ToLower(dataType) {
	case "string":
		return "text"
	case "integer", "int":
		return "int"
	case "bigint", "long":
		return "bigint"
	case "float":
		return "float"
	case "double":
		return "double"
	case "boolean", "bool":
		return "boolean"
	case "date":
		return "date"
	case "time":
		return "time"
	case "timestamp":
		return "timestamp"
	case "uuid":
		return "uuid"
	case "timeuuid":
		return "timeuuid"
	case "blob":
		return "blob"
	case "inet":
		return "inet"
	default:
		// If it's a custom type, assume it's already in Cassandra format
		return dataType
	}
}

func convertColumnType(column common.ColumnInfo) string {
	// Handle array/collection types
	if column.IsArray {
		if column.ArrayElementType != nil {
			elementType := convertToCassandraType(*column.ArrayElementType)
			return fmt.Sprintf("list<%s>", elementType)
		}
		// Default to list of text if element type is not specified
		return "list<text>"
	}

	// Handle custom types
	if column.CustomTypeName != nil && *column.CustomTypeName != "" {
		return fmt.Sprintf("frozen<%s>", *column.CustomTypeName)
	}

	// Handle regular types
	return convertToCassandraType(column.DataType)
}

func escapeJavaScriptBody(body string) string {
	// Escape single quotes in JavaScript body
	return strings.ReplaceAll(body, "'", "\\'")
}
