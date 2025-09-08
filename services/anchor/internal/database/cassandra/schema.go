package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of a Cassandra database and returns a UnifiedModel
func DiscoverSchema(session *gocql.Session) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:      dbcapabilities.Cassandra,
		Keyspaces:         make(map[string]unifiedmodel.Keyspace),
		Tables:            make(map[string]unifiedmodel.Table),
		Types:             make(map[string]unifiedmodel.Type),
		Functions:         make(map[string]unifiedmodel.Function),
		MaterializedViews: make(map[string]unifiedmodel.MaterializedView),
	}

	var err error

	// Get keyspaces directly as unifiedmodel types
	err = discoverKeyspacesUnified(session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering keyspaces: %v", err)
	}

	// Get tables directly as unifiedmodel types
	err = discoverTablesUnified(session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get user-defined types directly as unifiedmodel types
	err = discoverTypesUnified(session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering types: %v", err)
	}

	// Get functions directly as unifiedmodel types
	err = discoverFunctionsUnified(session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get aggregates directly as unifiedmodel types
	err = discoverAggregatesUnified(session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering aggregates: %v", err)
	}

	// Get materialized views directly as unifiedmodel types
	err = discoverMaterializedViewsUnified(session, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering materialized views: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}
	// Create keyspaces from UnifiedModel
	for _, keyspace := range um.Keyspaces {
		if err := createKeyspaceFromUnified(session, keyspace); err != nil {
			return fmt.Errorf("error creating keyspace %s: %v", keyspace.Name, err)
		}
	}

	// Create user-defined types from UnifiedModel
	for _, typeInfo := range um.Types {
		if err := createUserDefinedTypeFromUnified(session, typeInfo); err != nil {
			return fmt.Errorf("error creating user-defined type %s: %v", typeInfo.Name, err)
		}
	}

	// Create tables from UnifiedModel
	for _, table := range um.Tables {
		if err := createTableFromUnified(session, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Create materialized views from UnifiedModel
	for _, view := range um.MaterializedViews {
		if err := createMaterializedViewFromUnified(session, view); err != nil {
			return fmt.Errorf("error creating materialized view %s: %v", view.Name, err)
		}
	}

	// Create functions from UnifiedModel
	for _, function := range um.Functions {
		if err := createFunctionFromUnified(session, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	return nil
}

// discoverKeyspacesUnified discovers keyspaces directly into UnifiedModel
func discoverKeyspacesUnified(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	iter := session.Query("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces").Iter()

	var keyspaceName string
	var durableWrites bool
	var replication map[string]string

	for iter.Scan(&keyspaceName, &durableWrites, &replication) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		keyspace := unifiedmodel.Keyspace{
			Name:          keyspaceName,
			DurableWrites: durableWrites,
		}

		// Parse replication strategy
		if strategy, ok := replication["class"]; ok {
			keyspace.ReplicationStrategy = strategy
			keyspace.ReplicationOptions = make(map[string]string)
			for k, v := range replication {
				if k != "class" {
					keyspace.ReplicationOptions[k] = v
				}
			}
		}

		um.Keyspaces[keyspaceName] = keyspace
	}

	return iter.Close()
}

// discoverTablesUnified discovers tables directly into UnifiedModel
func discoverTablesUnified(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	iter := session.Query("SELECT keyspace_name, table_name FROM system_schema.tables").Iter()

	var keyspaceName, tableName string
	for iter.Scan(&keyspaceName, &tableName) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		table := unifiedmodel.Table{
			Name:        tableName,
			Comment:     keyspaceName, // Store keyspace in comment for reference
			Columns:     make(map[string]unifiedmodel.Column),
			Indexes:     make(map[string]unifiedmodel.Index),
			Constraints: make(map[string]unifiedmodel.Constraint),
		}

		// Get columns for this table
		err := getTableColumnsUnified(session, keyspaceName, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting columns for table %s.%s: %v", keyspaceName, tableName, err)
		}

		um.Tables[fmt.Sprintf("%s.%s", keyspaceName, tableName)] = table
	}

	return iter.Close()
}

// getTableColumnsUnified gets columns for a table directly into UnifiedModel
func getTableColumnsUnified(session *gocql.Session, keyspace, table string, tableModel *unifiedmodel.Table) error {
	iter := session.Query(`
		SELECT column_name, type, kind, clustering_order, position
		FROM system_schema.columns 
		WHERE keyspace_name = ? AND table_name = ?`,
		keyspace, table).Iter()

	var columnName, columnType, kind, clusteringOrder string
	var position int

	for iter.Scan(&columnName, &columnType, &kind, &clusteringOrder, &position) {
		column := unifiedmodel.Column{
			Name:     columnName,
			DataType: columnType,
		}

		// Set column properties based on kind
		switch kind {
		case "partition_key":
			column.IsPrimaryKey = true
		case "clustering":
			column.IsPrimaryKey = true
		case "regular":
			// Regular column
		case "static":
			// Static column in Cassandra
		}

		tableModel.Columns[columnName] = column
	}

	return iter.Close()
}

// discoverTypesUnified discovers user-defined types directly into UnifiedModel
func discoverTypesUnified(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	iter := session.Query("SELECT keyspace_name, type_name, field_names, field_types FROM system_schema.types").Iter()

	var keyspaceName, typeName string
	var fieldNames []string
	var fieldTypes []string

	for iter.Scan(&keyspaceName, &typeName, &fieldNames, &fieldTypes) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		typeInfo := unifiedmodel.Type{
			Name:     typeName,
			Category: "composite",
			Definition: map[string]any{
				"keyspace": keyspaceName,
				"fields":   make(map[string]string),
			},
		}

		// Combine field names and types
		if typeInfo.Definition != nil {
			if fields, ok := typeInfo.Definition["fields"].(map[string]string); ok {
				for i, fieldName := range fieldNames {
					if i < len(fieldTypes) {
						fields[fieldName] = fieldTypes[i]
					}
				}
			}
		}

		um.Types[fmt.Sprintf("%s.%s", keyspaceName, typeName)] = typeInfo
	}

	return iter.Close()
}

// discoverFunctionsUnified discovers functions directly into UnifiedModel
func discoverFunctionsUnified(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	iter := session.Query("SELECT keyspace_name, function_name, argument_types, return_type, body, language FROM system_schema.functions").Iter()

	var keyspaceName, functionName, returnType, body, language string
	var argumentTypes []string

	for iter.Scan(&keyspaceName, &functionName, &argumentTypes, &returnType, &body, &language) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		function := unifiedmodel.Function{
			Name:       functionName,
			Language:   language,
			Returns:    returnType,
			Definition: body,
		}

		um.Functions[fmt.Sprintf("%s.%s", keyspaceName, functionName)] = function
	}

	return iter.Close()
}

// discoverAggregatesUnified discovers aggregates directly into UnifiedModel
func discoverAggregatesUnified(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	iter := session.Query("SELECT keyspace_name, aggregate_name, argument_types, return_type, state_func, final_func FROM system_schema.aggregates").Iter()

	var keyspaceName, aggregateName, returnType, stateFunc, finalFunc string
	var argumentTypes []string

	for iter.Scan(&keyspaceName, &aggregateName, &argumentTypes, &returnType, &stateFunc, &finalFunc) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		aggregate := unifiedmodel.Function{
			Name:       aggregateName,
			Language:   "cql",
			Returns:    returnType,
			Definition: fmt.Sprintf("STATE FUNC: %s, FINAL FUNC: %s", stateFunc, finalFunc),
		}

		um.Functions[fmt.Sprintf("%s.%s", keyspaceName, aggregateName)] = aggregate
	}

	return iter.Close()
}

// discoverMaterializedViewsUnified discovers materialized views directly into UnifiedModel
func discoverMaterializedViewsUnified(session *gocql.Session, um *unifiedmodel.UnifiedModel) error {
	iter := session.Query("SELECT keyspace_name, view_name, base_table_name FROM system_schema.views").Iter()

	var keyspaceName, viewName, baseTableName string

	for iter.Scan(&keyspaceName, &viewName, &baseTableName) {
		// Skip system keyspaces
		if strings.HasPrefix(keyspaceName, "system") {
			continue
		}

		view := unifiedmodel.MaterializedView{
			Name:       viewName,
			Definition: fmt.Sprintf("SELECT * FROM %s.%s", keyspaceName, baseTableName),
		}

		um.MaterializedViews[fmt.Sprintf("%s.%s", keyspaceName, viewName)] = view
	}

	return iter.Close()
}

// createKeyspaceFromUnified creates a keyspace from UnifiedModel Keyspace
func createKeyspaceFromUnified(session *gocql.Session, keyspace unifiedmodel.Keyspace) error {
	replicationMap := make(map[string]interface{})
	replicationMap["class"] = keyspace.ReplicationStrategy

	for k, v := range keyspace.ReplicationOptions {
		replicationMap[k] = v
	}

	query := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = %v
		AND durable_writes = %t
	`, QuoteIdentifier(keyspace.Name), replicationMap, keyspace.DurableWrites)

	return session.Query(query).Exec()
}

// createUserDefinedTypeFromUnified creates a user-defined type from UnifiedModel Type
func createUserDefinedTypeFromUnified(session *gocql.Session, typeInfo unifiedmodel.Type) error {
	if typeInfo.Category != "composite" {
		return nil // Skip non-composite types
	}

	var keyspace string
	var fields map[string]string

	if typeInfo.Definition != nil {
		if ks, ok := typeInfo.Definition["keyspace"].(string); ok {
			keyspace = ks
		}
		if f, ok := typeInfo.Definition["fields"].(map[string]string); ok {
			fields = f
		}
	}

	if keyspace == "" || len(fields) == 0 {
		return fmt.Errorf("invalid type definition for %s", typeInfo.Name)
	}

	var fieldDefs []string
	for fieldName, fieldType := range fields {
		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", QuoteIdentifier(fieldName), fieldType))
	}

	query := fmt.Sprintf(`
		CREATE TYPE IF NOT EXISTS %s.%s (
			%s
		)
	`, QuoteIdentifier(keyspace), QuoteIdentifier(typeInfo.Name), strings.Join(fieldDefs, ",\n\t\t\t"))

	return session.Query(query).Exec()
}

// createTableFromUnified creates a table from UnifiedModel Table
func createTableFromUnified(session *gocql.Session, table unifiedmodel.Table) error {
	keyspace := table.Comment // Keyspace is stored in comment
	if keyspace == "" {
		keyspace = "default"
	}

	var columnDefs []string
	var primaryKeys []string

	for _, column := range table.Columns {
		columnDef := fmt.Sprintf("%s %s", QuoteIdentifier(column.Name), column.DataType)
		columnDefs = append(columnDefs, columnDef)

		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, QuoteIdentifier(column.Name))
		}
	}

	primaryKeyClause := ""
	if len(primaryKeys) > 0 {
		primaryKeyClause = fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			%s%s
		)
	`, QuoteIdentifier(keyspace), QuoteIdentifier(table.Name),
		strings.Join(columnDefs, ",\n\t\t\t"), primaryKeyClause)

	return session.Query(query).Exec()
}

// createMaterializedViewFromUnified creates a materialized view from UnifiedModel MaterializedView
func createMaterializedViewFromUnified(session *gocql.Session, view unifiedmodel.MaterializedView) error {
	query := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS
		SELECT * FROM %s.%s
		WHERE id IS NOT NULL
		PRIMARY KEY (id)
	`, "default", QuoteIdentifier(view.Name),
		"default", "base_table")

	return session.Query(query).Exec()
}

// createFunctionFromUnified creates a function from UnifiedModel Function
func createFunctionFromUnified(session *gocql.Session, function unifiedmodel.Function) error {
	// Extract keyspace from function name if it contains a dot
	parts := strings.Split(function.Name, ".")
	var keyspace, functionName string
	if len(parts) == 2 {
		keyspace = parts[0]
		functionName = parts[1]
	} else {
		keyspace = "default"
		functionName = function.Name
	}

	query := fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.%s()
		RETURNS %s
		LANGUAGE %s
		AS '%s'
	`, QuoteIdentifier(keyspace), QuoteIdentifier(functionName),
		function.Returns, function.Language, function.Definition)

	return session.Query(query).Exec()
}

// QuoteIdentifier quotes a Cassandra identifier
func QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}
