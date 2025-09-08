package mysql

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema discovers the schema of a MySQL database and returns a UnifiedModel
func DiscoverSchema(db interface{}) (*unifiedmodel.UnifiedModel, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid MySQL connection type")
	}

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MySQL,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Types:        make(map[string]unifiedmodel.Type),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}

	var err error

	// Get tables and their columns directly as UnifiedModel types
	err = discoverTablesAndColumnsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get enum types directly as UnifiedModel types
	err = discoverEnumTypesUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering enum types: %v", err)
	}

	// Get schemas directly as UnifiedModel types
	err = getSchemasUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Get functions directly as UnifiedModel types
	err = getFunctionsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Get triggers directly as UnifiedModel types
	err = getTriggersUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Get sequences directly as UnifiedModel types
	err = getSequencesUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get extensions directly as UnifiedModel types
	err = getExtensionsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error getting extensions: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Create enum types first
	for _, umType := range um.Types {
		if umType.Category == "enum" {
			values := []string{}
			if definition, ok := umType.Definition["values"].([]string); ok {
				values = definition
			} else if definition, ok := umType.Definition["values"].([]interface{}); ok {
				for _, v := range definition {
					if str, ok := v.(string); ok {
						values = append(values, str)
					}
				}
			}
			if len(values) > 0 {
				if err := createEnumType(tx, umType.Name, values); err != nil {
					return fmt.Errorf("error creating enum type %s: %v", umType.Name, err)
				}
			}
		}
	}

	// Create schemas first
	for _, schema := range um.Schemas {
		if schema.Name != "mysql" && schema.Name != "information_schema" && schema.Name != "performance_schema" && schema.Name != "sys" {
			if err := createSchema(tx, schema.Name); err != nil {
				return fmt.Errorf("error creating schema %s: %v", schema.Name, err)
			}
		}
	}

	// Sort tables based on dependencies (we'll need to implement this for UnifiedModel)
	sortedTables, err := sortTablesByDependencies(um.Tables)
	if err != nil {
		return fmt.Errorf("error sorting tables: %v", err)
	}

	// Create tables
	for _, table := range sortedTables {
		if err := CreateTableFromUnified(tx, table, um.Types); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Add table constraints
	for _, table := range sortedTables {
		if err := AddTableConstraintsFromUnified(tx, table); err != nil {
			return fmt.Errorf("error adding constraints to table %s: %v", table.Name, err)
		}
	}

	// Create views
	for _, view := range um.Views {
		if err := createView(tx, view); err != nil {
			return fmt.Errorf("error creating view %s: %v", view.Name, err)
		}
	}

	// Create functions
	for _, function := range um.Functions {
		if err := createFunction(tx, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	// Create triggers
	for _, trigger := range um.Triggers {
		if err := createTrigger(tx, trigger); err != nil {
			return fmt.Errorf("error creating trigger %s: %v", trigger.Name, err)
		}
	}

	// Create sequences (AUTO_INCREMENT in MySQL)
	for _, sequence := range um.Sequences {
		if err := createSequence(tx, sequence); err != nil {
			return fmt.Errorf("error creating sequence %s: %v", sequence.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// discoverTablesAndColumnsUnified discovers tables and columns directly into UnifiedModel
func discoverTablesAndColumnsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.column_comment,
            c.character_maximum_length,
            c.extra
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
        WHERE t.table_schema = DATABASE()
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name, c.ordinal_position`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error fetching table and column information: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, columnName, dataType, isNullable string
		var columnDefault, columnComment sql.NullString
		var varcharLength sql.NullInt64
		var extra string

		if err := rows.Scan(
			&schemaName, &tableName, &columnName, &dataType, &isNullable, &columnDefault, &columnComment,
			&varcharLength, &extra,
		); err != nil {
			return fmt.Errorf("error scanning table and column row: %v", err)
		}

		// Get or create table
		table, exists := um.Tables[tableName]
		if !exists {
			table = unifiedmodel.Table{
				Name:        tableName,
				Columns:     make(map[string]unifiedmodel.Column),
				Indexes:     make(map[string]unifiedmodel.Index),
				Constraints: make(map[string]unifiedmodel.Constraint),
			}
		}

		// Create column
		column := unifiedmodel.Column{
			Name:     columnName,
			DataType: dataType,
			Nullable: isNullable == "YES",
		}

		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		// Check for auto increment
		column.AutoIncrement = strings.Contains(extra, "auto_increment")

		// Check for generated columns
		if strings.Contains(extra, "GENERATED") {
			genExpr := extractGenerationExpression(extra)
			if genExpr != "" {
				column.GeneratedExpression = genExpr
			}
		}

		// Set varchar length if applicable
		if varcharLength.Valid && (dataType == "varchar" || dataType == "char") {
			length := int(varcharLength.Int64)
			column.DataType = fmt.Sprintf("%s(%d)", dataType, length)
		}

		table.Columns[columnName] = column
		um.Tables[tableName] = table
	}

	// Get primary keys for all tables
	err = discoverPrimaryKeysUnified(db, um)
	if err != nil {
		return fmt.Errorf("error discovering primary keys: %v", err)
	}

	// Get indexes for all tables
	err = discoverIndexesUnified(db, um)
	if err != nil {
		return fmt.Errorf("error discovering indexes: %v", err)
	}

	// Get constraints for all tables
	err = discoverConstraintsUnified(db, um)
	if err != nil {
		return fmt.Errorf("error discovering constraints: %v", err)
	}

	return nil
}

// discoverPrimaryKeysUnified discovers primary keys directly into UnifiedModel
func discoverPrimaryKeysUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.table_name,
			k.column_name
		FROM information_schema.table_constraints t
		JOIN information_schema.key_column_usage k
		USING(constraint_name,table_schema,table_name)
		WHERE t.constraint_type='PRIMARY KEY'
		AND t.table_schema = DATABASE()
		ORDER BY t.table_name, k.ordinal_position`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying primary keys: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, columnName string
		if err := rows.Scan(&tableName, &columnName); err != nil {
			return fmt.Errorf("error scanning primary key row: %v", err)
		}

		if table, exists := um.Tables[tableName]; exists {
			if column, colExists := table.Columns[columnName]; colExists {
				column.IsPrimaryKey = true
				table.Columns[columnName] = column
				um.Tables[tableName] = table
			}
		}
	}

	return nil
}

// discoverConstraintsUnified discovers constraints directly into UnifiedModel
func discoverConstraintsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			tc.table_name,
			tc.constraint_name,
			tc.constraint_type,
			kcu.column_name,
			kcu.referenced_table_name,
			kcu.referenced_column_name,
			rc.delete_rule,
			rc.update_rule
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name 
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		LEFT JOIN information_schema.referential_constraints rc
			ON tc.constraint_name = rc.constraint_name
			AND tc.table_schema = rc.constraint_schema
		WHERE tc.table_schema = DATABASE()
		AND tc.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE')
		ORDER BY tc.table_name, tc.constraint_name`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, constraintName, constraintType, columnName string
		var refTable, refColumn, deleteRule, updateRule sql.NullString
		if err := rows.Scan(&tableName, &constraintName, &constraintType, &columnName, &refTable, &refColumn, &deleteRule, &updateRule); err != nil {
			return fmt.Errorf("error scanning constraint row: %v", err)
		}

		if table, exists := um.Tables[tableName]; exists {
			var umConstraintType unifiedmodel.ConstraintType
			switch constraintType {
			case "FOREIGN KEY":
				umConstraintType = unifiedmodel.ConstraintTypeForeignKey
			case "CHECK":
				umConstraintType = unifiedmodel.ConstraintTypeCheck
			case "UNIQUE":
				umConstraintType = unifiedmodel.ConstraintTypeUnique
			default:
				continue // Skip unknown constraint types
			}

			constraint := unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    umConstraintType,
				Columns: []string{columnName},
			}

			if refTable.Valid && refColumn.Valid {
				constraint.Reference = unifiedmodel.Reference{
					Table:   refTable.String,
					Columns: []string{refColumn.String},
				}
				if deleteRule.Valid {
					constraint.Reference.OnDelete = deleteRule.String
				}
				if updateRule.Valid {
					constraint.Reference.OnUpdate = updateRule.String
				}
			}

			table.Constraints[constraintName] = constraint
			um.Tables[tableName] = table
		}
	}

	return nil
}

// discoverIndexesUnified discovers indexes directly into UnifiedModel
func discoverIndexesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			table_name,
			index_name,
			GROUP_CONCAT(column_name ORDER BY seq_in_index),
			MAX(non_unique)
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		AND index_name != 'PRIMARY'
		GROUP BY table_name, index_name`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, indexName, columnsStr string
		var nonUnique int
		if err := rows.Scan(&tableName, &indexName, &columnsStr, &nonUnique); err != nil {
			return fmt.Errorf("error scanning index row: %v", err)
		}

		if table, exists := um.Tables[tableName]; exists {
			index := unifiedmodel.Index{
				Name:    indexName,
				Columns: strings.Split(columnsStr, ","),
				Unique:  nonUnique == 0,
			}

			table.Indexes[indexName] = index
			um.Tables[tableName] = table
		}
	}

	return nil
}

// discoverEnumTypesUnified discovers enum types directly into UnifiedModel
func discoverEnumTypesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			COLUMN_NAME,
			COLUMN_TYPE
		FROM information_schema.columns
		WHERE TABLE_SCHEMA = DATABASE()
		AND DATA_TYPE = 'enum'`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error fetching enum types: %v", err)
	}
	defer rows.Close()

	// Map to store unique enum types
	enumMap := make(map[string][]string)

	for rows.Next() {
		var name string
		var columnType string

		if err := rows.Scan(&name, &columnType); err != nil {
			return fmt.Errorf("error scanning enum row: %v", err)
		}

		// Extract enum values from column type
		// Format is typically: enum('value1','value2',...)
		re := regexp.MustCompile(`'([^']*)'`)
		matches := re.FindAllStringSubmatch(columnType, -1)

		var values []string
		for _, match := range matches {
			if len(match) > 1 {
				values = append(values, match[1])
			}
		}

		enumMap[name] = values
	}

	for name, values := range enumMap {
		um.Types[name] = unifiedmodel.Type{
			Name:     name,
			Category: "enum",
			Definition: map[string]any{
				"values": values,
			},
		}
	}

	return rows.Err()
}

// getSchemasUnified gets schemas directly into UnifiedModel
func getSchemasUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT SCHEMA_NAME 
		FROM information_schema.SCHEMATA 
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return fmt.Errorf("error scanning schema: %v", err)
		}

		schema := unifiedmodel.Schema{
			Name: schemaName,
		}
		um.Schemas[schemaName] = schema
	}

	return nil
}

// getFunctionsUnified gets functions directly into UnifiedModel
func getFunctionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			ROUTINE_SCHEMA,
			ROUTINE_NAME,
			PARAMETER_STYLE,
			DTD_IDENTIFIER,
			ROUTINE_DEFINITION
		FROM information_schema.ROUTINES
		WHERE ROUTINE_TYPE = 'FUNCTION'
		AND ROUTINE_SCHEMA = DATABASE()`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, functionName, paramStyle, returnType, body string
		if err := rows.Scan(&schemaName, &functionName, &paramStyle, &returnType, &body); err != nil {
			return fmt.Errorf("error scanning function: %v", err)
		}

		// Get function arguments
		argsQuery := `
			SELECT 
				PARAMETER_NAME,
				DATA_TYPE
			FROM information_schema.PARAMETERS
			WHERE SPECIFIC_SCHEMA = ?
			AND SPECIFIC_NAME = ?
			AND PARAMETER_MODE IS NOT NULL
			ORDER BY ORDINAL_POSITION`

		argsRows, err := db.Query(argsQuery, schemaName, functionName)
		if err != nil {
			return fmt.Errorf("error querying function arguments: %v", err)
		}

		var args []unifiedmodel.Argument
		for argsRows.Next() {
			var name, dataType string
			if err := argsRows.Scan(&name, &dataType); err != nil {
				argsRows.Close()
				return fmt.Errorf("error scanning function argument: %v", err)
			}
			args = append(args, unifiedmodel.Argument{
				Name: name,
				Type: dataType,
			})
		}
		argsRows.Close()

		um.Functions[functionName] = unifiedmodel.Function{
			Name:       functionName,
			Language:   "sql", // Default for MySQL
			Returns:    returnType,
			Arguments:  args,
			Definition: body,
		}
	}

	return nil
}

// getTriggersUnified gets triggers directly into UnifiedModel
func getTriggersUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			TRIGGER_SCHEMA,
			TRIGGER_NAME,
			EVENT_OBJECT_TABLE,
			ACTION_TIMING,
			EVENT_MANIPULATION,
			ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = DATABASE()`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, triggerName, tableName, timing, event, statement string
		if err := rows.Scan(&schemaName, &triggerName, &tableName, &timing, &event, &statement); err != nil {
			return fmt.Errorf("error scanning trigger: %v", err)
		}

		um.Triggers[triggerName] = unifiedmodel.Trigger{
			Name:      triggerName,
			Table:     tableName,
			Timing:    timing,
			Events:    []string{event},
			Procedure: statement,
		}
	}

	return nil
}

// getSequencesUnified gets sequences directly into UnifiedModel
func getSequencesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			c.TABLE_SCHEMA,
			c.TABLE_NAME,
			c.COLUMN_NAME,
			c.DATA_TYPE,
			t.AUTO_INCREMENT
		FROM information_schema.COLUMNS c
		JOIN information_schema.TABLES t
		ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
		WHERE c.TABLE_SCHEMA = DATABASE()
		AND c.EXTRA LIKE '%auto_increment%'
		AND t.AUTO_INCREMENT IS NOT NULL`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table, column, dataType string
		var startValue int64

		if err := rows.Scan(&schema, &table, &column, &dataType, &startValue); err != nil {
			return fmt.Errorf("error scanning sequence: %v", err)
		}

		seqName := fmt.Sprintf("%s_%s_seq", table, column)
		maxValue := int64(9223372036854775807) // Default max value for BIGINT
		minValue := int64(1)
		cacheSize := int64(1)

		um.Sequences[seqName] = unifiedmodel.Sequence{
			Name:      seqName,
			Start:     startValue,
			Increment: 1,
			Min:       &minValue,
			Max:       &maxValue,
			Cache:     &cacheSize,
			Cycle:     false,
		}
	}

	return nil
}

// getExtensionsUnified gets extensions directly into UnifiedModel
func getExtensionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			PLUGIN_NAME,
			PLUGIN_VERSION,
			PLUGIN_STATUS,
			PLUGIN_TYPE,
			PLUGIN_DESCRIPTION
		FROM information_schema.PLUGINS
		WHERE PLUGIN_STATUS = 'ACTIVE'`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying extensions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, version, status, pluginType, description string

		if err := rows.Scan(&name, &version, &status, &pluginType, &description); err != nil {
			return fmt.Errorf("error scanning extension: %v", err)
		}

		extension := unifiedmodel.Extension{
			Name:    name,
			Version: version,
		}

		// Store description in Options if needed
		if description != "" {
			if extension.Options == nil {
				extension.Options = make(map[string]any)
			}
			extension.Options["description"] = description
			extension.Options["type"] = pluginType
		}

		um.Extensions[name] = extension
	}

	return nil
}

// CreateTableFromUnified creates a table from UnifiedModel Table
func CreateTableFromUnified(tx *sql.Tx, table unifiedmodel.Table, types map[string]unifiedmodel.Type) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	if table.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Check if the table already exists
	var exists bool
	err := tx.QueryRow("SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists {
		return fmt.Errorf("table '%s' already exists", table.Name)
	}

	// Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", QuoteIdentifier(table.Name))
	columnCount := 0
	primaryKeys := []string{}

	for _, column := range table.Columns {
		if columnCount > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s ", QuoteIdentifier(column.Name))

		// Handle data type
		createTableSQL += mapUnifiedDataTypeToMySQL(column.DataType)

		if !column.Nullable {
			createTableSQL += " NOT NULL"
		}
		if column.Default != "" {
			createTableSQL += fmt.Sprintf(" DEFAULT %s", column.Default)
		}
		if column.AutoIncrement {
			createTableSQL += " AUTO_INCREMENT"
		}

		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, column.Name)
		}

		columnCount++
	}

	// Add primary key constraint
	if len(primaryKeys) > 0 {
		createTableSQL += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(quoteStringSlice(primaryKeys), ", "))
	}

	createTableSQL += ")"

	// Print the SQL statement for debugging
	fmt.Printf("Creating table %s with SQL: %s\n", table.Name, createTableSQL)

	_, err = tx.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Create indexes
	for _, index := range table.Indexes {
		indexSQL := "CREATE"
		if index.Unique {
			indexSQL += " UNIQUE"
		}
		indexSQL += fmt.Sprintf(" INDEX %s ON %s (%s)",
			QuoteIdentifier(index.Name), QuoteIdentifier(table.Name), strings.Join(quoteStringSlice(index.Columns), ", "))
		_, err = tx.Exec(indexSQL)
		if err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

// AddTableConstraintsFromUnified adds constraints from UnifiedModel Table
func AddTableConstraintsFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	addedConstraints := make(map[string]bool)
	for _, constraint := range table.Constraints {
		// Skip if constraint has already been added
		if addedConstraints[constraint.Name] {
			continue
		}

		var constraintSQL string
		switch constraint.Type {
		case unifiedmodel.ConstraintTypeForeignKey:
			if len(constraint.Reference.Table) == 0 || len(constraint.Reference.Columns) == 0 {
				fmt.Printf("Warning: Skipping incomplete foreign key constraint definition %s for table %s\n", constraint.Name, table.Name)
				continue
			}
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				QuoteIdentifier(table.Name), QuoteIdentifier(constraint.Name),
				strings.Join(quoteStringSlice(constraint.Columns), ", "),
				QuoteIdentifier(constraint.Reference.Table),
				strings.Join(quoteStringSlice(constraint.Reference.Columns), ", "))

			if constraint.Reference.OnDelete != "" {
				constraintSQL += fmt.Sprintf(" ON DELETE %s", constraint.Reference.OnDelete)
			}
			if constraint.Reference.OnUpdate != "" {
				constraintSQL += fmt.Sprintf(" ON UPDATE %s", constraint.Reference.OnUpdate)
			}
		case unifiedmodel.ConstraintTypeCheck:
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
				QuoteIdentifier(table.Name), QuoteIdentifier(constraint.Name), constraint.Expression)
		case unifiedmodel.ConstraintTypeUnique:
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
				QuoteIdentifier(table.Name), QuoteIdentifier(constraint.Name), strings.Join(quoteStringSlice(constraint.Columns), ", "))
		case unifiedmodel.ConstraintTypePrimaryKey:
			// Skip primary key constraints as they are handled during table creation
			continue
		default:
			return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
		}

		// Print the SQL statement for debugging
		fmt.Printf("Executing constraint SQL: %s\n", constraintSQL)

		_, err := tx.Exec(constraintSQL)
		if err != nil {
			// If an error occurs, print the error message and full SQL statement
			fmt.Printf("Error adding constraint %s: %v\n", constraint.Name, err)
			fmt.Printf("Full SQL statement: %s\n", constraintSQL)
			return fmt.Errorf("error adding constraint %s: %v", constraint.Name, err)
		}
		addedConstraints[constraint.Name] = true
	}

	return nil
}

// sortTablesByDependencies sorts tables based on foreign key dependencies
// Note: This is a simplified implementation that returns tables in map iteration order.
// MySQL allows creating tables in any order since we create constraints separately,
// so dependency sorting is not strictly required for this implementation.
func sortTablesByDependencies(tables map[string]unifiedmodel.Table) ([]unifiedmodel.Table, error) {
	var sortedTables []unifiedmodel.Table
	for _, table := range tables {
		sortedTables = append(sortedTables, table)
	}
	return sortedTables, nil
}

// createEnumType creates an enum type (MySQL uses ENUM column type)
func createEnumType(tx *sql.Tx, enumName string, enumValues []string) error {
	// MySQL doesn't have standalone enum types like PostgreSQL
	// Enums are defined at the column level in MySQL
	// This function is a placeholder for compatibility
	return nil
}

// createSchema creates a database schema (MySQL uses databases instead of schemas)
func createSchema(tx *sql.Tx, schemaName string) error {
	schemaSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", QuoteIdentifier(schemaName))
	_, err := tx.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("error creating schema %s: %v", schemaName, err)
	}
	return nil
}

// createView creates a view from UnifiedModel View
func createView(tx *sql.Tx, view unifiedmodel.View) error {
	viewSQL := fmt.Sprintf("CREATE VIEW %s AS %s", QuoteIdentifier(view.Name), view.Definition)
	_, err := tx.Exec(viewSQL)
	if err != nil {
		return fmt.Errorf("error creating view %s: %v", view.Name, err)
	}
	return nil
}

// createFunction creates a function from UnifiedModel Function
func createFunction(tx *sql.Tx, function unifiedmodel.Function) error {
	// This is a simplified implementation - real implementation would need to handle parameters, etc.
	var args []string
	for _, arg := range function.Arguments {
		args = append(args, fmt.Sprintf("%s %s", arg.Name, arg.Type))
	}
	argsStr := strings.Join(args, ", ")

	functionSQL := fmt.Sprintf("CREATE FUNCTION %s(%s) RETURNS %s DETERMINISTIC %s",
		QuoteIdentifier(function.Name), argsStr, function.Returns, function.Definition)
	_, err := tx.Exec(functionSQL)
	if err != nil {
		return fmt.Errorf("error creating function %s: %v", function.Name, err)
	}
	return nil
}

// createTrigger creates a trigger from UnifiedModel Trigger
func createTrigger(tx *sql.Tx, trigger unifiedmodel.Trigger) error {
	// This is a simplified implementation
	events := strings.Join(trigger.Events, " OR ")
	triggerSQL := fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW %s",
		QuoteIdentifier(trigger.Name), trigger.Timing, events, QuoteIdentifier(trigger.Table), trigger.Procedure)
	_, err := tx.Exec(triggerSQL)
	if err != nil {
		return fmt.Errorf("error creating trigger %s: %v", trigger.Name, err)
	}
	return nil
}

// createSequence creates a sequence from UnifiedModel Sequence (MySQL uses AUTO_INCREMENT)
func createSequence(tx *sql.Tx, sequence unifiedmodel.Sequence) error {
	// MySQL doesn't have standalone sequences like PostgreSQL
	// AUTO_INCREMENT is handled at the column level
	// This function is a placeholder for compatibility
	return nil
}

// mapUnifiedDataTypeToMySQL maps UnifiedModel data types to MySQL types
func mapUnifiedDataTypeToMySQL(dataType string) string {
	switch strings.ToLower(dataType) {
	case "integer", "int32":
		return "INT"
	case "bigint", "int64":
		return "BIGINT"
	case "smallint", "int16":
		return "SMALLINT"
	case "tinyint", "int8":
		return "TINYINT"
	case "boolean", "bool":
		return "BOOLEAN"
	case "varchar", "string":
		return "VARCHAR(255)"
	case "text":
		return "TEXT"
	case "timestamp":
		return "TIMESTAMP"
	case "datetime":
		return "DATETIME"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "decimal", "numeric":
		return "DECIMAL"
	case "float", "float32":
		return "FLOAT"
	case "double", "float64":
		return "DOUBLE"
	case "binary":
		return "BINARY"
	case "varbinary":
		return "VARBINARY(255)"
	case "blob":
		return "BLOB"
	case "json":
		return "JSON"
	default:
		// Return as-is for custom types or unrecognized types
		return strings.ToUpper(dataType)
	}
}

// quoteStringSlice quotes each string in a slice for safe SQL usage
func quoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = QuoteIdentifier(s)
	}
	return quoted
}

// extractGenerationExpression extracts the generation expression from the EXTRA column
func extractGenerationExpression(extra string) string {
	re := regexp.MustCompile(`GENERATED ALWAYS AS \((.*)\)`)
	matches := re.FindStringSubmatch(extra)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}
