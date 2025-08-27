package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

	// Get tables and convert to unified model format
	tables, err := getTables(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting tables: %w", err)
	}
	for _, table := range tables {
		unifiedTable := ConvertMySQLTableToUnified(table)
		um.Tables[table.Name] = unifiedTable
	}

	// Get enum types and convert to unified types
	enumTypes, err := getEnumTypes(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting enum types: %w", err)
	}
	for _, enumType := range enumTypes {
		um.Types[enumType.Name] = unifiedmodel.Type{
			Name:     enumType.Name,
			Category: "enum",
			Definition: map[string]any{
				"values": enumType.Values,
			},
		}
	}

	// Get schemas
	schemas, err := getSchemas(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %w", err)
	}
	for _, schema := range schemas {
		um.Schemas[schema.Name] = unifiedmodel.Schema{
			Name:    schema.Name,
			Comment: schema.Description,
		}
	}

	// Get functions
	functions, err := getFunctions(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %w", err)
	}
	for _, function := range functions {
		um.Functions[function.Name] = unifiedmodel.Function{
			Name:       function.Name,
			Language:   "sql", // Default for MySQL
			Returns:    function.ReturnType,
			Definition: function.Body,
		}
	}

	// Get triggers
	triggers, err := getTriggers(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting triggers: %w", err)
	}
	for _, trigger := range triggers {
		um.Triggers[trigger.Name] = unifiedmodel.Trigger{
			Name:      trigger.Name,
			Table:     trigger.Table,
			Timing:    trigger.Timing,
			Events:    []string{trigger.Event},
			Procedure: trigger.Statement,
		}
	}

	// MySQL doesn't have sequences like PostgreSQL, but we can use AUTO_INCREMENT columns
	// as a similar concept
	sequences, err := getSequences(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %w", err)
	}
	for _, sequence := range sequences {
		um.Sequences[sequence.Name] = unifiedmodel.Sequence{
			Name:      sequence.Name,
			Start:     sequence.Start,
			Increment: sequence.Increment,
			Min:       &sequence.MinValue,
			Max:       &sequence.MaxValue,
			Cache:     &sequence.CacheSize,
			Cycle:     sequence.Cycle,
		}
	}

	// Get extensions (plugins in MySQL)
	extensions, err := getExtensions(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("error getting extensions: %w", err)
	}
	for _, extension := range extensions {
		um.Extensions[extension.Name] = unifiedmodel.Extension{
			Name:    extension.Name,
			Version: extension.Version,
		}
	}

	return um, nil
}

// CreateStructure creates database structure based on provided parameters
func CreateStructure(db *sql.DB, params common.StructureParams) error {
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Sort tables based on dependencies
	sortedTables, err := common.TopologicalSort(params.Tables)
	if err != nil {
		return fmt.Errorf("error sorting tables: %v", err)
	}

	// Create tables
	for _, tableInfo := range sortedTables {
		err = CreateTable(tx, tableInfo)
		if err != nil {
			return fmt.Errorf("error creating table %s: %v", tableInfo.Name, err)
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// getTables retrieves tables from the database
func getTables(db *sql.DB) ([]common.TableInfo, error) {
	query := `
		SELECT 
			TABLE_SCHEMA,
			TABLE_NAME
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()
		AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []common.TableInfo
	for rows.Next() {
		var table common.TableInfo
		err := rows.Scan(&table.Schema, &table.Name)
		if err != nil {
			return nil, err
		}

		// Get columns for this table
		columns, err := getTableColumns(db, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("error getting columns for table %s: %w", table.Name, err)
		}
		table.Columns = columns

		// Get constraints for this table
		constraints, err := getConstraints(db, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("error getting constraints for table %s: %w", table.Name, err)
		}
		table.Constraints = constraints

		// Get indexes for this table
		indexes, err := getIndexes(db, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("error getting indexes for table %s: %w", table.Name, err)
		}
		table.Indexes = indexes

		// Set primary key
		for _, col := range table.Columns {
			if col.IsPrimaryKey {
				table.PrimaryKey = append(table.PrimaryKey, col.Name)
			}
		}

		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

// getTableColumns retrieves columns for a specific table
func getTableColumns(db *sql.DB, schema, table string) ([]common.ColumnInfo, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_COMMENT,
			CHARACTER_MAXIMUM_LENGTH,
			EXTRA
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []common.ColumnInfo
	for rows.Next() {
		var col common.ColumnInfo
		var columnDefault sql.NullString
		var description sql.NullString
		var varcharLength sql.NullInt64
		var extra string
		var isNullable string

		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&isNullable,
			&columnDefault,
			&description,
			&varcharLength,
			&extra,
		)
		if err != nil {
			return nil, err
		}

		// Set nullable flag
		col.IsNullable = isNullable == "YES"

		// Set default value if present
		if columnDefault.Valid {
			defaultVal := columnDefault.String
			col.ColumnDefault = &defaultVal
		}

		// Check for auto increment
		col.IsAutoIncrement = strings.Contains(extra, "auto_increment")

		// Check for generated columns
		if strings.Contains(extra, "GENERATED") {
			col.IsGenerated = true
			// Extract generation expression if available
			genExpr := extractGenerationExpression(extra)
			if genExpr != "" {
				col.GenerationExpression = &genExpr
			}
		}

		// Set varchar length if applicable
		if varcharLength.Valid && (col.DataType == "varchar" || col.DataType == "char") {
			length := int(varcharLength.Int64)
			col.VarcharLength = &length
		}

		// Check if column is part of primary key (will be updated later with constraints)
		// This is just a placeholder, will be properly set when processing constraints

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Get primary key information
	pkQuery := `
		SELECT k.COLUMN_NAME
		FROM information_schema.table_constraints t
		JOIN information_schema.key_column_usage k
		USING(constraint_name,table_schema,table_name)
		WHERE t.constraint_type='PRIMARY KEY'
		AND t.table_schema=?
		AND t.table_name=?`

	pkRows, err := db.Query(pkQuery, schema, table)
	if err != nil {
		return nil, err
	}
	defer pkRows.Close()

	// Mark primary key columns
	pkColumns := make(map[string]bool)
	for pkRows.Next() {
		var colName string
		if err := pkRows.Scan(&colName); err != nil {
			return nil, err
		}
		pkColumns[colName] = true
	}

	// Update primary key flag
	for i := range columns {
		if pkColumns[columns[i].Name] {
			columns[i].IsPrimaryKey = true
		}
	}

	return columns, nil
}

// getConstraints retrieves constraints for a specific table
func getConstraints(db *sql.DB, schema, table string) ([]common.Constraint, error) {
	query := `
		SELECT
			CONSTRAINT_NAME,
			CONSTRAINT_TYPE,
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM information_schema.key_column_usage k
		JOIN information_schema.table_constraints t
		USING (CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME)
		WHERE k.TABLE_SCHEMA = ?
		AND k.TABLE_NAME = ?
		AND k.REFERENCED_TABLE_SCHEMA IS NOT NULL`

	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []common.Constraint
	for rows.Next() {
		var c common.Constraint
		var refTable, refColumn sql.NullString

		err := rows.Scan(
			&c.Name,
			&c.Type,
			&c.Column,
			&refTable,
			&refColumn,
		)
		if err != nil {
			return nil, err
		}

		c.Table = table

		if refTable.Valid && refColumn.Valid {
			c.ForeignKey = &common.ForeignKeyInfo{
				Table:  refTable.String,
				Column: refColumn.String,
			}

			// Get ON DELETE and ON UPDATE actions
			actionsQuery := `
				SELECT
					DELETE_RULE,
					UPDATE_RULE
				FROM information_schema.referential_constraints
				WHERE CONSTRAINT_SCHEMA = ?
				AND CONSTRAINT_NAME = ?`

			var deleteRule, updateRule string
			err := db.QueryRow(actionsQuery, schema, c.Name).Scan(&deleteRule, &updateRule)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}

			if err == nil {
				c.ForeignKey.OnDelete = deleteRule
				c.ForeignKey.OnUpdate = updateRule
			}
		}

		constraints = append(constraints, c)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return constraints, nil
}

// getIndexes retrieves indexes for a specific table
func getIndexes(db *sql.DB, schema, table string) ([]common.IndexInfo, error) {
	query := `
		SELECT 
			INDEX_NAME,
			GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX),
			MAX(NON_UNIQUE)
		FROM information_schema.statistics
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND INDEX_NAME != 'PRIMARY'
		GROUP BY INDEX_NAME`

	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []common.IndexInfo
	for rows.Next() {
		var idx common.IndexInfo
		var columnsStr string
		var nonUnique int

		err := rows.Scan(
			&idx.Name,
			&columnsStr,
			&nonUnique,
		)
		if err != nil {
			return nil, err
		}

		idx.Columns = strings.Split(columnsStr, ",")
		idx.IsUnique = nonUnique == 0

		indexes = append(indexes, idx)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return indexes, nil
}

// getEnumTypes retrieves enum types from the database
func getEnumTypes(db *sql.DB) ([]common.EnumInfo, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			COLUMN_TYPE
		FROM information_schema.columns
		WHERE TABLE_SCHEMA = DATABASE()
		AND DATA_TYPE = 'enum'`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Map to store unique enum types
	enumMap := make(map[string][]string)

	for rows.Next() {
		var name string
		var columnType string

		if err := rows.Scan(&name, &columnType); err != nil {
			return nil, err
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

	var enums []common.EnumInfo
	for name, values := range enumMap {
		enums = append(enums, common.EnumInfo{
			Name:   name,
			Values: values,
		})
	}

	return enums, nil
}

// getSchemas retrieves schemas from the database
func getSchemas(db *sql.DB) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT SCHEMA_NAME 
		FROM information_schema.SCHEMATA 
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []common.DatabaseSchemaInfo
	for rows.Next() {
		var schema common.DatabaseSchemaInfo
		err := rows.Scan(&schema.Name)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return schemas, nil
}

// getFunctions retrieves functions from the database
func getFunctions(db *sql.DB) ([]common.FunctionInfo, error) {
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
		return nil, err
	}
	defer rows.Close()

	var functions []common.FunctionInfo
	for rows.Next() {
		var function common.FunctionInfo
		var paramStyle string
		err := rows.Scan(
			&function.Schema,
			&function.Name,
			&paramStyle,
			&function.ReturnType,
			&function.Body,
		)
		if err != nil {
			return nil, err
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

		argsRows, err := db.Query(argsQuery, function.Schema, function.Name)
		if err != nil {
			return nil, err
		}

		var args []string
		for argsRows.Next() {
			var name, dataType string
			if err := argsRows.Scan(&name, &dataType); err != nil {
				argsRows.Close()
				return nil, err
			}
			args = append(args, fmt.Sprintf("%s %s", name, dataType))
		}
		argsRows.Close()

		function.Arguments = strings.Join(args, ", ")
		functions = append(functions, function)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return functions, nil
}

// getTriggers retrieves triggers from the database
func getTriggers(db *sql.DB) ([]common.TriggerInfo, error) {
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
		return nil, err
	}
	defer rows.Close()

	var triggers []common.TriggerInfo
	for rows.Next() {
		var trigger common.TriggerInfo
		err := rows.Scan(
			&trigger.Schema,
			&trigger.Name,
			&trigger.Table,
			&trigger.Timing,
			&trigger.Event,
			&trigger.Statement,
		)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, trigger)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return triggers, nil
}

// getSequences retrieves auto-increment columns as sequences
func getSequences(db *sql.DB) ([]common.SequenceInfo, error) {
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
		return nil, err
	}
	defer rows.Close()

	var sequences []common.SequenceInfo
	for rows.Next() {
		var seq common.SequenceInfo
		var schema, table, column, dataType string
		var startValue int64

		err := rows.Scan(
			&schema,
			&table,
			&column,
			&dataType,
			&startValue,
		)
		if err != nil {
			return nil, err
		}

		seq.Schema = schema
		seq.Name = fmt.Sprintf("%s_%s_seq", table, column)
		seq.DataType = dataType
		seq.Start = startValue
		seq.Increment = 1
		seq.MinValue = 1
		seq.MaxValue = 9223372036854775807 // Default max value for BIGINT
		seq.CacheSize = 1
		seq.Cycle = false

		sequences = append(sequences, seq)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sequences, nil
}

// getExtensions retrieves installed plugins as extensions
func getExtensions(db *sql.DB) ([]common.ExtensionInfo, error) {
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
		return nil, err
	}
	defer rows.Close()

	var extensions []common.ExtensionInfo
	for rows.Next() {
		var ext common.ExtensionInfo
		var status, pluginType string

		err := rows.Scan(
			&ext.Name,
			&ext.Version,
			&status,
			&pluginType,
			&ext.Description,
		)
		if err != nil {
			return nil, err
		}

		ext.Schema = "mysql"
		extensions = append(extensions, ext)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return extensions, nil
}

// CreateTable creates a new table in the database
func CreateTable(tx *sql.Tx, tableInfo common.TableInfo) error {
	if tableInfo.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Start building the CREATE TABLE statement
	var query strings.Builder
	fmt.Fprintf(&query, "CREATE TABLE %s (\n", QuoteIdentifier(tableInfo.Name))

	// Add columns
	for i, col := range tableInfo.Columns {
		fmt.Fprintf(&query, "  %s %s", QuoteIdentifier(col.Name), col.DataType)

		// Add varchar length if specified
		if col.VarcharLength != nil && (strings.Contains(col.DataType, "varchar") || strings.Contains(col.DataType, "char")) {
			fmt.Fprintf(&query, "(%d)", *col.VarcharLength)
		}

		// Add NOT NULL constraint if needed
		if !col.IsNullable {
			fmt.Fprint(&query, " NOT NULL")
		}

		// Add DEFAULT if specified
		if col.ColumnDefault != nil {
			fmt.Fprintf(&query, " DEFAULT %s", *col.ColumnDefault)
		}

		// Add AUTO_INCREMENT if needed
		if col.IsAutoIncrement {
			fmt.Fprint(&query, " AUTO_INCREMENT")
		}

		// Add comma if not the last column
		if i < len(tableInfo.Columns)-1 {
			fmt.Fprint(&query, ",\n")
		}
	}

	// Add primary key if specified
	if len(tableInfo.PrimaryKey) > 0 {
		if len(tableInfo.Columns) > 0 {
			fmt.Fprint(&query, ",\n")
		}
		fmt.Fprintf(&query, "  PRIMARY KEY (%s)", strings.Join(common.QuoteStringSlice(tableInfo.PrimaryKey), ", "))
	}

	// Close the CREATE TABLE statement
	fmt.Fprint(&query, "\n)")

	// Execute the CREATE TABLE statement
	_, err := tx.Exec(query.String())
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	// Add indexes
	for _, index := range tableInfo.Indexes {
		var indexQuery strings.Builder
		if index.IsUnique {
			fmt.Fprintf(&indexQuery, "CREATE UNIQUE INDEX %s ON %s (%s)",
				QuoteIdentifier(index.Name),
				QuoteIdentifier(tableInfo.Name),
				strings.Join(common.QuoteStringSlice(index.Columns), ", "))
		} else {
			fmt.Fprintf(&indexQuery, "CREATE INDEX %s ON %s (%s)",
				QuoteIdentifier(index.Name),
				QuoteIdentifier(tableInfo.Name),
				strings.Join(common.QuoteStringSlice(index.Columns), ", "))
		}

		_, err := tx.Exec(indexQuery.String())
		if err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	// Add foreign key constraints
	for _, constraint := range tableInfo.Constraints {
		if constraint.ForeignKey != nil {
			var constraintQuery strings.Builder
			fmt.Fprintf(&constraintQuery, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				QuoteIdentifier(tableInfo.Name),
				QuoteIdentifier(constraint.Name),
				QuoteIdentifier(constraint.Column),
				QuoteIdentifier(constraint.ForeignKey.Table),
				QuoteIdentifier(constraint.ForeignKey.Column))

			if constraint.ForeignKey.OnDelete != "" {
				fmt.Fprintf(&constraintQuery, " ON DELETE %s", constraint.ForeignKey.OnDelete)
			}

			if constraint.ForeignKey.OnUpdate != "" {
				fmt.Fprintf(&constraintQuery, " ON UPDATE %s", constraint.ForeignKey.OnUpdate)
			}

			_, err := tx.Exec(constraintQuery.String())
			if err != nil {
				return fmt.Errorf("error creating foreign key constraint %s: %v", constraint.Name, err)
			}
		}
	}

	return nil
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
