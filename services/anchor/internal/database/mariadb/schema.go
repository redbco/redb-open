package mariadb

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema discovers the schema of a MariaDB database and returns a UnifiedModel
func DiscoverSchema(db interface{}) (*unifiedmodel.UnifiedModel, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid MariaDB connection type")
	}

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.MariaDB,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Types:        make(map[string]unifiedmodel.Type),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}

	var err error

	// Get tables directly as UnifiedModel types
	err = discoverTablesAndColumnsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}

	// Get primary keys directly into UnifiedModel
	err = discoverPrimaryKeysUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering primary keys: %w", err)
	}

	// Get constraints directly into UnifiedModel
	err = discoverConstraintsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering constraints: %w", err)
	}

	// Get indexes directly into UnifiedModel
	err = discoverIndexesUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering indexes: %w", err)
	}

	// Get enum types directly into UnifiedModel
	err = discoverEnumTypesUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering enum types: %w", err)
	}

	// Get schemas directly into UnifiedModel
	err = discoverSchemasUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering schemas: %w", err)
	}

	// Get functions directly into UnifiedModel
	err = discoverFunctionsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %w", err)
	}

	// Get triggers directly into UnifiedModel
	err = discoverTriggersUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering triggers: %w", err)
	}

	// Get sequences directly into UnifiedModel
	err = discoverSequencesUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering sequences: %w", err)
	}

	// Get extensions directly into UnifiedModel
	err = discoverExtensionsUnified(sqlDB, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering extensions: %w", err)
	}

	return um, nil
}

// CreateStructure creates database structure from a UnifiedModel
func CreateStructure(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}
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

	// Create tables from UnifiedModel
	for _, table := range um.Tables {
		err = CreateTableFromUnified(tx, table)
		if err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// getTables retrieves tables from the database
// discoverTablesAndColumnsUnified discovers tables and columns directly into UnifiedModel
func discoverTablesAndColumnsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schema, tableName string
		err := rows.Scan(&schema, &tableName)
		if err != nil {
			return err
		}

		// Create table in UnifiedModel
		table := unifiedmodel.Table{
			Name:        tableName,
			Columns:     make(map[string]unifiedmodel.Column),
			Indexes:     make(map[string]unifiedmodel.Index),
			Constraints: make(map[string]unifiedmodel.Constraint),
		}

		// Get columns for this table
		err = getTableColumnsUnified(db, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting columns for table %s: %w", tableName, err)
		}

		um.Tables[tableName] = table
	}

	return rows.Err()
}

// getTableColumnsUnified gets columns for a table directly into UnifiedModel
func getTableColumnsUnified(db *sql.DB, schema, tableName string, table *unifiedmodel.Table) error {
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_COMMENT,
			CHARACTER_MAXIMUM_LENGTH,
			EXTRA,
			COLUMN_KEY,
			GENERATION_EXPRESSION
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType, isNullable, extra, columnKey string
		var columnDefault, columnComment, generationExpression sql.NullString
		var varcharLength sql.NullInt64

		err := rows.Scan(
			&columnName,
			&dataType,
			&isNullable,
			&columnDefault,
			&columnComment,
			&varcharLength,
			&extra,
			&columnKey,
			&generationExpression,
		)
		if err != nil {
			return err
		}

		column := unifiedmodel.Column{
			Name:          columnName,
			DataType:      dataType,
			Nullable:      isNullable == "YES",
			IsPrimaryKey:  columnKey == "PRI",
			AutoIncrement: strings.Contains(extra, "auto_increment"),
		}

		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		// Note: Comment and Length are not part of unifiedmodel.Column
		// They could be stored in Options if needed
		_ = columnComment // Suppress unused variable warning
		_ = varcharLength // Suppress unused variable warning

		if generationExpression.Valid {
			column.GeneratedExpression = generationExpression.String
		}

		table.Columns[columnName] = column
	}

	return rows.Err()
}

// discoverPrimaryKeysUnified discovers primary keys directly into UnifiedModel
func discoverPrimaryKeysUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	// Primary keys are already handled in getTableColumnsUnified
	// This function ensures primary key constraints are properly set
	for tableName, table := range um.Tables {
		var pkColumns []string
		for _, column := range table.Columns {
			if column.IsPrimaryKey {
				pkColumns = append(pkColumns, column.Name)
			}
		}

		if len(pkColumns) > 0 {
			table.Constraints["PRIMARY"] = unifiedmodel.Constraint{
				Name:    "PRIMARY",
				Type:    unifiedmodel.ConstraintTypePrimaryKey,
				Columns: pkColumns,
			}
			um.Tables[tableName] = table
		}
	}
	return nil
}

// discoverConstraintsUnified discovers constraints directly into UnifiedModel
func discoverConstraintsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT
			kcu.CONSTRAINT_NAME,
			kcu.TABLE_NAME,
			kcu.COLUMN_NAME,
			tc.CONSTRAINT_TYPE,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			rc.UPDATE_RULE,
			rc.DELETE_RULE
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.table_constraints tc
		ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
		AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
		LEFT JOIN information_schema.referential_constraints rc
		ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
		WHERE kcu.TABLE_SCHEMA = DATABASE()
		AND tc.CONSTRAINT_TYPE IN ('FOREIGN KEY', 'UNIQUE')
		ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME`

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, tableName, columnName, constraintType string
		var referencedTable, referencedColumn, updateRule, deleteRule sql.NullString

		err := rows.Scan(
			&constraintName,
			&tableName,
			&columnName,
			&constraintType,
			&referencedTable,
			&referencedColumn,
			&updateRule,
			&deleteRule,
		)
		if err != nil {
			return err
		}

		if table, exists := um.Tables[tableName]; exists {
			var cType unifiedmodel.ConstraintType
			switch constraintType {
			case "FOREIGN KEY":
				cType = unifiedmodel.ConstraintTypeForeignKey
			case "UNIQUE":
				cType = unifiedmodel.ConstraintTypeUnique
			default:
				continue
			}

			constraint := unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    cType,
				Columns: []string{columnName},
			}

			if referencedTable.Valid && referencedColumn.Valid {
				constraint.Reference = unifiedmodel.Reference{
					Table:   referencedTable.String,
					Columns: []string{referencedColumn.String},
				}
				if updateRule.Valid {
					constraint.Reference.OnUpdate = updateRule.String
				}
				if deleteRule.Valid {
					constraint.Reference.OnDelete = deleteRule.String
				}
			}

			table.Constraints[constraintName] = constraint
			um.Tables[tableName] = table
		}
	}

	return rows.Err()
}

// discoverIndexesUnified discovers indexes directly into UnifiedModel
func discoverIndexesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			TABLE_NAME,
			INDEX_NAME,
			GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX),
			MAX(NON_UNIQUE)
		FROM information_schema.statistics
		WHERE TABLE_SCHEMA = DATABASE()
		AND INDEX_NAME != 'PRIMARY'
		GROUP BY TABLE_NAME, INDEX_NAME`

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, indexName, columnsStr string
		var nonUnique int

		err := rows.Scan(&tableName, &indexName, &columnsStr, &nonUnique)
		if err != nil {
			return err
		}

		if table, exists := um.Tables[tableName]; exists {
			columns := strings.Split(columnsStr, ",")

			index := unifiedmodel.Index{
				Name:    indexName,
				Columns: columns,
				Unique:  nonUnique == 0,
			}

			table.Indexes[indexName] = index
			um.Tables[tableName] = table
		}
	}

	return rows.Err()
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
		return err
	}
	defer rows.Close()

	enumMap := make(map[string][]string)

	for rows.Next() {
		var name, columnType string
		if err := rows.Scan(&name, &columnType); err != nil {
			return err
		}

		// Extract enum values from column type
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

	// Convert to UnifiedModel types
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

// discoverSchemasUnified discovers schemas directly into UnifiedModel
func discoverSchemasUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT SCHEMA_NAME 
		FROM information_schema.SCHEMATA 
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')`

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName string
		err := rows.Scan(&schemaName)
		if err != nil {
			return err
		}

		um.Schemas[schemaName] = unifiedmodel.Schema{
			Name: schemaName,
		}
	}

	return rows.Err()
}

// discoverFunctionsUnified discovers functions directly into UnifiedModel
func discoverFunctionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schema, name, paramStyle, returnType, body string
		err := rows.Scan(&schema, &name, &paramStyle, &returnType, &body)
		if err != nil {
			return err
		}

		um.Functions[name] = unifiedmodel.Function{
			Name:       name,
			Language:   "sql",
			Returns:    returnType,
			Definition: body,
		}
	}

	return rows.Err()
}

// discoverTriggersUnified discovers triggers directly into UnifiedModel
func discoverTriggersUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schema, name, table, timing, event, statement string
		err := rows.Scan(&schema, &name, &table, &timing, &event, &statement)
		if err != nil {
			return err
		}

		um.Triggers[name] = unifiedmodel.Trigger{
			Name:      name,
			Table:     table,
			Timing:    timing,
			Events:    []string{event},
			Procedure: statement,
		}
	}

	return rows.Err()
}

// discoverSequencesUnified discovers sequences directly into UnifiedModel
func discoverSequencesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table, column, dataType string
		var startValue int64

		err := rows.Scan(&schema, &table, &column, &dataType, &startValue)
		if err != nil {
			return err
		}

		seqName := fmt.Sprintf("%s_%s_seq", table, column)
		um.Sequences[seqName] = unifiedmodel.Sequence{
			Name:      seqName,
			Start:     startValue,
			Increment: 1,
			Options: map[string]any{
				"dataType": dataType,
			},
		}
	}

	return rows.Err()
}

// discoverExtensionsUnified discovers extensions directly into UnifiedModel
func discoverExtensionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name, version, status, pluginType, description string
		err := rows.Scan(&name, &version, &status, &pluginType, &description)
		if err != nil {
			return err
		}

		um.Extensions[name] = unifiedmodel.Extension{
			Name:    name,
			Version: version,
		}
	}

	return rows.Err()
}

// CreateTableFromUnified creates a table from UnifiedModel Table
func CreateTableFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Start building the CREATE TABLE statement
	var query strings.Builder
	fmt.Fprintf(&query, "CREATE TABLE %s (\n", QuoteIdentifier(table.Name))

	// Add columns
	columnDefs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		var colDef strings.Builder
		fmt.Fprintf(&colDef, "  %s %s", QuoteIdentifier(col.Name), col.DataType)

		// Note: Length is not part of unifiedmodel.Column
		// For now, we skip length specification in CREATE TABLE

		// Add NOT NULL constraint if needed
		if !col.Nullable {
			fmt.Fprint(&colDef, " NOT NULL")
		}

		// Add DEFAULT if specified
		if col.Default != "" {
			fmt.Fprintf(&colDef, " DEFAULT %s", col.Default)
		}

		// Add AUTO_INCREMENT if needed
		if col.AutoIncrement {
			fmt.Fprint(&colDef, " AUTO_INCREMENT")
		}

		// Add generated expression if specified
		if col.GeneratedExpression != "" {
			fmt.Fprintf(&colDef, " AS (%s)", col.GeneratedExpression)
		}

		// Note: Comment is not part of unifiedmodel.Column
		// Comments would need to be stored in Options if needed

		columnDefs = append(columnDefs, colDef.String())
	}

	query.WriteString(strings.Join(columnDefs, ",\n"))

	// Add primary key constraint if exists
	if pkConstraint, exists := table.Constraints["PRIMARY"]; exists {
		fmt.Fprintf(&query, ",\n  PRIMARY KEY (%s)", strings.Join(QuoteStringSlice(pkConstraint.Columns), ", "))
	}

	query.WriteString("\n)")

	// Execute the CREATE TABLE statement
	if _, err := tx.Exec(query.String()); err != nil {
		return fmt.Errorf("error executing CREATE TABLE: %w", err)
	}

	// Create indexes
	for _, index := range table.Indexes {
		var indexQuery string
		if index.Unique {
			indexQuery = fmt.Sprintf(
				"CREATE UNIQUE INDEX %s ON %s (%s)",
				QuoteIdentifier(index.Name),
				QuoteIdentifier(table.Name),
				strings.Join(QuoteStringSlice(index.Columns), ", "))
		} else {
			indexQuery = fmt.Sprintf(
				"CREATE INDEX %s ON %s (%s)",
				QuoteIdentifier(index.Name),
				QuoteIdentifier(table.Name),
				strings.Join(QuoteStringSlice(index.Columns), ", "))
		}

		if _, err := tx.Exec(indexQuery); err != nil {
			return fmt.Errorf("error creating index %s: %w", index.Name, err)
		}
	}

	// Add foreign key constraints
	for _, constraint := range table.Constraints {
		if constraint.Type == unifiedmodel.ConstraintTypeForeignKey {
			fkQuery := fmt.Sprintf(
				"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				QuoteIdentifier(table.Name),
				QuoteIdentifier(constraint.Name),
				QuoteIdentifier(constraint.Columns[0]),
				QuoteIdentifier(constraint.Reference.Table),
				QuoteIdentifier(constraint.Reference.Columns[0]))

			if constraint.Reference.OnUpdate != "" {
				fkQuery += fmt.Sprintf(" ON UPDATE %s", constraint.Reference.OnUpdate)
			}
			if constraint.Reference.OnDelete != "" {
				fkQuery += fmt.Sprintf(" ON DELETE %s", constraint.Reference.OnDelete)
			}

			if _, err := tx.Exec(fkQuery); err != nil {
				return fmt.Errorf("error adding foreign key constraint %s: %w", constraint.Name, err)
			}
		}
	}

	return nil
}

// QuoteIdentifier quotes an identifier for MariaDB
func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteStringSlice quotes each string in a slice
func QuoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = QuoteIdentifier(s)
	}
	return quoted
}
