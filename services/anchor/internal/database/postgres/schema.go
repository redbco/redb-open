package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DiscoverSchema fetches the current schema of a PostgreSQL database and returns a UnifiedModel
func DiscoverSchema(pool *pgxpool.Pool) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.PostgreSQL,
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
	err = discoverTablesAndColumnsUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get enum types directly as UnifiedModel types
	err = discoverEnumTypesUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering enum types: %v", err)
	}

	// Get schemas directly as UnifiedModel types
	err = getSchemasUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Get functions directly as UnifiedModel types
	err = getFunctionsUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Get triggers directly as UnifiedModel types
	err = getTriggersUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Get sequences directly as UnifiedModel types
	err = getSequencesUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get extensions directly as UnifiedModel types
	err = getExtensionsUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error getting extensions: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Start a transaction
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback(context.Background())

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
		if schema.Name != "public" { // Skip default schema
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

	// Create sequences
	for _, sequence := range um.Sequences {
		if err := createSequence(tx, sequence); err != nil {
			return fmt.Errorf("error creating sequence %s: %v", sequence.Name, err)
		}
	}

	// Create extensions
	for _, extension := range um.Extensions {
		if err := createExtension(tx, extension); err != nil {
			return fmt.Errorf("error creating extension %s: %v", extension.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func createEnumType(tx pgx.Tx, enumName string, enumValues []string) error {
	enumSQL := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)",
		enumName,
		strings.Join(quoteStringSlice(enumValues), ", "))
	_, err := tx.Exec(context.Background(), enumSQL)
	if err != nil {
		return fmt.Errorf("error creating enum type %s: %v", enumName, err)
	}
	return nil
}

// quoteStringSlice quotes each string in a slice for safe SQL usage
func quoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
	}
	return quoted
}

// sortTablesByDependencies sorts tables based on foreign key dependencies
// Note: This is a simplified implementation that returns tables in map iteration order.
// PostgreSQL allows creating tables in any order since we create constraints separately,
// so dependency sorting is not strictly required for this implementation.
func sortTablesByDependencies(tables map[string]unifiedmodel.Table) ([]unifiedmodel.Table, error) {
	var sortedTables []unifiedmodel.Table
	for _, table := range tables {
		sortedTables = append(sortedTables, table)
	}
	return sortedTables, nil
}

// createSchema creates a database schema
func createSchema(tx pgx.Tx, schemaName string) error {
	schemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName)
	_, err := tx.Exec(context.Background(), schemaSQL)
	if err != nil {
		return fmt.Errorf("error creating schema %s: %v", schemaName, err)
	}
	return nil
}

// CreateTableFromUnified creates a table from UnifiedModel Table
func CreateTableFromUnified(tx pgx.Tx, table unifiedmodel.Table, types map[string]unifiedmodel.Type) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	if table.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Check if the table already exists
	var exists bool
	err := tx.QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)", table.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists {
		return fmt.Errorf("table '%s' already exists", table.Name)
	}

	// Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", table.Name)
	columnCount := 0
	primaryKeys := []string{}

	for _, column := range table.Columns {
		if columnCount > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s ", column.Name)

		// Handle data type
		createTableSQL += mapUnifiedDataTypeToPostgres(column.DataType)

		if !column.Nullable {
			createTableSQL += " NOT NULL"
		}
		if column.Default != "" {
			createTableSQL += fmt.Sprintf(" DEFAULT %s", column.Default)
		}

		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, column.Name)
		}

		columnCount++
	}

	// Add primary key constraint
	if len(primaryKeys) > 0 {
		createTableSQL += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
	}

	createTableSQL += ")"

	// Print the SQL statement for debugging
	fmt.Printf("Creating table %s with SQL: %s\n", table.Name, createTableSQL)

	_, err = tx.Exec(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Create indexes
	for _, index := range table.Indexes {
		// Skip primary key indexes
		if isPrimaryKeyIndex(index.Name, table.Name) {
			continue
		}
		indexSQL := "CREATE"
		if index.Unique {
			indexSQL += " UNIQUE"
		}
		indexSQL += fmt.Sprintf(" INDEX %s ON %s (%s)",
			index.Name, table.Name, strings.Join(index.Columns, ", "))
		_, err = tx.Exec(context.Background(), indexSQL)
		if err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

// AddTableConstraintsFromUnified adds constraints from UnifiedModel Table
func AddTableConstraintsFromUnified(tx pgx.Tx, table unifiedmodel.Table) error {
	addedConstraints := make(map[string]bool)
	for _, constraint := range table.Constraints {
		// Skip if constraint has already been added
		if addedConstraints[constraint.Name] {
			continue
		}

		var constraintSQL string
		switch constraint.Type {
		case unifiedmodel.ConstraintTypeForeignKey:
			if constraint.Expression == "" {
				fmt.Printf("Warning: Skipping empty foreign key constraint definition %s for table %s\n", constraint.Name, table.Name)
				continue
			}
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
				table.Name, constraint.Name, constraint.Expression)
		case unifiedmodel.ConstraintTypeCheck:
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
				table.Name, constraint.Name, constraint.Expression)
		case unifiedmodel.ConstraintTypeUnique:
			// Unique constraints should specify columns, but we'll use expression for now
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
				table.Name, constraint.Name, constraint.Expression)
		case unifiedmodel.ConstraintTypePrimaryKey:
			// Skip primary key constraints as they are handled during table creation
			continue
		default:
			return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
		}

		// Print the SQL statement for debugging
		fmt.Printf("Executing constraint SQL: %s\n", constraintSQL)

		_, err := tx.Exec(context.Background(), constraintSQL)
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

// createView creates a view from UnifiedModel View
func createView(tx pgx.Tx, view unifiedmodel.View) error {
	viewSQL := fmt.Sprintf("CREATE VIEW %s AS %s", view.Name, view.Definition)
	_, err := tx.Exec(context.Background(), viewSQL)
	if err != nil {
		return fmt.Errorf("error creating view %s: %v", view.Name, err)
	}
	return nil
}

// createFunction creates a function from UnifiedModel Function
func createFunction(tx pgx.Tx, function unifiedmodel.Function) error {
	// This is a simplified implementation - real implementation would need to handle parameters, etc.
	functionSQL := fmt.Sprintf("CREATE OR REPLACE FUNCTION %s() RETURNS %s AS $$ %s $$ LANGUAGE %s",
		function.Name, function.Returns, function.Definition, function.Language)
	_, err := tx.Exec(context.Background(), functionSQL)
	if err != nil {
		return fmt.Errorf("error creating function %s: %v", function.Name, err)
	}
	return nil
}

// createTrigger creates a trigger from UnifiedModel Trigger
func createTrigger(tx pgx.Tx, trigger unifiedmodel.Trigger) error {
	// This is a simplified implementation
	events := strings.Join(trigger.Events, " OR ")
	triggerSQL := fmt.Sprintf("CREATE TRIGGER %s %s %s ON %s FOR EACH ROW EXECUTE FUNCTION %s",
		trigger.Name, trigger.Timing, events, trigger.Table, trigger.Procedure)
	_, err := tx.Exec(context.Background(), triggerSQL)
	if err != nil {
		return fmt.Errorf("error creating trigger %s: %v", trigger.Name, err)
	}
	return nil
}

// createSequence creates a sequence from UnifiedModel Sequence
func createSequence(tx pgx.Tx, sequence unifiedmodel.Sequence) error {
	sequenceSQL := fmt.Sprintf("CREATE SEQUENCE %s START %d INCREMENT %d",
		sequence.Name, sequence.Start, sequence.Increment)

	if sequence.Min != nil {
		sequenceSQL += fmt.Sprintf(" MINVALUE %d", *sequence.Min)
	}
	if sequence.Max != nil {
		sequenceSQL += fmt.Sprintf(" MAXVALUE %d", *sequence.Max)
	}
	if sequence.Cache != nil {
		sequenceSQL += fmt.Sprintf(" CACHE %d", *sequence.Cache)
	}
	if sequence.Cycle {
		sequenceSQL += " CYCLE"
	}

	_, err := tx.Exec(context.Background(), sequenceSQL)
	if err != nil {
		return fmt.Errorf("error creating sequence %s: %v", sequence.Name, err)
	}
	return nil
}

// createExtension creates an extension from UnifiedModel Extension
func createExtension(tx pgx.Tx, extension unifiedmodel.Extension) error {
	extensionSQL := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", extension.Name)
	if extension.Version != "" {
		extensionSQL += fmt.Sprintf(" VERSION '%s'", extension.Version)
	}
	_, err := tx.Exec(context.Background(), extensionSQL)
	if err != nil {
		return fmt.Errorf("error creating extension %s: %v", extension.Name, err)
	}
	return nil
}

// mapUnifiedDataTypeToPostgres maps UnifiedModel data types to PostgreSQL types
func mapUnifiedDataTypeToPostgres(dataType string) string {
	switch strings.ToLower(dataType) {
	case "integer", "int32":
		return "INTEGER"
	case "bigint", "int64":
		return "BIGINT"
	case "smallint", "int16":
		return "SMALLINT"
	case "boolean", "bool":
		return "BOOLEAN"
	case "varchar", "string":
		return "VARCHAR"
	case "text":
		return "TEXT"
	case "timestamp":
		return "TIMESTAMP"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "decimal", "numeric":
		return "DECIMAL"
	case "float", "float32":
		return "REAL"
	case "double", "float64":
		return "DOUBLE PRECISION"
	case "uuid":
		return "UUID"
	case "json":
		return "JSON"
	case "jsonb":
		return "JSONB"
	case "bytea", "binary":
		return "BYTEA"
	default:
		// Return as-is for custom types or unrecognized types
		return strings.ToUpper(dataType)
	}
}

// isPrimaryKeyIndex checks if an index is a primary key index
func isPrimaryKeyIndex(indexName, tableName string) bool {
	return indexName == tableName+"_pkey" || strings.HasSuffix(indexName, "_pkey")
}

// discoverTablesAndColumnsUnified discovers tables and columns directly into UnifiedModel
func discoverTablesAndColumnsUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.udt_name as custom_type_name,
            CASE 
                WHEN c.data_type = 'ARRAY' THEN (
                    SELECT e.data_type 
                    FROM information_schema.element_types e
                    WHERE e.object_catalog = c.table_catalog
                      AND e.object_schema = c.table_schema
                      AND e.object_name = c.table_name
                      AND e.object_type = 'TABLE'
                      AND e.collection_type_identifier = c.dtd_identifier
                )
                ELSE NULL
            END as array_element_type,
            a.atttypmod,
            CASE 
                WHEN pk.constraint_name IS NOT NULL THEN true
                ELSE false
            END as is_primary_key,
            c.data_type = 'ARRAY' as is_array,
            CASE
                WHEN u.constraint_name IS NOT NULL THEN true
                ELSE false
            END as is_unique,
            CASE
                WHEN pg_get_serial_sequence(t.table_name, c.column_name) IS NOT NULL THEN true
                ELSE false
            END as is_auto_increment,
            CASE
                WHEN t.table_type = 'BASE TABLE' AND EXISTS (
                    SELECT 1 FROM pg_class pc
                    WHERE pc.relname = t.table_name AND pc.relkind = 'p'
                ) THEN 'postgres.partitioned'
                WHEN t.table_type = 'BASE TABLE' AND EXISTS (
                    SELECT 1 FROM pg_inherits
                    WHERE inhrelid = t.table_name::regclass
                ) THEN 'postgres.partition'
                WHEN t.table_type = 'BASE TABLE' AND EXISTS (
                    SELECT 1 FROM pg_class pc
                    JOIN pg_index pi ON pc.oid = pi.indrelid
                    WHERE pc.relname = t.table_name AND pi.indisprimary
                ) THEN 'postgres.indexed'
                WHEN t.table_type = 'LOCAL TEMPORARY' THEN 'postgres.temporary'
                ELSE 'postgres.standard'
            END as table_type,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM pg_inherits
                    WHERE inhrelid = t.table_name::regclass
                ) THEN (
                    SELECT parent.relname
                    FROM pg_inherits
                    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                    WHERE inhrelid = t.table_name::regclass
                )
                ELSE NULL
            END as parent_table,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM pg_inherits
                    WHERE inhrelid = t.table_name::regclass
                ) THEN (
                    SELECT pg_get_expr(c.relpartbound, c.oid)
                    FROM pg_class c
                    WHERE c.relname = t.table_name
                )
                ELSE NULL
            END as partition_value
        FROM 
            information_schema.tables t
        JOIN 
            information_schema.columns c ON t.table_name = c.table_name
        JOIN 
            pg_attribute a ON a.attrelid = c.table_name::regclass AND a.attname = c.column_name
        LEFT JOIN 
            (SELECT kcu.table_name, kcu.column_name, tc.constraint_name
             FROM information_schema.key_column_usage kcu
             JOIN information_schema.table_constraints tc 
                ON kcu.constraint_name = tc.constraint_name
             WHERE tc.constraint_type = 'PRIMARY KEY') pk
        ON c.table_name = pk.table_name AND c.column_name = pk.column_name
        LEFT JOIN 
            (SELECT kcu.table_name, kcu.column_name, tc.constraint_name
             FROM information_schema.key_column_usage kcu
             JOIN information_schema.table_constraints tc 
                ON kcu.constraint_name = tc.constraint_name
             WHERE tc.constraint_type = 'UNIQUE') u
        ON c.table_name = u.table_name AND c.column_name = u.column_name
        WHERE 
            t.table_schema = 'public' AND
            c.table_schema = 'public' AND
            a.attnum > 0 AND
            t.table_type IN ('BASE TABLE', 'LOCAL TEMPORARY')
        ORDER BY 
            t.table_name, c.ordinal_position
    `

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error fetching table and column information: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, columnName, dataType, isNullable string
		var columnDefault, arrayElementType, customTypeName, parentTable, partitionValue sql.NullString
		var atttypmod sql.NullInt64
		var isPrimaryKey, isArray, isUnique, isAutoIncrement bool
		var tableType string

		if err := rows.Scan(
			&schemaName, &tableName, &columnName, &dataType, &isNullable, &columnDefault, &customTypeName,
			&arrayElementType, &atttypmod, &isPrimaryKey, &isArray, &isUnique, &isAutoIncrement, &tableType, &parentTable, &partitionValue,
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
			Name:          columnName,
			DataType:      dataType,
			Nullable:      isNullable == "YES",
			IsPrimaryKey:  isPrimaryKey,
			AutoIncrement: isAutoIncrement,
		}

		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		// Handle array types
		if isArray && arrayElementType.Valid {
			column.DataType = arrayElementType.String + "[]"
		}

		// Handle custom types
		if dataType == "USER-DEFINED" && customTypeName.Valid {
			column.DataType = customTypeName.String
		}

		// Handle varchar length
		if dataType == "character varying" && atttypmod.Valid && atttypmod.Int64 != -1 {
			varcharLength := int(atttypmod.Int64) - 4
			column.DataType = fmt.Sprintf("varchar(%d)", varcharLength)
		}

		table.Columns[columnName] = column
		um.Tables[tableName] = table
	}

	// Get indexes for all tables
	err = discoverIndexesUnified(pool, um)
	if err != nil {
		return fmt.Errorf("error discovering indexes: %v", err)
	}

	// Get constraints for all tables
	err = discoverConstraintsUnified(pool, um)
	if err != nil {
		return fmt.Errorf("error discovering constraints: %v", err)
	}

	// Handle partitioning info for partitioned tables
	// Note: We'll need to track table types during discovery to handle partitioning
	// For now, we'll check all tables for partitioning info
	for tableName, table := range um.Tables {
		// Check if table has partitioning info by querying directly
		err := fetchPartitioningInfoUnified(pool, tableName, &table)
		if err != nil {
			// If error is just "no partitioning info", continue
			if !strings.Contains(err.Error(), "no rows") {
				return fmt.Errorf("error fetching partitioning info for table %s: %v", tableName, err)
			}
		}
		um.Tables[tableName] = table
	}

	return nil
}

// discoverIndexesUnified discovers indexes directly into UnifiedModel
func discoverIndexesUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			schemaname,
			tablename,
			indexname,
			indexdef
		FROM pg_indexes 
		WHERE schemaname = 'public'
		ORDER BY tablename, indexname
	`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName, indexName, indexDef string
		if err := rows.Scan(&schemaName, &tableName, &indexName, &indexDef); err != nil {
			return fmt.Errorf("error scanning index row: %v", err)
		}

		// Skip primary key indexes as they're handled in table creation
		if isPrimaryKeyIndex(indexName, tableName) {
			continue
		}

		if table, exists := um.Tables[tableName]; exists {
			index := unifiedmodel.Index{
				Name:   indexName,
				Unique: strings.Contains(strings.ToUpper(indexDef), "UNIQUE"),
			}

			// Extract column names from index definition (simplified)
			// This is a basic implementation - could be enhanced for complex indexes
			if strings.Contains(indexDef, "(") && strings.Contains(indexDef, ")") {
				start := strings.Index(indexDef, "(")
				end := strings.LastIndex(indexDef, ")")
				if start < end {
					columnsPart := indexDef[start+1 : end]
					columns := strings.Split(columnsPart, ",")
					for i, col := range columns {
						columns[i] = strings.TrimSpace(col)
					}
					index.Columns = columns
				}
			}

			table.Indexes[indexName] = index
			um.Tables[tableName] = table
		}
	}

	return nil
}

// discoverConstraintsUnified discovers constraints directly into UnifiedModel
func discoverConstraintsUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			tc.table_name,
			tc.constraint_name,
			tc.constraint_type,
			pg_get_constraintdef(pgc.oid) as constraint_definition
		FROM information_schema.table_constraints tc
		JOIN pg_constraint pgc ON pgc.conname = tc.constraint_name
		WHERE tc.table_schema = 'public'
		AND tc.constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE')
		ORDER BY tc.table_name, tc.constraint_name
	`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, constraintName, constraintType, constraintDef string
		if err := rows.Scan(&tableName, &constraintName, &constraintType, &constraintDef); err != nil {
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
				Name:       constraintName,
				Type:       umConstraintType,
				Expression: constraintDef,
			}

			table.Constraints[constraintName] = constraint
			um.Tables[tableName] = table
		}
	}

	return nil
}

// fetchPartitioningInfoUnified fetches partitioning information directly into UnifiedModel
func fetchPartitioningInfoUnified(pool *pgxpool.Pool, tableName string, table *unifiedmodel.Table) error {
	query := `
		SELECT
			pg_get_partkeydef(c.oid) AS partition_key,
			CASE
				WHEN c.relkind = 'p' THEN 'RANGE'
				WHEN c.relkind = 'h' THEN 'HASH'
				WHEN c.relkind = 'l' THEN 'LIST'
			END AS partition_strategy,
			array_agg(c2.relname)::text[] AS partitions
		FROM
			pg_class c
		LEFT JOIN
			pg_inherits i ON i.inhparent = c.oid
		LEFT JOIN
			pg_class c2 ON i.inhrelid = c2.oid
		WHERE
			c.relname = $1
		GROUP BY
			c.oid, c.relkind;
	`

	var partitionKey, partitionStrategy sql.NullString
	var partitions []sql.NullString

	row := pool.QueryRow(context.Background(), query, tableName)
	err := row.Scan(&partitionKey, &partitionStrategy, &partitions)
	if err != nil {
		return fmt.Errorf("error querying partitioning info: %v", err)
	}

	// Store partitioning info in table options
	if table.Options == nil {
		table.Options = make(map[string]any)
	}

	if partitionStrategy.Valid {
		table.Options["partition_strategy"] = partitionStrategy.String
	}

	if partitionKey.Valid {
		// Extract column names from partition key definition
		keyDef := partitionKey.String
		startIndex := strings.Index(keyDef, "(")
		endIndex := strings.LastIndex(keyDef, ")")
		if startIndex != -1 && endIndex != -1 && endIndex > startIndex {
			keyDef = keyDef[startIndex+1 : endIndex]
			keys := strings.Split(keyDef, ",")
			for i, key := range keys {
				keys[i] = strings.TrimSpace(key)
			}
			table.Options["partition_key"] = keys
		}
	}

	if len(partitions) > 0 {
		// Convert []sql.NullString to []string, filtering out NULL values
		validPartitions := make([]string, 0, len(partitions))
		for _, p := range partitions {
			if p.Valid {
				validPartitions = append(validPartitions, p.String)
			}
		}
		if len(validPartitions) > 0 {
			table.Options["partitions"] = validPartitions
		}
	}

	return nil
}

// discoverEnumTypesUnified discovers enum types directly into UnifiedModel
func discoverEnumTypesUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT t.typname AS enum_name, 
               e.enumlabel AS enum_value
        FROM pg_type t 
        JOIN pg_enum e ON t.oid = e.enumtypid  
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public'
        ORDER BY t.typname, e.enumsortorder
    `
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error fetching enum types: %v", err)
	}
	defer rows.Close()

	enumMap := make(map[string][]string)
	for rows.Next() {
		var enumName, enumValue string
		if err := rows.Scan(&enumName, &enumValue); err != nil {
			return fmt.Errorf("error scanning enum row: %v", err)
		}
		enumMap[enumName] = append(enumMap[enumName], enumValue)
	}

	for enumName, enumValues := range enumMap {
		um.Types[enumName] = unifiedmodel.Type{
			Name:     enumName,
			Category: "enum",
			Definition: map[string]any{
				"values": enumValues,
			},
		}
	}

	return rows.Err()
}

// getSchemasUnified gets schemas directly into UnifiedModel
func getSchemasUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			nspname as schema_name,
			pg_catalog.obj_description(oid, 'pg_namespace') as description
		FROM pg_catalog.pg_namespace
		WHERE nspname !~ '^pg_'
		AND nspname != 'information_schema'
		ORDER BY nspname;`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName string
		var description sql.NullString
		if err := rows.Scan(&schemaName, &description); err != nil {
			return fmt.Errorf("error scanning schema: %v", err)
		}

		schema := unifiedmodel.Schema{
			Name: schemaName,
		}
		if description.Valid {
			schema.Comment = description.String
		}
		um.Schemas[schemaName] = schema
	}

	return nil
}

// getFunctionsUnified gets functions directly into UnifiedModel
func getFunctionsUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `SELECT n.nspname AS schema_name, p.proname AS function_name, 
              pg_get_function_arguments(p.oid) AS argument_data_types,
              t.typname AS return_type,
              p.prosrc AS function_body
              FROM pg_proc p
              LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
              LEFT JOIN pg_type t ON p.prorettype = t.oid
              WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')`
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, functionName, arguments, returnType, body string
		if err := rows.Scan(&schemaName, &functionName, &arguments, &returnType, &body); err != nil {
			return err
		}

		um.Functions[functionName] = unifiedmodel.Function{
			Name:       functionName,
			Language:   "plpgsql", // Default for PostgreSQL
			Returns:    returnType,
			Definition: body,
		}
	}
	return rows.Err()
}

// getTriggersUnified gets triggers directly into UnifiedModel
func getTriggersUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			tgname as trigger_name,
			n.nspname as schema_name,
			c.relname as table_name,
			CASE 
				WHEN tgtype & 2 > 0 THEN 'BEFORE'
				WHEN tgtype & 64 > 0 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END as timing,
			CASE 
				WHEN tgtype & 4 > 0 THEN 'INSERT'
				WHEN tgtype & 8 > 0 THEN 'DELETE'
				WHEN tgtype & 16 > 0 THEN 'UPDATE'
				ELSE 'TRUNCATE'
			END as event,
			pg_get_triggerdef(t.oid) as definition
		FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE NOT t.tgisinternal
		AND n.nspname = 'public'
		ORDER BY t.tgname;`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var triggerName, schemaName, tableName, timing, event, definition string
		if err := rows.Scan(&triggerName, &schemaName, &tableName, &timing, &event, &definition); err != nil {
			return fmt.Errorf("error scanning trigger: %v", err)
		}

		um.Triggers[triggerName] = unifiedmodel.Trigger{
			Name:      triggerName,
			Table:     tableName,
			Timing:    timing,
			Events:    []string{event},
			Procedure: definition,
		}
	}

	return nil
}

// getSequencesUnified gets sequences directly into UnifiedModel
func getSequencesUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	// First, check the PostgreSQL version
	var version string
	err := pool.QueryRow(context.Background(), "SHOW server_version_num").Scan(&version)
	if err != nil {
		return fmt.Errorf("error fetching PostgreSQL version: %v", err)
	}

	versionNum, err := strconv.Atoi(version)
	if err != nil {
		return fmt.Errorf("error parsing PostgreSQL version: %v", err)
	}

	var query string
	if versionNum >= 100000 {
		// PostgreSQL 10 and later
		query = `
        SELECT 
            n.nspname AS schema,
            s.relname AS name,
            format_type(s.reltype, NULL) AS data_type,
            seq.seqstart,
            seq.seqincrement,
            seq.seqmax,
            seq.seqmin,
            seq.seqcache,
            seq.seqcycle
        FROM pg_class s
        JOIN pg_namespace n ON n.oid = s.relnamespace
        JOIN pg_sequence seq ON seq.seqrelid = s.oid
        WHERE s.relkind = 'S'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, s.relname
        `
	} else {
		// PostgreSQL 9.6 and earlier
		query = `
        SELECT 
            n.nspname AS schema,
            s.relname AS name,
            format_type(s.reltype, NULL) AS data_type,
            NULL AS seqstart,
            NULL AS seqincrement,
            NULL AS seqmax,
            NULL AS seqmin,
            NULL AS seqcache,
            NULL AS seqcycle
        FROM pg_class s
        JOIN pg_namespace n ON n.oid = s.relnamespace
        WHERE s.relkind = 'S'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, s.relname
        `
	}

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, seqName, dataType string
		var startValue, incrementBy, maxValue, minValue, cacheSize sql.NullInt64
		var isCycled sql.NullBool

		err = rows.Scan(
			&schemaName,
			&seqName,
			&dataType,
			&startValue,
			&incrementBy,
			&maxValue,
			&minValue,
			&cacheSize,
			&isCycled,
		)

		if err != nil {
			return fmt.Errorf("error scanning sequence row: %v", err)
		}

		sequence := unifiedmodel.Sequence{
			Name: seqName,
		}

		// Set values only if they are not NULL
		if startValue.Valid {
			sequence.Start = startValue.Int64
		}
		if incrementBy.Valid {
			sequence.Increment = incrementBy.Int64
		}
		if maxValue.Valid {
			sequence.Max = &maxValue.Int64
		}
		if minValue.Valid {
			sequence.Min = &minValue.Int64
		}
		if cacheSize.Valid {
			sequence.Cache = &cacheSize.Int64
		}
		if isCycled.Valid {
			sequence.Cycle = isCycled.Bool
		}

		um.Sequences[seqName] = sequence
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating sequence rows: %v", err)
	}

	return nil
}

// getExtensionsUnified gets extensions directly into UnifiedModel
func getExtensionsUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			extname as extension_name,
			extversion as version,
			n.nspname as schema_name,
			e.extconfig::text[] as tables,
			pg_catalog.obj_description(e.oid, 'pg_extension') as description
		FROM pg_extension e
		JOIN pg_namespace n ON e.extnamespace = n.oid
		ORDER BY extname;`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying extensions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var extName, version, schemaName string
		var tables []string
		var description sql.NullString
		if err := rows.Scan(&extName, &version, &schemaName, &tables, &description); err != nil {
			return fmt.Errorf("error scanning extension: %v", err)
		}

		extension := unifiedmodel.Extension{
			Name:    extName,
			Version: version,
		}

		// Note: UnifiedModel Extension doesn't have a Comment field
		// We could store description in Options if needed
		if description.Valid {
			if extension.Options == nil {
				extension.Options = make(map[string]any)
			}
			extension.Options["description"] = description.String
		}

		um.Extensions[extName] = extension
	}

	return nil
}
