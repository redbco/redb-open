package cockroach

import (
	"context"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DiscoverSchema fetches the current schema of a CockroachDB database and returns a UnifiedModel
func DiscoverSchema(pool *pgxpool.Pool) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.CockroachDB,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Extensions:   make(map[string]unifiedmodel.Extension),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	var err error

	// Get tables directly as unifiedmodel types
	err = discoverTablesUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get enum types directly as unifiedmodel types
	err = discoverEnumTypesUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering enum types: %v", err)
	}

	// Get schemas directly as unifiedmodel types
	err = discoverSchemasUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering schemas: %v", err)
	}

	// Get functions directly as unifiedmodel types
	err = discoverFunctionsUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get triggers directly as unifiedmodel types
	err = discoverTriggersUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering triggers: %v", err)
	}

	// Get sequences directly as unifiedmodel types
	err = discoverSequencesUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering sequences: %v", err)
	}

	// Get extensions directly as unifiedmodel types
	err = discoverExtensionsUnified(pool, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering extensions: %v", err)
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

	// Create schemas from UnifiedModel
	for _, schema := range um.Schemas {
		if err := createSchemaFromUnified(tx, schema); err != nil {
			return fmt.Errorf("error creating schema %s: %v", schema.Name, err)
		}
	}

	// Create enum types from UnifiedModel
	for _, typeInfo := range um.Types {
		if typeInfo.Category == "enum" {
			if err := createEnumTypeFromUnified(tx, typeInfo); err != nil {
				return fmt.Errorf("error creating enum type %s: %v", typeInfo.Name, err)
			}
		}
	}

	// Create sequences from UnifiedModel
	for _, sequence := range um.Sequences {
		if err := createSequenceFromUnified(tx, sequence); err != nil {
			return fmt.Errorf("error creating sequence %s: %v", sequence.Name, err)
		}
	}

	// Create tables from UnifiedModel
	for _, table := range um.Tables {
		if err := createTableFromUnified(tx, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Create functions from UnifiedModel
	for _, function := range um.Functions {
		if err := createFunctionFromUnified(tx, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	// Create triggers from UnifiedModel
	for _, trigger := range um.Triggers {
		if err := createTriggerFromUnified(tx, trigger); err != nil {
			return fmt.Errorf("error creating trigger %s: %v", trigger.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// discoverTablesUnified discovers tables directly into UnifiedModel
func discoverTablesUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            t.table_schema,
            t.table_name,
            t.table_type,
            COALESCE(obj_description(c.oid), '') as table_comment
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')
        ORDER BY t.table_schema, t.table_name
    `

	rows, err := pool.Query(context.Background(), query)
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
		err := getTableColumnsUnified(pool, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting columns for table %s.%s: %v", schema, tableName, err)
		}

		// Get constraints for this table
		err = getTableConstraintsUnified(pool, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting constraints for table %s.%s: %v", schema, tableName, err)
		}

		// Get indexes for this table
		err = getTableIndexesUnified(pool, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting indexes for table %s.%s: %v", schema, tableName, err)
		}

		um.Tables[fmt.Sprintf("%s.%s", schema, tableName)] = table
	}

	return rows.Err()
}

// getTableColumnsUnified gets columns for a table directly into UnifiedModel
func getTableColumnsUnified(pool *pgxpool.Pool, schema, tableName string, tableModel *unifiedmodel.Table) error {
	query := `
        SELECT 
            c.column_name,
            c.data_type,
            c.is_nullable = 'YES' as is_nullable,
            COALESCE(c.column_default, '') as column_default,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
            COALESCE(col_description(pgc.oid, c.ordinal_position), '') as column_comment
        FROM information_schema.columns c
        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
        LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
        LEFT JOIN (
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
    `

	rows, err := pool.Query(context.Background(), query, schema, tableName)
	if err != nil {
		return fmt.Errorf("error querying columns: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType, columnDefault, comment string
		var isNullable, isPrimaryKey bool
		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &isPrimaryKey, &comment); err != nil {
			return fmt.Errorf("error scanning column row: %v", err)
		}

		column := unifiedmodel.Column{
			Name:         columnName,
			DataType:     dataType,
			Nullable:     isNullable,
			Default:      columnDefault,
			IsPrimaryKey: isPrimaryKey,
			Options:      map[string]any{"comment": comment},
		}

		tableModel.Columns[columnName] = column
	}

	return rows.Err()
}

// getTableConstraintsUnified gets constraints for a table directly into UnifiedModel
func getTableConstraintsUnified(pool *pgxpool.Pool, schema, tableName string, tableModel *unifiedmodel.Table) error {
	query := `
        SELECT 
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            COALESCE(ccu.table_schema, '') as referenced_schema,
            COALESCE(ccu.table_name, '') as referenced_table,
            COALESCE(ccu.column_name, '') as referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.constraint_schema
        WHERE tc.table_schema = $1 AND tc.table_name = $2
        ORDER BY tc.constraint_name, kcu.ordinal_position
    `

	rows, err := pool.Query(context.Background(), query, schema, tableName)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	constraintMap := make(map[string]*unifiedmodel.Constraint)

	for rows.Next() {
		var constraintName, constraintType, columnName, referencedSchema, referencedTable, referencedColumn string
		if err := rows.Scan(&constraintName, &constraintType, &columnName, &referencedSchema, &referencedTable, &referencedColumn); err != nil {
			return fmt.Errorf("error scanning constraint row: %v", err)
		}

		if constraint, exists := constraintMap[constraintName]; exists {
			constraint.Columns = append(constraint.Columns, columnName)
		} else {
			// Convert string constraint type to ConstraintType enum
			var cType unifiedmodel.ConstraintType
			switch constraintType {
			case "PRIMARY KEY":
				cType = unifiedmodel.ConstraintTypePrimaryKey
			case "FOREIGN KEY":
				cType = unifiedmodel.ConstraintTypeForeignKey
			case "UNIQUE":
				cType = unifiedmodel.ConstraintTypeUnique
			case "CHECK":
				cType = unifiedmodel.ConstraintTypeCheck
			default:
				cType = unifiedmodel.ConstraintType(constraintType)
			}

			constraint := &unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    cType,
				Columns: []string{columnName},
			}

			if constraintType == "FOREIGN KEY" && referencedTable != "" {
				constraint.Reference = unifiedmodel.Reference{
					Table:   fmt.Sprintf("%s.%s", referencedSchema, referencedTable),
					Columns: []string{referencedColumn},
				}
			}

			constraintMap[constraintName] = constraint
		}
	}

	for _, constraint := range constraintMap {
		tableModel.Constraints[constraint.Name] = *constraint
	}

	return rows.Err()
}

// getTableIndexesUnified gets indexes for a table directly into UnifiedModel
func getTableIndexesUnified(pool *pgxpool.Pool, schema, tableName string, tableModel *unifiedmodel.Table) error {
	query := `
        SELECT 
            i.indexname as index_name,
            i.indexdef as index_definition,
            CASE WHEN i.indexdef LIKE '%UNIQUE%' THEN true ELSE false END as is_unique
        FROM pg_indexes i
        WHERE i.schemaname = $1 AND i.tablename = $2
        AND i.indexname NOT LIKE '%_pkey'
        ORDER BY i.indexname
    `

	rows, err := pool.Query(context.Background(), query, schema, tableName)
	if err != nil {
		return fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var indexName, indexDefinition string
		var isUnique bool
		if err := rows.Scan(&indexName, &indexDefinition, &isUnique); err != nil {
			return fmt.Errorf("error scanning index row: %v", err)
		}

		// Extract columns from index definition (simplified approach)
		var columns []string
		if strings.Contains(indexDefinition, "(") && strings.Contains(indexDefinition, ")") {
			start := strings.Index(indexDefinition, "(") + 1
			end := strings.Index(indexDefinition, ")")
			columnsPart := indexDefinition[start:end]
			columns = strings.Split(strings.ReplaceAll(columnsPart, " ", ""), ",")
		}

		index := unifiedmodel.Index{
			Name:    indexName,
			Columns: columns,
			Unique:  isUnique,
			Type:    unifiedmodel.IndexTypeBTree, // CockroachDB primarily uses B-tree indexes
		}

		tableModel.Indexes[indexName] = index
	}

	return rows.Err()
}

// discoverEnumTypesUnified discovers enum types directly into UnifiedModel
func discoverEnumTypesUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            t.typname as type_name,
            array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'crdb_internal')
        GROUP BY t.typname
        ORDER BY t.typname
    `

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying enum types: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var typeName string
		var enumValues []string
		if err := rows.Scan(&typeName, &enumValues); err != nil {
			return fmt.Errorf("error scanning enum type row: %v", err)
		}

		typeInfo := unifiedmodel.Type{
			Name:     typeName,
			Category: "enum",
			Definition: map[string]any{
				"values": enumValues,
			},
		}

		um.Types[typeName] = typeInfo
	}

	return rows.Err()
}

// discoverSchemasUnified discovers schemas directly into UnifiedModel
func discoverSchemasUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            schema_name,
            COALESCE(obj_description(n.oid, 'pg_namespace'), '') as schema_comment
        FROM information_schema.schemata s
        LEFT JOIN pg_namespace n ON n.nspname = s.schema_name
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')
        ORDER BY schema_name
    `

	rows, err := pool.Query(context.Background(), query)
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
func discoverFunctionsUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            p.proname as function_name,
            l.lanname as language,
            COALESCE(p.prosrc, '') as function_body,
            pg_get_function_result(p.oid) as return_type
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_language l ON l.oid = p.prolang
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'crdb_internal')
        AND p.prokind = 'f'
        ORDER BY p.proname
    `

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, language, body, returnType string
		if err := rows.Scan(&name, &language, &body, &returnType); err != nil {
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

// discoverTriggersUnified discovers triggers directly into UnifiedModel
func discoverTriggersUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	// CockroachDB has limited trigger support, so this is mostly a placeholder
	query := `
        SELECT 
            trigger_name,
            event_object_table as table_name,
            action_statement
        FROM information_schema.triggers
        WHERE trigger_schema NOT IN ('information_schema', 'pg_catalog', 'crdb_internal')
        ORDER BY trigger_name
    `

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		// CockroachDB might not support triggers, so we ignore errors
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var name, tableName, statement string
		if err := rows.Scan(&name, &tableName, &statement); err != nil {
			return fmt.Errorf("error scanning trigger row: %v", err)
		}

		trigger := unifiedmodel.Trigger{
			Name:      name,
			Table:     tableName,
			Procedure: statement,
		}

		um.Triggers[name] = trigger
	}

	return rows.Err()
}

// discoverSequencesUnified discovers sequences directly into UnifiedModel
func discoverSequencesUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	query := `
        SELECT 
            sequence_name,
            start_value::bigint,
            increment::bigint,
            minimum_value::bigint,
            maximum_value::bigint,
            cycle_option = 'YES' as cycle
        FROM information_schema.sequences
        WHERE sequence_schema NOT IN ('information_schema', 'pg_catalog', 'crdb_internal')
        ORDER BY sequence_name
    `

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var start, increment, minValue, maxValue int64
		var cycle bool
		if err := rows.Scan(&name, &start, &increment, &minValue, &maxValue, &cycle); err != nil {
			return fmt.Errorf("error scanning sequence row: %v", err)
		}

		sequence := unifiedmodel.Sequence{
			Name:      name,
			Start:     start,
			Increment: increment,
			Min:       &minValue,
			Max:       &maxValue,
			Cycle:     cycle,
		}

		um.Sequences[name] = sequence
	}

	return rows.Err()
}

// discoverExtensionsUnified discovers extensions directly into UnifiedModel
func discoverExtensionsUnified(pool *pgxpool.Pool, um *unifiedmodel.UnifiedModel) error {
	// CockroachDB doesn't have traditional PostgreSQL extensions, so this is mostly a placeholder
	query := `
        SELECT 
            extname as extension_name,
            extversion as extension_version
        FROM pg_extension
        WHERE extname NOT IN ('plpgsql')
        ORDER BY extname
    `

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		// CockroachDB might not support extensions, so we ignore errors
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var name, version string
		if err := rows.Scan(&name, &version); err != nil {
			return fmt.Errorf("error scanning extension row: %v", err)
		}

		extension := unifiedmodel.Extension{
			Name:    name,
			Version: version,
		}

		um.Extensions[name] = extension
	}

	return rows.Err()
}

// createSchemaFromUnified creates a schema from UnifiedModel Schema
func createSchemaFromUnified(tx pgx.Tx, schema unifiedmodel.Schema) error {
	if schema.Name == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", QuoteIdentifier(schema.Name))
	if schema.Comment != "" {
		query += fmt.Sprintf("; COMMENT ON SCHEMA %s IS '%s'", QuoteIdentifier(schema.Name), strings.ReplaceAll(schema.Comment, "'", "''"))
	}

	_, err := tx.Exec(context.Background(), query)
	return err
}

// createEnumTypeFromUnified creates an enum type from UnifiedModel Type
func createEnumTypeFromUnified(tx pgx.Tx, typeInfo unifiedmodel.Type) error {
	if typeInfo.Name == "" {
		return fmt.Errorf("type name cannot be empty")
	}

	if typeInfo.Category != "enum" {
		return fmt.Errorf("type %s is not an enum", typeInfo.Name)
	}

	var values []string
	if typeInfo.Definition != nil {
		if vals, ok := typeInfo.Definition["values"].([]string); ok {
			values = vals
		}
	}

	if len(values) == 0 {
		return fmt.Errorf("enum type %s has no values", typeInfo.Name)
	}

	quotedValues := make([]string, len(values))
	for i, value := range values {
		quotedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
	}

	query := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)", QuoteIdentifier(typeInfo.Name), strings.Join(quotedValues, ", "))

	_, err := tx.Exec(context.Background(), query)
	return err
}

// createSequenceFromUnified creates a sequence from UnifiedModel Sequence
func createSequenceFromUnified(tx pgx.Tx, sequence unifiedmodel.Sequence) error {
	if sequence.Name == "" {
		return fmt.Errorf("sequence name cannot be empty")
	}

	query := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", QuoteIdentifier(sequence.Name))

	if sequence.Start != 0 {
		query += fmt.Sprintf(" START %d", sequence.Start)
	}
	if sequence.Increment != 0 {
		query += fmt.Sprintf(" INCREMENT %d", sequence.Increment)
	}
	if sequence.Min != nil {
		query += fmt.Sprintf(" MINVALUE %d", *sequence.Min)
	}
	if sequence.Max != nil {
		query += fmt.Sprintf(" MAXVALUE %d", *sequence.Max)
	}
	if sequence.Cycle {
		query += " CYCLE"
	} else {
		query += " NO CYCLE"
	}

	_, err := tx.Exec(context.Background(), query)
	return err
}

// createTableFromUnified creates a table from UnifiedModel Table
func createTableFromUnified(tx pgx.Tx, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	var columnDefs []string
	var primaryKeys []string

	for _, column := range table.Columns {
		columnDef := fmt.Sprintf("%s %s", QuoteIdentifier(column.Name), column.DataType)

		if !column.Nullable {
			columnDef += " NOT NULL"
		}

		if column.Default != "" {
			columnDef += fmt.Sprintf(" DEFAULT %s", column.Default)
		}

		columnDefs = append(columnDefs, columnDef)

		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, QuoteIdentifier(column.Name))
		}
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s", QuoteIdentifier(table.Name), strings.Join(columnDefs, ",\n\t"))

	if len(primaryKeys) > 0 {
		query += fmt.Sprintf(",\n\tPRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
	}

	query += "\n)"

	_, err := tx.Exec(context.Background(), query)
	if err != nil {
		return err
	}

	// Add table comment if present
	if table.Comment != "" {
		commentQuery := fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", QuoteIdentifier(table.Name), strings.ReplaceAll(table.Comment, "'", "''"))
		_, err = tx.Exec(context.Background(), commentQuery)
		if err != nil {
			return err
		}
	}

	// Add column comments if present
	for _, column := range table.Columns {
		if column.Options != nil {
			if comment, ok := column.Options["comment"].(string); ok && comment != "" {
				commentQuery := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
					QuoteIdentifier(table.Name), QuoteIdentifier(column.Name), strings.ReplaceAll(comment, "'", "''"))
				_, err = tx.Exec(context.Background(), commentQuery)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// createFunctionFromUnified creates a function from UnifiedModel Function
func createFunctionFromUnified(tx pgx.Tx, function unifiedmodel.Function) error {
	if function.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	// CockroachDB has limited function support, so this is mostly a placeholder
	query := fmt.Sprintf("CREATE OR REPLACE FUNCTION %s() RETURNS %s LANGUAGE %s AS '%s'",
		QuoteIdentifier(function.Name), function.Returns, function.Language, function.Definition)

	_, err := tx.Exec(context.Background(), query)
	return err
}

// createTriggerFromUnified creates a trigger from UnifiedModel Trigger
func createTriggerFromUnified(tx pgx.Tx, trigger unifiedmodel.Trigger) error {
	if trigger.Name == "" {
		return fmt.Errorf("trigger name cannot be empty")
	}

	// CockroachDB has limited trigger support, so this is mostly a placeholder
	query := fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE %s",
		QuoteIdentifier(trigger.Name), QuoteIdentifier(trigger.Table), trigger.Procedure)

	_, err := tx.Exec(context.Background(), query)
	return err
}

// QuoteIdentifier quotes a CockroachDB identifier
func QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

// QuoteStringSlice quotes a slice of strings for CockroachDB
func QuoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = QuoteIdentifier(s)
	}
	return quoted
}
