package mssql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of a Microsoft SQL Server database and returns a UnifiedModel
func DiscoverSchema(db *sql.DB) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.SQLServer,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Triggers:     make(map[string]unifiedmodel.Trigger),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Views:        make(map[string]unifiedmodel.View),
		Sequences:    make(map[string]unifiedmodel.Sequence),
		Indexes:      make(map[string]unifiedmodel.Index),
	}

	// Get tables and their columns
	if err := discoverTablesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get schemas
	if err := discoverSchemasUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Get functions
	if err := discoverFunctionsUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Get triggers
	if err := discoverTriggersUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Get procedures
	if err := discoverProceduresUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting procedures: %v", err)
	}

	// Get views
	if err := discoverViewsUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting views: %v", err)
	}

	// Get sequences
	if err := discoverSequencesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
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

	// Create schemas first
	for _, schema := range um.Schemas {
		if err := createSchemaFromUnified(tx, schema); err != nil {
			return fmt.Errorf("error creating schema %s: %v", schema.Name, err)
		}
	}

	// Create sequences
	for _, sequence := range um.Sequences {
		if err := createSequenceFromUnified(tx, sequence); err != nil {
			return fmt.Errorf("error creating sequence %s: %v", sequence.Name, err)
		}
	}

	// Create tables (with topological sorting for dependencies)
	if err := createTablesFromUnified(tx, um.Tables); err != nil {
		return fmt.Errorf("error creating tables: %v", err)
	}

	// Create views
	for _, view := range um.Views {
		if err := createViewFromUnified(tx, view); err != nil {
			return fmt.Errorf("error creating view %s: %v", view.Name, err)
		}
	}

	// Create functions
	for _, function := range um.Functions {
		if err := createFunctionFromUnified(tx, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	// Create procedures
	for _, procedure := range um.Procedures {
		if err := createProcedureFromUnified(tx, procedure); err != nil {
			return fmt.Errorf("error creating procedure %s: %v", procedure.Name, err)
		}
	}

	// Create triggers
	for _, trigger := range um.Triggers {
		if err := createTriggerFromUnified(tx, trigger); err != nil {
			return fmt.Errorf("error creating trigger %s: %v", trigger.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// discoverTablesUnified discovers MSSQL tables directly into UnifiedModel
func discoverTablesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.name AS schema_name,
			t.name AS table_name,
			c.name AS column_name,
			ty.name AS data_type,
			c.max_length,
			c.precision,
			c.scale,
			c.is_nullable,
			c.is_identity,
			ISNULL(dc.definition, '') AS default_value,
			CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		INNER JOIN sys.columns c ON t.object_id = c.object_id
		INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
		LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
		LEFT JOIN (
			SELECT 
				kcu.table_schema,
				kcu.table_name,
				kcu.column_name
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
				ON kcu.constraint_name = tc.constraint_name 
				AND kcu.table_schema = tc.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
		) pk ON s.name = pk.table_schema AND t.name = pk.table_name AND c.name = pk.column_name
		ORDER BY s.name, t.name, c.column_id
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*unifiedmodel.Table)

	for rows.Next() {
		var schemaName, tableName, columnName, dataType, defaultValue string
		var maxLength, precision, scale int
		var isNullable, isIdentity, isPrimaryKey bool

		err := rows.Scan(&schemaName, &tableName, &columnName, &dataType,
			&maxLength, &precision, &scale, &isNullable, &isIdentity,
			&defaultValue, &isPrimaryKey)
		if err != nil {
			return fmt.Errorf("error scanning table row: %v", err)
		}

		fullTableName := schemaName + "." + tableName

		// Create table if it doesn't exist
		if tables[fullTableName] == nil {
			tables[fullTableName] = &unifiedmodel.Table{
				Name:        tableName,
				Columns:     make(map[string]unifiedmodel.Column),
				Indexes:     make(map[string]unifiedmodel.Index),
				Constraints: make(map[string]unifiedmodel.Constraint),
				Options: map[string]interface{}{
					"schema": schemaName,
				},
			}
		}

		// Add column to table
		column := unifiedmodel.Column{
			Name:         columnName,
			DataType:     dataType,
			Nullable:     isNullable,
			Default:      defaultValue,
			IsPrimaryKey: isPrimaryKey,
			Options: map[string]interface{}{
				"max_length":  maxLength,
				"precision":   precision,
				"scale":       scale,
				"is_identity": isIdentity,
			},
		}

		tables[fullTableName].Columns[columnName] = column
	}

	// Add tables to unified model
	for _, table := range tables {
		um.Tables[table.Name] = *table
	}

	return nil
}

// discoverSchemasUnified discovers MSSQL schemas directly into UnifiedModel
func discoverSchemasUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			name,
			ISNULL(CAST(value AS NVARCHAR(MAX)), '') AS description
		FROM sys.schemas s
		LEFT JOIN sys.extended_properties ep ON s.schema_id = ep.major_id 
			AND ep.minor_id = 0 AND ep.name = 'MS_Description'
		WHERE s.name NOT IN ('sys', 'information_schema', 'guest', 'INFORMATION_SCHEMA')
		ORDER BY name
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, description string
		if err := rows.Scan(&name, &description); err != nil {
			return fmt.Errorf("error scanning schema row: %v", err)
		}

		um.Schemas[name] = unifiedmodel.Schema{
			Name:    name,
			Comment: description,
		}
	}

	return nil
}

// discoverFunctionsUnified discovers MSSQL functions directly into UnifiedModel
func discoverFunctionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.name AS schema_name,
			o.name AS function_name,
			m.definition AS function_body,
			CASE o.type 
				WHEN 'FN' THEN 'scalar'
				WHEN 'IF' THEN 'table'
				WHEN 'TF' THEN 'table'
				ELSE 'unknown'
			END AS function_type
		FROM sys.objects o
		INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
		INNER JOIN sys.sql_modules m ON o.object_id = m.object_id
		WHERE o.type IN ('FN', 'IF', 'TF')
		ORDER BY s.name, o.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, functionName, functionBody, functionType string
		if err := rows.Scan(&schemaName, &functionName, &functionBody, &functionType); err != nil {
			return fmt.Errorf("error scanning function row: %v", err)
		}

		um.Functions[functionName] = unifiedmodel.Function{
			Name:       functionName,
			Language:   "tsql",
			Definition: functionBody,
			Options: map[string]interface{}{
				"schema": schemaName,
				"type":   functionType,
			},
		}
	}

	return nil
}

// discoverTriggersUnified discovers MSSQL triggers directly into UnifiedModel
func discoverTriggersUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.name AS schema_name,
			tr.name AS trigger_name,
			t.name AS table_name,
			m.definition AS trigger_body,
			CASE 
				WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS trigger_timing
		FROM sys.triggers tr
		INNER JOIN sys.tables t ON tr.parent_id = t.object_id
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		INNER JOIN sys.sql_modules m ON tr.object_id = m.object_id
		WHERE tr.is_ms_shipped = 0
		ORDER BY s.name, t.name, tr.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, triggerName, tableName, triggerBody, triggerTiming string
		if err := rows.Scan(&schemaName, &triggerName, &tableName, &triggerBody, &triggerTiming); err != nil {
			return fmt.Errorf("error scanning trigger row: %v", err)
		}

		um.Triggers[triggerName] = unifiedmodel.Trigger{
			Name:  triggerName,
			Table: tableName,
			Options: map[string]interface{}{
				"schema":     schemaName,
				"timing":     triggerTiming,
				"definition": triggerBody,
			},
		}
	}

	return nil
}

// discoverProceduresUnified discovers MSSQL procedures directly into UnifiedModel
func discoverProceduresUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.name AS schema_name,
			p.name AS procedure_name,
			m.definition AS procedure_body
		FROM sys.procedures p
		INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
		INNER JOIN sys.sql_modules m ON p.object_id = m.object_id
		WHERE p.is_ms_shipped = 0
		ORDER BY s.name, p.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, procedureName, procedureBody string
		if err := rows.Scan(&schemaName, &procedureName, &procedureBody); err != nil {
			return fmt.Errorf("error scanning procedure row: %v", err)
		}

		um.Procedures[procedureName] = unifiedmodel.Procedure{
			Name:       procedureName,
			Language:   "tsql",
			Definition: procedureBody,
			Options: map[string]interface{}{
				"schema": schemaName,
			},
		}
	}

	return nil
}

// discoverViewsUnified discovers MSSQL views directly into UnifiedModel
func discoverViewsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.name AS schema_name,
			v.name AS view_name,
			m.definition AS view_definition
		FROM sys.views v
		INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
		INNER JOIN sys.sql_modules m ON v.object_id = m.object_id
		WHERE v.is_ms_shipped = 0
		ORDER BY s.name, v.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, viewName, viewDefinition string
		if err := rows.Scan(&schemaName, &viewName, &viewDefinition); err != nil {
			return fmt.Errorf("error scanning view row: %v", err)
		}

		um.Views[viewName] = unifiedmodel.View{
			Name:       viewName,
			Definition: viewDefinition,
			Options: map[string]interface{}{
				"schema": schemaName,
			},
		}
	}

	return nil
}

// discoverSequencesUnified discovers MSSQL sequences directly into UnifiedModel
func discoverSequencesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.name AS schema_name,
			seq.name AS sequence_name,
			seq.start_value,
			seq.increment,
			seq.minimum_value,
			seq.maximum_value
		FROM sys.sequences seq
		INNER JOIN sys.schemas s ON seq.schema_id = s.schema_id
		ORDER BY s.name, seq.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, sequenceName string
		var startValue, increment, minValue, maxValue int64
		if err := rows.Scan(&schemaName, &sequenceName, &startValue, &increment, &minValue, &maxValue); err != nil {
			return fmt.Errorf("error scanning sequence row: %v", err)
		}

		um.Sequences[sequenceName] = unifiedmodel.Sequence{
			Name:      sequenceName,
			Start:     startValue,
			Increment: increment,
			Options: map[string]interface{}{
				"schema":    schemaName,
				"min_value": minValue,
				"max_value": maxValue,
			},
		}
	}

	return nil
}

// Helper function to quote SQL Server identifiers
func QuoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// createSchemaFromUnified creates a SQL Server schema from UnifiedModel Schema
func createSchemaFromUnified(tx *sql.Tx, schema unifiedmodel.Schema) error {
	if schema.Name == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	query := fmt.Sprintf("CREATE SCHEMA %s", QuoteIdentifier(schema.Name))
	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating schema: %v", err)
	}

	return nil
}

// createSequenceFromUnified creates a SQL Server sequence from UnifiedModel Sequence
func createSequenceFromUnified(tx *sql.Tx, sequence unifiedmodel.Sequence) error {
	if sequence.Name == "" {
		return fmt.Errorf("sequence name cannot be empty")
	}

	schemaName := "dbo"
	if sequence.Options != nil {
		if schema, ok := sequence.Options["schema"].(string); ok && schema != "" {
			schemaName = schema
		}
	}

	query := fmt.Sprintf("CREATE SEQUENCE %s.%s START WITH %d INCREMENT BY %d",
		QuoteIdentifier(schemaName), QuoteIdentifier(sequence.Name),
		sequence.Start, sequence.Increment)

	// Add min/max values if specified in options
	if sequence.Options != nil {
		if minVal, ok := sequence.Options["min_value"].(int64); ok {
			query += fmt.Sprintf(" MINVALUE %d", minVal)
		}
		if maxVal, ok := sequence.Options["max_value"].(int64); ok {
			query += fmt.Sprintf(" MAXVALUE %d", maxVal)
		}
	}

	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating sequence: %v", err)
	}

	return nil
}

// createTablesFromUnified creates SQL Server tables from UnifiedModel Tables with dependency sorting
func createTablesFromUnified(tx *sql.Tx, tables map[string]unifiedmodel.Table) error {
	// Simple table creation without dependency sorting for now
	// In a full implementation, you would implement topological sorting
	for _, table := range tables {
		if err := createTableFromUnified(tx, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Add constraints after all tables are created
	for _, table := range tables {
		if err := addTableConstraintsFromUnified(tx, table); err != nil {
			return fmt.Errorf("error adding constraints to table %s: %v", table.Name, err)
		}
	}

	return nil
}

// createTableFromUnified creates a SQL Server table from UnifiedModel Table
func createTableFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	schemaName := "dbo"
	if table.Options != nil {
		if schema, ok := table.Options["schema"].(string); ok && schema != "" {
			schemaName = schema
		}
	}

	var columns []string
	for _, column := range table.Columns {
		columnDef := QuoteIdentifier(column.Name) + " " + column.DataType

		if !column.Nullable {
			columnDef += " NOT NULL"
		}

		if column.Default != "" {
			columnDef += " DEFAULT " + column.Default
		}

		// Handle identity columns
		if column.Options != nil {
			if isIdentity, ok := column.Options["is_identity"].(bool); ok && isIdentity {
				columnDef += " IDENTITY(1,1)"
			}
		}

		columns = append(columns, columnDef)
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (%s)",
		QuoteIdentifier(schemaName), QuoteIdentifier(table.Name),
		strings.Join(columns, ", "))

	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	return nil
}

// addTableConstraintsFromUnified adds constraints to a SQL Server table from UnifiedModel Table
func addTableConstraintsFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	schemaName := "dbo"
	if table.Options != nil {
		if schema, ok := table.Options["schema"].(string); ok && schema != "" {
			schemaName = schema
		}
	}

	// Add primary key constraint
	var pkColumns []string
	for _, column := range table.Columns {
		if column.IsPrimaryKey {
			pkColumns = append(pkColumns, QuoteIdentifier(column.Name))
		}
	}

	if len(pkColumns) > 0 {
		pkName := fmt.Sprintf("PK_%s", table.Name)
		query := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s PRIMARY KEY (%s)",
			QuoteIdentifier(schemaName), QuoteIdentifier(table.Name),
			QuoteIdentifier(pkName), strings.Join(pkColumns, ", "))

		_, err := tx.Exec(query)
		if err != nil {
			return fmt.Errorf("error adding primary key: %v", err)
		}
	}

	// Add other constraints
	for _, constraint := range table.Constraints {
		if err := createConstraintFromUnified(tx, schemaName, table.Name, constraint); err != nil {
			return fmt.Errorf("error creating constraint %s: %v", constraint.Name, err)
		}
	}

	return nil
}

// createConstraintFromUnified creates a SQL Server constraint from UnifiedModel Constraint
func createConstraintFromUnified(tx *sql.Tx, schemaName, tableName string, constraint unifiedmodel.Constraint) error {
	var query string

	switch constraint.Type {
	case unifiedmodel.ConstraintTypeForeignKey:
		if len(constraint.Columns) > 0 && constraint.Reference.Table != "" && len(constraint.Reference.Columns) > 0 {
			query = fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				QuoteIdentifier(schemaName), QuoteIdentifier(tableName),
				QuoteIdentifier(constraint.Name),
				QuoteIdentifier(constraint.Columns[0]),
				QuoteIdentifier(constraint.Reference.Table),
				QuoteIdentifier(constraint.Reference.Columns[0]))
		}
	case unifiedmodel.ConstraintTypeUnique:
		query = fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s)",
			QuoteIdentifier(schemaName), QuoteIdentifier(tableName),
			QuoteIdentifier(constraint.Name),
			strings.Join(QuoteStringSlice(constraint.Columns), ", "))
	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Options != nil {
			if definition, ok := constraint.Options["definition"].(string); ok && definition != "" {
				query = fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s)",
					QuoteIdentifier(schemaName), QuoteIdentifier(tableName),
					QuoteIdentifier(constraint.Name), definition)
			}
		}
	}

	if query != "" {
		_, err := tx.Exec(query)
		if err != nil {
			return fmt.Errorf("error creating constraint: %v", err)
		}
	}

	return nil
}

// createViewFromUnified creates a SQL Server view from UnifiedModel View
func createViewFromUnified(tx *sql.Tx, view unifiedmodel.View) error {
	if view.Name == "" || view.Definition == "" {
		return fmt.Errorf("view name and definition cannot be empty")
	}

	schemaName := "dbo"
	if view.Options != nil {
		if schema, ok := view.Options["schema"].(string); ok && schema != "" {
			schemaName = schema
		}
	}

	query := fmt.Sprintf("CREATE VIEW %s.%s AS %s",
		QuoteIdentifier(schemaName), QuoteIdentifier(view.Name), view.Definition)

	_, err := tx.Exec(query)
	if err != nil {
		return fmt.Errorf("error creating view: %v", err)
	}

	return nil
}

// createFunctionFromUnified creates a SQL Server function from UnifiedModel Function
func createFunctionFromUnified(tx *sql.Tx, function unifiedmodel.Function) error {
	if function.Name == "" || function.Definition == "" {
		return fmt.Errorf("function name and definition cannot be empty")
	}

	// SQL Server functions require the full CREATE FUNCTION statement in the definition
	_, err := tx.Exec(function.Definition)
	if err != nil {
		return fmt.Errorf("error creating function: %v", err)
	}

	return nil
}

// createProcedureFromUnified creates a SQL Server procedure from UnifiedModel Procedure
func createProcedureFromUnified(tx *sql.Tx, procedure unifiedmodel.Procedure) error {
	if procedure.Name == "" || procedure.Definition == "" {
		return fmt.Errorf("procedure name and definition cannot be empty")
	}

	// SQL Server procedures require the full CREATE PROCEDURE statement in the definition
	_, err := tx.Exec(procedure.Definition)
	if err != nil {
		return fmt.Errorf("error creating procedure: %v", err)
	}

	return nil
}

// createTriggerFromUnified creates a SQL Server trigger from UnifiedModel Trigger
func createTriggerFromUnified(tx *sql.Tx, trigger unifiedmodel.Trigger) error {
	if trigger.Name == "" || trigger.Table == "" {
		return fmt.Errorf("trigger name and table cannot be empty")
	}

	// Get trigger definition from options
	var definition string
	if trigger.Options != nil {
		if def, ok := trigger.Options["definition"].(string); ok {
			definition = def
		}
	}

	if definition == "" {
		return fmt.Errorf("trigger definition cannot be empty")
	}

	// SQL Server triggers require the full CREATE TRIGGER statement in the definition
	_, err := tx.Exec(definition)
	if err != nil {
		return fmt.Errorf("error creating trigger: %v", err)
	}

	return nil
}

// QuoteStringSlice quotes each string in a slice
func QuoteStringSlice(slice []string) []string {
	quoted := make([]string, len(slice))
	for i, s := range slice {
		quoted[i] = QuoteIdentifier(s)
	}
	return quoted
}
