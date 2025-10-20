//go:build enterprise
// +build enterprise

package db2

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of an IBM Db2 database and returns a UnifiedModel
func DiscoverSchema(db *sql.DB) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.DB2,
		Tables:       make(map[string]unifiedmodel.Table),
		Schemas:      make(map[string]unifiedmodel.Schema),
		Functions:    make(map[string]unifiedmodel.Function),
		Procedures:   make(map[string]unifiedmodel.Procedure),
		Triggers:     make(map[string]unifiedmodel.Trigger),
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

	// Get sequences
	if err := discoverSequencesUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get procedures
	if err := discoverProceduresUnified(db, um); err != nil {
		return nil, fmt.Errorf("error getting procedures: %v", err)
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

// discoverTablesUnified discovers DB2 tables directly into UnifiedModel
func discoverTablesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.TABSCHEMA,
			t.TABNAME,
			c.COLNAME,
			c.TYPENAME,
			c.LENGTH,
			c.SCALE,
			CASE WHEN c.NULLS = 'Y' THEN 1 ELSE 0 END AS IS_NULLABLE,
			COALESCE(c.DEFAULT, '') AS DEFAULT_VALUE,
			CASE WHEN pk.COLNAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY,
			COALESCE(c.IDENTITY, 'N') AS IS_IDENTITY,
			c.COLNO
		FROM SYSCAT.TABLES t
		INNER JOIN SYSCAT.COLUMNS c ON t.TABSCHEMA = c.TABSCHEMA AND t.TABNAME = c.TABNAME
		LEFT JOIN (
			SELECT DISTINCT kc.TABSCHEMA, kc.TABNAME, kc.COLNAME
			FROM SYSCAT.KEYCOLUSE kc
			INNER JOIN SYSCAT.TABCONST tc ON kc.TABSCHEMA = tc.TABSCHEMA 
				AND kc.TABNAME = tc.TABNAME 
				AND kc.CONSTNAME = tc.CONSTNAME
			WHERE tc.TYPE = 'P'
		) pk ON t.TABSCHEMA = pk.TABSCHEMA AND t.TABNAME = pk.TABNAME AND c.COLNAME = pk.COLNAME
		WHERE t.TYPE = 'T' AND t.TABSCHEMA NOT LIKE 'SYS%'
		ORDER BY t.TABSCHEMA, t.TABNAME, c.COLNO
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*unifiedmodel.Table)

	for rows.Next() {
		var schemaName, tableName, columnName, dataType, defaultValue, isIdentity string
		var length, scale, colNo int
		var isNullable, isPrimaryKey bool

		err := rows.Scan(&schemaName, &tableName, &columnName, &dataType,
			&length, &scale, &isNullable, &defaultValue, &isPrimaryKey, &isIdentity, &colNo)
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
				"length":      length,
				"scale":       scale,
				"column_no":   colNo,
				"is_identity": isIdentity == "Y",
			},
		}

		tables[fullTableName].Columns[columnName] = column
	}

	// Discover indexes and constraints for each table
	for _, table := range tables {
		schemaName := table.Options["schema"].(string)
		if err := discoverTableIndexesUnified(db, schemaName, table.Name, table); err != nil {
			return fmt.Errorf("error discovering indexes for table %s.%s: %v", schemaName, table.Name, err)
		}
		if err := discoverTableConstraintsUnified(db, schemaName, table.Name, table); err != nil {
			return fmt.Errorf("error discovering constraints for table %s.%s: %v", schemaName, table.Name, err)
		}
	}

	// Add tables to unified model
	for _, table := range tables {
		um.Tables[table.Name] = *table
	}

	return nil
}

// discoverTableIndexesUnified discovers DB2 indexes for a specific table
func discoverTableIndexesUnified(db *sql.DB, schema, tableName string, table *unifiedmodel.Table) error {
	query := `
		SELECT 
			i.INDNAME,
			i.UNIQUERULE,
			ic.COLNAME,
			ic.COLSEQ
		FROM SYSCAT.INDEXES i
		INNER JOIN SYSCAT.INDEXCOLUSE ic ON i.INDSCHEMA = ic.INDSCHEMA AND i.INDNAME = ic.INDNAME
		WHERE i.TABSCHEMA = ? AND i.TABNAME = ?
		ORDER BY i.INDNAME, ic.COLSEQ
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	indexes := make(map[string]*unifiedmodel.Index)

	for rows.Next() {
		var indexName, uniqueRule, columnName string
		var colSeq int

		if err := rows.Scan(&indexName, &uniqueRule, &columnName, &colSeq); err != nil {
			return fmt.Errorf("error scanning index row: %v", err)
		}

		if indexes[indexName] == nil {
			indexes[indexName] = &unifiedmodel.Index{
				Name:    indexName,
				Columns: []string{},
				Unique:  uniqueRule == "U" || uniqueRule == "P",
			}
		}

		indexes[indexName].Columns = append(indexes[indexName].Columns, columnName)
	}

	// Add indexes to table
	for _, index := range indexes {
		table.Indexes[index.Name] = *index
	}

	return nil
}

// discoverTableConstraintsUnified discovers DB2 constraints for a specific table
func discoverTableConstraintsUnified(db *sql.DB, schema, tableName string, table *unifiedmodel.Table) error {
	query := `
		SELECT 
			tc.CONSTNAME,
			tc.TYPE,
			kc.COLNAME,
			tc.CHECKEXISTINGDATA,
			COALESCE(r.REFTABSCHEMA, '') AS REF_SCHEMA,
			COALESCE(r.REFTABNAME, '') AS REF_TABLE,
			COALESCE(rkc.COLNAME, '') AS REF_COLUMN
		FROM SYSCAT.TABCONST tc
		LEFT JOIN SYSCAT.KEYCOLUSE kc ON tc.TABSCHEMA = kc.TABSCHEMA 
			AND tc.TABNAME = kc.TABNAME 
			AND tc.CONSTNAME = kc.CONSTNAME
		LEFT JOIN SYSCAT.REFERENCES r ON tc.TABSCHEMA = r.TABSCHEMA 
			AND tc.TABNAME = r.TABNAME 
			AND tc.CONSTNAME = r.CONSTNAME
		LEFT JOIN SYSCAT.KEYCOLUSE rkc ON r.REFTABSCHEMA = rkc.TABSCHEMA 
			AND r.REFTABNAME = rkc.TABNAME 
			AND r.REFKEYNAME = rkc.CONSTNAME
		WHERE tc.TABSCHEMA = ? AND tc.TABNAME = ?
		ORDER BY tc.CONSTNAME, kc.COLSEQ
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	constraints := make(map[string]*unifiedmodel.Constraint)

	for rows.Next() {
		var constraintName, constraintType, columnName, checkExisting, refSchema, refTable, refColumn string

		if err := rows.Scan(&constraintName, &constraintType, &columnName, &checkExisting, &refSchema, &refTable, &refColumn); err != nil {
			return fmt.Errorf("error scanning constraint row: %v", err)
		}

		if constraints[constraintName] == nil {
			var cType unifiedmodel.ConstraintType
			switch constraintType {
			case "P":
				cType = unifiedmodel.ConstraintTypePrimaryKey
			case "F":
				cType = unifiedmodel.ConstraintTypeForeignKey
			case "U":
				cType = unifiedmodel.ConstraintTypeUnique
			case "K":
				cType = unifiedmodel.ConstraintTypeCheck
			default:
				cType = unifiedmodel.ConstraintTypeCheck
			}

			constraints[constraintName] = &unifiedmodel.Constraint{
				Name:    constraintName,
				Type:    cType,
				Columns: []string{},
			}

			// Set foreign key reference if applicable
			if constraintType == "F" && refTable != "" {
				constraints[constraintName].Reference = unifiedmodel.Reference{
					Table:   refTable,
					Columns: []string{refColumn},
				}
			}
		}

		if columnName != "" {
			constraints[constraintName].Columns = append(constraints[constraintName].Columns, columnName)
		}
	}

	// Add constraints to table
	for _, constraint := range constraints {
		table.Constraints[constraint.Name] = *constraint
	}

	return nil
}

// discoverSchemasUnified discovers DB2 schemas directly into UnifiedModel
func discoverSchemasUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			SCHEMANAME,
			COALESCE(REMARKS, '') AS DESCRIPTION
		FROM SYSCAT.SCHEMATA
		WHERE SCHEMANAME NOT LIKE 'SYS%' 
			AND SCHEMANAME NOT IN ('NULLID', 'SQLJ', 'DB2INST1')
		ORDER BY SCHEMANAME
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

// discoverFunctionsUnified discovers DB2 functions directly into UnifiedModel
func discoverFunctionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			f.FUNCSCHEMA,
			f.FUNCNAME,
			f.LANGUAGE,
			COALESCE(f.BODY, f.TEXT, '') AS FUNCTION_BODY,
			f.RETURN_TYPE
		FROM SYSCAT.FUNCTIONS f
		WHERE f.FUNCSCHEMA NOT LIKE 'SYS%'
			AND f.ORIGIN = 'U'
		ORDER BY f.FUNCSCHEMA, f.FUNCNAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, functionName, language, functionBody, returnType string
		if err := rows.Scan(&schemaName, &functionName, &language, &functionBody, &returnType); err != nil {
			return fmt.Errorf("error scanning function row: %v", err)
		}

		um.Functions[functionName] = unifiedmodel.Function{
			Name:       functionName,
			Language:   strings.ToLower(language),
			Definition: functionBody,
			Options: map[string]interface{}{
				"schema":      schemaName,
				"return_type": returnType,
			},
		}
	}

	return nil
}

// discoverTriggersUnified discovers DB2 triggers directly into UnifiedModel
func discoverTriggersUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.TRIGSCHEMA,
			t.TRIGNAME,
			t.TABSCHEMA,
			t.TABNAME,
			t.TRIGEVENT,
			t.TRIGTIME,
			COALESCE(t.TEXT, '') AS TRIGGER_BODY
		FROM SYSCAT.TRIGGERS t
		WHERE t.TRIGSCHEMA NOT LIKE 'SYS%'
		ORDER BY t.TRIGSCHEMA, t.TRIGNAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, triggerName, tableSchema, tableName, trigEvent, trigTime, triggerBody string
		if err := rows.Scan(&schemaName, &triggerName, &tableSchema, &tableName, &trigEvent, &trigTime, &triggerBody); err != nil {
			return fmt.Errorf("error scanning trigger row: %v", err)
		}

		um.Triggers[triggerName] = unifiedmodel.Trigger{
			Name:  triggerName,
			Table: tableName,
			Options: map[string]interface{}{
				"schema":       schemaName,
				"table_schema": tableSchema,
				"event":        trigEvent,
				"timing":       trigTime,
				"definition":   triggerBody,
			},
		}
	}

	return nil
}

// discoverSequencesUnified discovers DB2 sequences directly into UnifiedModel
func discoverSequencesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			s.SEQSCHEMA,
			s.SEQNAME,
			s.START,
			s.INCREMENT,
			s.MINVALUE,
			s.MAXVALUE
		FROM SYSCAT.SEQUENCES s
		WHERE s.SEQSCHEMA NOT LIKE 'SYS%'
		ORDER BY s.SEQSCHEMA, s.SEQNAME
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

// discoverProceduresUnified discovers DB2 procedures directly into UnifiedModel
func discoverProceduresUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			p.PROCSCHEMA,
			p.PROCNAME,
			p.LANGUAGE,
			COALESCE(p.TEXT, '') AS PROCEDURE_BODY
		FROM SYSCAT.PROCEDURES p
		WHERE p.PROCSCHEMA NOT LIKE 'SYS%'
			AND p.ORIGIN = 'U'
		ORDER BY p.PROCSCHEMA, p.PROCNAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, procedureName, language, procedureBody string
		if err := rows.Scan(&schemaName, &procedureName, &language, &procedureBody); err != nil {
			return fmt.Errorf("error scanning procedure row: %v", err)
		}

		um.Procedures[procedureName] = unifiedmodel.Procedure{
			Name:       procedureName,
			Language:   strings.ToLower(language),
			Definition: procedureBody,
			Options: map[string]interface{}{
				"schema": schemaName,
			},
		}
	}

	return nil
}

// Helper function to quote DB2 identifiers
func QuoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// createSchemaFromUnified creates a DB2 schema from UnifiedModel Schema
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

// createSequenceFromUnified creates a DB2 sequence from UnifiedModel Sequence
func createSequenceFromUnified(tx *sql.Tx, sequence unifiedmodel.Sequence) error {
	if sequence.Name == "" {
		return fmt.Errorf("sequence name cannot be empty")
	}

	schemaName := "DB2INST1"
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

// createTablesFromUnified creates DB2 tables from UnifiedModel Tables with dependency sorting
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

// createTableFromUnified creates a DB2 table from UnifiedModel Table
func createTableFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	if table.Name == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	schemaName := "DB2INST1"
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
				columnDef += " GENERATED ALWAYS AS IDENTITY"
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

// addTableConstraintsFromUnified adds constraints to a DB2 table from UnifiedModel Table
func addTableConstraintsFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
	schemaName := "DB2INST1"
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

// createConstraintFromUnified creates a DB2 constraint from UnifiedModel Constraint
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

// createFunctionFromUnified creates a DB2 function from UnifiedModel Function
func createFunctionFromUnified(tx *sql.Tx, function unifiedmodel.Function) error {
	if function.Name == "" || function.Definition == "" {
		return fmt.Errorf("function name and definition cannot be empty")
	}

	// DB2 functions require the full CREATE FUNCTION statement in the definition
	_, err := tx.Exec(function.Definition)
	if err != nil {
		return fmt.Errorf("error creating function: %v", err)
	}

	return nil
}

// createProcedureFromUnified creates a DB2 procedure from UnifiedModel Procedure
func createProcedureFromUnified(tx *sql.Tx, procedure unifiedmodel.Procedure) error {
	if procedure.Name == "" || procedure.Definition == "" {
		return fmt.Errorf("procedure name and definition cannot be empty")
	}

	// DB2 procedures require the full CREATE PROCEDURE statement in the definition
	_, err := tx.Exec(procedure.Definition)
	if err != nil {
		return fmt.Errorf("error creating procedure: %v", err)
	}

	return nil
}

// createTriggerFromUnified creates a DB2 trigger from UnifiedModel Trigger
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

	// DB2 triggers require the full CREATE TRIGGER statement in the definition
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
