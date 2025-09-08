package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of a Snowflake database and returns a UnifiedModel
func DiscoverSchema(db *sql.DB) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:   dbcapabilities.Snowflake,
		Tables:         make(map[string]unifiedmodel.Table),
		Schemas:        make(map[string]unifiedmodel.Schema),
		Functions:      make(map[string]unifiedmodel.Function),
		Procedures:     make(map[string]unifiedmodel.Procedure),
		Sequences:      make(map[string]unifiedmodel.Sequence),
		Views:          make(map[string]unifiedmodel.View),
		ExternalTables: make(map[string]unifiedmodel.ExternalTable),
	}

	var err error

	// Get tables directly as unifiedmodel types
	err = discoverTablesUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Get schemas directly as unifiedmodel types
	err = discoverSchemasUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering schemas: %v", err)
	}

	// Get functions directly as unifiedmodel types
	err = discoverFunctionsUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get procedures directly as unifiedmodel types
	err = discoverProceduresUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering procedures: %v", err)
	}

	// Get sequences directly as unifiedmodel types
	err = discoverSequencesUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering sequences: %v", err)
	}

	// Get views directly as unifiedmodel types
	err = discoverViewsUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering views: %v", err)
	}

	// Get stages directly as unifiedmodel types
	err = discoverStagesUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering stages: %v", err)
	}

	// Get pipes directly as unifiedmodel types
	err = discoverPipesUnified(db, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering pipes: %v", err)
	}

	// Note: Warehouses are compute resources and don't have a direct equivalent in UnifiedModel
	// They could be stored in metadata if needed

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

	// Create schemas from UnifiedModel
	for _, schema := range um.Schemas {
		if err := createSchemaFromUnified(tx, schema); err != nil {
			return fmt.Errorf("error creating schema %s: %v", schema.Name, err)
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

	// Create views from UnifiedModel
	for _, view := range um.Views {
		if err := createViewFromUnified(tx, view); err != nil {
			return fmt.Errorf("error creating view %s: %v", view.Name, err)
		}
	}

	// Create functions from UnifiedModel
	for _, function := range um.Functions {
		if err := createFunctionFromUnified(tx, function); err != nil {
			return fmt.Errorf("error creating function %s: %v", function.Name, err)
		}
	}

	// Create procedures from UnifiedModel
	for _, procedure := range um.Procedures {
		if err := createProcedureFromUnified(tx, procedure); err != nil {
			return fmt.Errorf("error creating procedure %s: %v", procedure.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

// discoverTablesUnified discovers tables directly into UnifiedModel
func discoverTablesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			t.TABLE_SCHEMA,
			t.TABLE_NAME,
			t.TABLE_TYPE,
			COALESCE(t.COMMENT, '') as TABLE_COMMENT
		FROM INFORMATION_SCHEMA.TABLES t
		WHERE t.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
	`

	rows, err := db.Query(query)
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
		err := getTableColumnsUnified(db, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting columns for table %s.%s: %v", schema, tableName, err)
		}

		// Get constraints for this table
		err = getTableConstraintsUnified(db, schema, tableName, &table)
		if err != nil {
			return fmt.Errorf("error getting constraints for table %s.%s: %v", schema, tableName, err)
		}

		um.Tables[fmt.Sprintf("%s.%s", schema, tableName)] = table
	}

	return rows.Err()
}

// getTableColumnsUnified gets columns for a table directly into UnifiedModel
func getTableColumnsUnified(db *sql.DB, schema, tableName string, tableModel *unifiedmodel.Table) error {
	query := `
		SELECT 
			c.COLUMN_NAME,
			c.DATA_TYPE,
			CASE WHEN c.IS_NULLABLE = 'YES' THEN true ELSE false END as IS_NULLABLE,
			COALESCE(c.COLUMN_DEFAULT, '') as COLUMN_DEFAULT,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN true ELSE false END as IS_PRIMARY_KEY,
			COALESCE(c.COMMENT, '') as COLUMN_COMMENT
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN (
			SELECT kcu.COLUMN_NAME
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
				ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
				AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
			WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
				AND tc.TABLE_SCHEMA = ?
				AND tc.TABLE_NAME = ?
		) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
		WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION
	`

	rows, err := db.Query(query, schema, tableName, schema, tableName)
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
func getTableConstraintsUnified(db *sql.DB, schema, tableName string, tableModel *unifiedmodel.Table) error {
	query := `
		SELECT 
			tc.CONSTRAINT_NAME,
			tc.CONSTRAINT_TYPE,
			kcu.COLUMN_NAME,
			COALESCE(rc.UNIQUE_CONSTRAINT_NAME, '') as REFERENCED_CONSTRAINT,
			COALESCE(kcu2.TABLE_SCHEMA, '') as REFERENCED_SCHEMA,
			COALESCE(kcu2.TABLE_NAME, '') as REFERENCED_TABLE,
			COALESCE(kcu2.COLUMN_NAME, '') as REFERENCED_COLUMN
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
			ON rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME
			AND rc.UNIQUE_CONSTRAINT_SCHEMA = kcu2.CONSTRAINT_SCHEMA
		WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
		ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	constraintMap := make(map[string]*unifiedmodel.Constraint)

	for rows.Next() {
		var constraintName, constraintType, columnName, referencedConstraint, referencedSchema, referencedTable, referencedColumn string
		if err := rows.Scan(&constraintName, &constraintType, &columnName, &referencedConstraint, &referencedSchema, &referencedTable, &referencedColumn); err != nil {
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

// discoverSchemasUnified discovers schemas directly into UnifiedModel
func discoverSchemasUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			SCHEMA_NAME,
			COALESCE(COMMENT, '') as SCHEMA_COMMENT
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY SCHEMA_NAME
	`

	rows, err := db.Query(query)
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
func discoverFunctionsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			FUNCTION_NAME,
			FUNCTION_LANGUAGE,
			COALESCE(FUNCTION_DEFINITION, '') as FUNCTION_BODY,
			COALESCE(DATA_TYPE, '') as RETURN_TYPE
		FROM INFORMATION_SCHEMA.FUNCTIONS
		WHERE FUNCTION_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY FUNCTION_NAME
	`

	rows, err := db.Query(query)
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

// discoverProceduresUnified discovers procedures directly into UnifiedModel
func discoverProceduresUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			PROCEDURE_NAME,
			PROCEDURE_LANGUAGE,
			COALESCE(PROCEDURE_DEFINITION, '') as PROCEDURE_BODY
		FROM INFORMATION_SCHEMA.PROCEDURES
		WHERE PROCEDURE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY PROCEDURE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, language, body string
		if err := rows.Scan(&name, &language, &body); err != nil {
			return fmt.Errorf("error scanning procedure row: %v", err)
		}

		procedure := unifiedmodel.Procedure{
			Name:       name,
			Language:   language,
			Definition: body,
		}

		um.Procedures[name] = procedure
	}

	return rows.Err()
}

// discoverSequencesUnified discovers sequences directly into UnifiedModel
func discoverSequencesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			SEQUENCE_NAME,
			START_VALUE,
			INCREMENT,
			MINIMUM_VALUE,
			MAXIMUM_VALUE,
			CACHE_SIZE,
			CYCLE_OPTION
		FROM INFORMATION_SCHEMA.SEQUENCES
		WHERE SEQUENCE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY SEQUENCE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, cycleOption string
		var start, increment, minValue, maxValue, cacheSize int64
		if err := rows.Scan(&name, &start, &increment, &minValue, &maxValue, &cacheSize, &cycleOption); err != nil {
			return fmt.Errorf("error scanning sequence row: %v", err)
		}

		sequence := unifiedmodel.Sequence{
			Name:      name,
			Start:     start,
			Increment: increment,
			Min:       &minValue,
			Max:       &maxValue,
			Cache:     &cacheSize,
			Cycle:     cycleOption == "Y",
		}

		um.Sequences[name] = sequence
	}

	return rows.Err()
}

// discoverViewsUnified discovers views directly into UnifiedModel
func discoverViewsUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			TABLE_NAME,
			VIEW_DEFINITION
		FROM INFORMATION_SCHEMA.VIEWS
		WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY TABLE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return fmt.Errorf("error scanning view row: %v", err)
		}

		view := unifiedmodel.View{
			Name:       name,
			Definition: definition,
		}

		um.Views[name] = view
	}

	return rows.Err()
}

// discoverStagesUnified discovers stages directly into UnifiedModel as external tables
func discoverStagesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			STAGE_NAME,
			STAGE_URL,
			STAGE_TYPE,
			COALESCE(COMMENT, '') as STAGE_COMMENT
		FROM INFORMATION_SCHEMA.STAGES
		WHERE STAGE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY STAGE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying stages: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, url, stageType, comment string
		if err := rows.Scan(&name, &url, &stageType, &comment); err != nil {
			return fmt.Errorf("error scanning stage row: %v", err)
		}

		externalTable := unifiedmodel.ExternalTable{
			Name:     name,
			Location: url,
			Format:   stageType,
			Options:  map[string]any{"comment": comment},
		}

		um.ExternalTables[name] = externalTable
	}

	return rows.Err()
}

// discoverPipesUnified discovers pipes directly into UnifiedModel as functions
func discoverPipesUnified(db *sql.DB, um *unifiedmodel.UnifiedModel) error {
	query := `
		SELECT 
			PIPE_NAME,
			DEFINITION,
			COALESCE(COMMENT, '') as PIPE_COMMENT
		FROM INFORMATION_SCHEMA.PIPES
		WHERE PIPE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'ACCOUNT_USAGE')
		ORDER BY PIPE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error querying pipes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, definition, comment string
		if err := rows.Scan(&name, &definition, &comment); err != nil {
			return fmt.Errorf("error scanning pipe row: %v", err)
		}

		// Represent pipes as functions since they're data loading functions
		function := unifiedmodel.Function{
			Name:       name,
			Language:   "sql",
			Definition: definition,
			Returns:    "pipe", // Indicate this is a pipe function
			Options:    map[string]any{"comment": comment, "type": "pipe"},
		}

		um.Functions[name] = function
	}

	return rows.Err()
}

// createSchemaFromUnified creates a schema from UnifiedModel Schema
func createSchemaFromUnified(tx *sql.Tx, schema unifiedmodel.Schema) error {
	if schema.Name == "" {
		return fmt.Errorf("schema name cannot be empty")
	}

	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", QuoteIdentifier(schema.Name))
	if schema.Comment != "" {
		query += fmt.Sprintf(" COMMENT = '%s'", strings.ReplaceAll(schema.Comment, "'", "''"))
	}

	_, err := tx.Exec(query)
	return err
}

// createSequenceFromUnified creates a sequence from UnifiedModel Sequence
func createSequenceFromUnified(tx *sql.Tx, sequence unifiedmodel.Sequence) error {
	if sequence.Name == "" {
		return fmt.Errorf("sequence name cannot be empty")
	}

	query := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", QuoteIdentifier(sequence.Name))

	if sequence.Start != 0 {
		query += fmt.Sprintf(" START = %d", sequence.Start)
	}
	if sequence.Increment != 0 {
		query += fmt.Sprintf(" INCREMENT = %d", sequence.Increment)
	}

	_, err := tx.Exec(query)
	return err
}

// createTableFromUnified creates a table from UnifiedModel Table
func createTableFromUnified(tx *sql.Tx, table unifiedmodel.Table) error {
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

		if column.Options != nil {
			if comment, ok := column.Options["comment"].(string); ok && comment != "" {
				columnDef += fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(comment, "'", "''"))
			}
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

	if table.Comment != "" {
		query += fmt.Sprintf(" COMMENT = '%s'", strings.ReplaceAll(table.Comment, "'", "''"))
	}

	_, err := tx.Exec(query)
	return err
}

// createViewFromUnified creates a view from UnifiedModel View
func createViewFromUnified(tx *sql.Tx, view unifiedmodel.View) error {
	if view.Name == "" {
		return fmt.Errorf("view name cannot be empty")
	}

	query := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s AS %s", QuoteIdentifier(view.Name), view.Definition)

	_, err := tx.Exec(query)
	return err
}

// createFunctionFromUnified creates a function from UnifiedModel Function
func createFunctionFromUnified(tx *sql.Tx, function unifiedmodel.Function) error {
	if function.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	// Check if this is a pipe function
	if function.Options != nil {
		if funcType, ok := function.Options["type"].(string); ok && funcType == "pipe" {
			// Create pipe instead of function
			query := fmt.Sprintf("CREATE PIPE IF NOT EXISTS %s AS %s", QuoteIdentifier(function.Name), function.Definition)
			_, err := tx.Exec(query)
			return err
		}
	}

	// Create regular function
	query := fmt.Sprintf("CREATE FUNCTION IF NOT EXISTS %s() RETURNS %s LANGUAGE %s AS '%s'",
		QuoteIdentifier(function.Name), function.Returns, function.Language, function.Definition)

	_, err := tx.Exec(query)
	return err
}

// createProcedureFromUnified creates a procedure from UnifiedModel Procedure
func createProcedureFromUnified(tx *sql.Tx, procedure unifiedmodel.Procedure) error {
	if procedure.Name == "" {
		return fmt.Errorf("procedure name cannot be empty")
	}

	query := fmt.Sprintf("CREATE PROCEDURE IF NOT EXISTS %s() LANGUAGE %s AS '%s'",
		QuoteIdentifier(procedure.Name), procedure.Language, procedure.Definition)

	_, err := tx.Exec(query)
	return err
}

// QuoteIdentifier quotes a Snowflake identifier
func QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}
