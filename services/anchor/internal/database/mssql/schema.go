package mssql

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

	var err error

	// Get tables and their columns
	tablesMap, _, err := discoverTablesAndColumns(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Convert tables to unified model
	for _, table := range tablesMap {
		unifiedTable := ConvertMSSQLTable(*table)
		um.Tables[table.Name] = unifiedTable
	}

	// Get schemas
	schemas, err := getSchemas(db)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Convert schemas to unified model
	for _, schema := range schemas {
		um.Schemas[schema.Name] = unifiedmodel.Schema{
			Name:    schema.Name,
			Comment: schema.Description,
		}
	}

	// Get functions
	functions, err := getFunctions(db)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Convert functions to unified model
	for _, function := range functions {
		um.Functions[function.Name] = unifiedmodel.Function{
			Name:       function.Name,
			Language:   "sql", // MSSQL functions are SQL-based
			Definition: function.Body,
			Returns:    function.ReturnType,
		}
	}

	// Get triggers
	triggers, err := getTriggers(db)
	if err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Convert triggers to unified model
	for _, trigger := range triggers {
		um.Triggers[trigger.Name] = unifiedmodel.Trigger{
			Name:      trigger.Name,
			Table:     trigger.Table,
			Procedure: trigger.Statement, // Store trigger statement as procedure
		}
	}

	// Get procedures
	procedures, err := getProcedures(db)
	if err != nil {
		return nil, fmt.Errorf("error getting procedures: %v", err)
	}

	// Convert procedures to unified model
	for _, procedure := range procedures {
		um.Procedures[procedure.Name] = unifiedmodel.Procedure{
			Name:       procedure.Name,
			Language:   "sql", // MSSQL procedures are SQL-based
			Definition: procedure.Body,
		}
	}

	// Get views
	views, err := getViews(db)
	if err != nil {
		return nil, fmt.Errorf("error getting views: %v", err)
	}

	// Convert views to unified model
	for _, view := range views {
		um.Views[view.Name] = unifiedmodel.View{
			Name:       view.Name,
			Definition: view.Definition,
		}
	}

	// Get sequences
	sequences, err := getSequences(db)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Convert sequences to unified model
	for _, sequence := range sequences {
		um.Sequences[sequence.Name] = unifiedmodel.Sequence{
			Name:      sequence.Name,
			Start:     sequence.Start,
			Increment: sequence.Increment,
		}
	}

	return um, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db *sql.DB, params common.StructureParams) error {
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Sort tables based on dependencies
	sortedTables, err := common.TopologicalSort(params.Tables)
	if err != nil {
		return fmt.Errorf("error sorting tables: %v", err)
	}

	// Create tables
	for _, table := range sortedTables {
		if err := CreateTable(tx, table); err != nil {
			return fmt.Errorf("error creating table %s: %v", table.Name, err)
		}
	}

	// Add table constraints
	for _, table := range sortedTables {
		if err := AddTableConstraints(tx, table); err != nil {
			return fmt.Errorf("error adding constraints to table %s: %v", table.Name, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func discoverTablesAndColumns(db *sql.DB) (map[string]*common.TableInfo, []string, error) {
	query := `
		SELECT 
			s.name AS schema_name,
			t.name AS table_name,
			c.name AS column_name,
			ty.name AS data_type,
			c.is_nullable,
			c.column_default,
			CASE WHEN c.is_identity = 1 THEN 1 ELSE 0 END AS is_identity,
			CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key,
			CASE WHEN ty.name = 'nvarchar' OR ty.name = 'varchar' THEN c.max_length ELSE NULL END AS varchar_length,
			CASE WHEN idx.is_unique = 1 THEN 1 ELSE 0 END AS is_unique
		FROM sys.tables t
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		JOIN sys.columns c ON t.object_id = c.object_id
		JOIN sys.types ty ON c.user_type_id = ty.user_type_id
		LEFT JOIN (
			SELECT ic.object_id, ic.column_id
			FROM sys.index_columns ic
			JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
			WHERE i.is_primary_key = 1
		) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
		LEFT JOIN (
			SELECT ic.object_id, ic.column_id, i.is_unique
			FROM sys.index_columns ic
			JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
			WHERE i.is_unique = 1 AND i.is_primary_key = 0
		) idx ON c.object_id = idx.object_id AND c.column_id = idx.column_id
		WHERE t.type = 'U'
		ORDER BY t.name, c.column_id
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching table and column information: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*common.TableInfo)
	tableNames := make([]string, 0)
	for rows.Next() {
		var schemaName, tableName, columnName, dataType string
		var isNullable, isIdentity, isPrimaryKey, isUnique bool
		var columnDefault sql.NullString
		var varcharLength sql.NullInt32

		if err := rows.Scan(
			&schemaName, &tableName, &columnName, &dataType, &isNullable, &columnDefault,
			&isIdentity, &isPrimaryKey, &varcharLength, &isUnique,
		); err != nil {
			return nil, nil, fmt.Errorf("error scanning table and column row: %v", err)
		}

		fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
		if _, exists := tables[fullTableName]; !exists {
			tables[fullTableName] = &common.TableInfo{
				Name:      tableName,
				Schema:    schemaName,
				TableType: "mssql.standard",
			}
			tableNames = append(tableNames, fullTableName)
		}

		var defaultValue *string
		if columnDefault.Valid {
			defaultValue = &columnDefault.String
		}

		columnInfo := common.ColumnInfo{
			Name:            columnName,
			DataType:        dataType,
			IsNullable:      isNullable,
			ColumnDefault:   defaultValue,
			IsPrimaryKey:    isPrimaryKey,
			IsAutoIncrement: isIdentity,
			IsUnique:        isUnique,
		}

		if (dataType == "nvarchar" || dataType == "varchar") && varcharLength.Valid {
			length := int(varcharLength.Int32)
			if dataType == "nvarchar" {
				length = length / 2 // nvarchar length is in bytes, divide by 2 to get character count
			}
			columnInfo.VarcharLength = &length
		}

		tables[fullTableName].Columns = append(tables[fullTableName].Columns, columnInfo)
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func getSchemas(db *sql.DB) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT 
			name AS schema_name,
			ISNULL(CAST(extended_property AS NVARCHAR(MAX)), '') AS description
		FROM sys.schemas s
		LEFT JOIN (
			SELECT major_id, value AS extended_property
			FROM sys.extended_properties
			WHERE name = 'MS_Description' AND class = 3
		) ep ON s.schema_id = ep.major_id
		WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
		ORDER BY s.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	var schemas []common.DatabaseSchemaInfo
	for rows.Next() {
		var schema common.DatabaseSchemaInfo
		if err := rows.Scan(&schema.Name, &schema.Description); err != nil {
			return nil, fmt.Errorf("error scanning schema: %v", err)
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func getFunctions(db *sql.DB) ([]common.FunctionInfo, error) {
	query := `
		SELECT 
			s.name AS schema_name,
			o.name AS function_name,
			ISNULL(p.parameters, '') AS arguments,
			ISNULL(t.name, 'void') AS return_type,
			ISNULL(m.definition, '') AS function_body
		FROM sys.objects o
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
		LEFT JOIN sys.types t ON o.return_type = t.user_type_id
		OUTER APPLY (
			SELECT STUFF((
				SELECT ', ' + CASE WHEN p.is_output = 1 THEN '@' + p.name + ' ' + ty.name + ' OUTPUT' ELSE '@' + p.name + ' ' + ty.name END
				FROM sys.parameters p
				JOIN sys.types ty ON p.user_type_id = ty.user_type_id
				WHERE p.object_id = o.object_id AND p.parameter_id > 0
				ORDER BY p.parameter_id
				FOR XML PATH('')
			), 1, 2, '') AS parameters
		) p
		WHERE o.type IN ('FN', 'IF', 'TF')
		ORDER BY s.name, o.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	var functions []common.FunctionInfo
	for rows.Next() {
		var function common.FunctionInfo
		if err := rows.Scan(
			&function.Schema,
			&function.Name,
			&function.Arguments,
			&function.ReturnType,
			&function.Body,
		); err != nil {
			return nil, fmt.Errorf("error scanning function: %v", err)
		}
		functions = append(functions, function)
	}

	return functions, nil
}

func getTriggers(db *sql.DB) ([]common.TriggerInfo, error) {
	query := `
		SELECT 
			s.name AS schema_name,
			t.name AS trigger_name,
			OBJECT_NAME(t.parent_id) AS table_name,
			CASE 
				WHEN t.is_instead_of_trigger = 1 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS timing,
			CASE 
				WHEN OBJECTPROPERTY(t.object_id, 'ExecIsInsertTrigger') = 1 THEN 'INSERT'
				WHEN OBJECTPROPERTY(t.object_id, 'ExecIsUpdateTrigger') = 1 THEN 'UPDATE'
				WHEN OBJECTPROPERTY(t.object_id, 'ExecIsDeleteTrigger') = 1 THEN 'DELETE'
				ELSE 'MULTIPLE'
			END AS event,
			ISNULL(m.definition, '') AS definition
		FROM sys.triggers t
		JOIN sys.objects o ON t.object_id = o.object_id
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		LEFT JOIN sys.sql_modules m ON t.object_id = m.object_id
		ORDER BY s.name, t.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	var triggers []common.TriggerInfo
	for rows.Next() {
		var trigger common.TriggerInfo
		if err := rows.Scan(
			&trigger.Schema,
			&trigger.Name,
			&trigger.Table,
			&trigger.Timing,
			&trigger.Event,
			&trigger.Statement,
		); err != nil {
			return nil, fmt.Errorf("error scanning trigger: %v", err)
		}
		triggers = append(triggers, trigger)
	}

	return triggers, nil
}

func getProcedures(db *sql.DB) ([]common.ProcedureInfo, error) {
	query := `
		SELECT 
			s.name AS schema_name,
			o.name AS procedure_name,
			USER_NAME(o.principal_id) AS definer_user,
			HOST_NAME() AS definer_host,
			CONVERT(VARCHAR, o.create_date, 120) AS created,
			CONVERT(VARCHAR, o.modify_date, 120) AS modified,
			CASE WHEN o.is_ms_shipped = 1 THEN 'DEFINER' ELSE 'INVOKER' END AS security_type,
			ISNULL(ep.value, '') AS comment,
			'SQL' AS routine_body,
			ISNULL(m.definition, '') AS routine_definition,
			ISNULL(p.parameters, '') AS parameter_list,
			ISNULL(p.parameters, '') AS arguments,
			ISNULL(m.definition, '') AS body
		FROM sys.objects o
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
		LEFT JOIN sys.extended_properties ep ON o.object_id = ep.major_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
		OUTER APPLY (
			SELECT STUFF((
				SELECT ', ' + CASE WHEN p.is_output = 1 THEN '@' + p.name + ' ' + ty.name + ' OUTPUT' ELSE '@' + p.name + ' ' + ty.name END
				FROM sys.parameters p
				JOIN sys.types ty ON p.user_type_i
								FROM sys.parameters p
				JOIN sys.types ty ON p.user_type_id = ty.user_type_id
				WHERE p.object_id = o.object_id AND p.parameter_id > 0
				ORDER BY p.parameter_id
				FOR XML PATH('')
			), 1, 2, '') AS parameters
		) p
		WHERE o.type = 'P'
		ORDER BY s.name, o.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	var procedures []common.ProcedureInfo
	for rows.Next() {
		var procedure common.ProcedureInfo
		if err := rows.Scan(
			&procedure.Schema,
			&procedure.Name,
			&procedure.DefinerUser,
			&procedure.DefinerHost,
			&procedure.Created,
			&procedure.Modified,
			&procedure.SecurityType,
			&procedure.Comment,
			&procedure.RoutineBody,
			&procedure.RoutineDefinition,
			&procedure.ParameterList,
			&procedure.Arguments,
			&procedure.Body,
		); err != nil {
			return nil, fmt.Errorf("error scanning procedure: %v", err)
		}
		procedures = append(procedures, procedure)
	}

	return procedures, nil
}

func getViews(db *sql.DB) ([]common.ViewInfo, error) {
	query := `
		SELECT 
			s.name AS schema_name,
			v.name AS view_name,
			ISNULL(m.definition, '') AS definition
		FROM sys.views v
		JOIN sys.schemas s ON v.schema_id = s.schema_id
		LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
		ORDER BY s.name, v.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	var views []common.ViewInfo
	for rows.Next() {
		var view common.ViewInfo
		if err := rows.Scan(
			&view.Schema,
			&view.Name,
			&view.Definition,
		); err != nil {
			return nil, fmt.Errorf("error scanning view: %v", err)
		}
		views = append(views, view)
	}

	return views, nil
}

func getSequences(db *sql.DB) ([]common.SequenceInfo, error) {
	query := `
		SELECT 
			s.name AS schema_name,
			seq.name AS sequence_name,
			t.name AS data_type,
			seq.start_value,
			seq.increment,
			seq.minimum_value,
			seq.maximum_value,
			seq.cache_size,
			seq.is_cycling
		FROM sys.sequences seq
		JOIN sys.schemas s ON seq.schema_id = s.schema_id
		JOIN sys.types t ON seq.user_type_id = t.user_type_id
		ORDER BY s.name, seq.name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	var sequences []common.SequenceInfo
	for rows.Next() {
		var sequence common.SequenceInfo
		var isCycling bool
		if err := rows.Scan(
			&sequence.Schema,
			&sequence.Name,
			&sequence.DataType,
			&sequence.Start,
			&sequence.Increment,
			&sequence.MinValue,
			&sequence.MaxValue,
			&sequence.CacheSize,
			&isCycling,
		); err != nil {
			return nil, fmt.Errorf("error scanning sequence: %v", err)
		}
		sequence.Cycle = isCycling
		sequences = append(sequences, sequence)
	}

	return sequences, nil
}

func CreateTable(tx *sql.Tx, tableInfo common.TableInfo) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	if tableInfo.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Check if the table already exists
	var exists int
	err := tx.QueryRow(`
		SELECT COUNT(*) 
		FROM sys.tables t 
		JOIN sys.schemas s ON t.schema_id = s.schema_id 
		WHERE t.name = ? AND s.name = ?
	`, tableInfo.Name, tableInfo.Schema).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists > 0 {
		return fmt.Errorf("table '%s.%s' already exists", tableInfo.Schema, tableInfo.Name)
	}

	// Create schema if it doesn't exist
	_, err = tx.Exec(fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%s') EXEC('CREATE SCHEMA %s')",
		tableInfo.Schema, tableInfo.Schema))
	if err != nil {
		return fmt.Errorf("error creating schema: %v", err)
	}

	// Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE [%s].[%s] (", tableInfo.Schema, tableInfo.Name)
	for i, column := range tableInfo.Columns {
		if i > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("[%s] ", column.Name)

		// Handle data type
		switch strings.ToLower(column.DataType) {
		case "varchar":
			if column.VarcharLength != nil {
				createTableSQL += fmt.Sprintf("VARCHAR(%d)", *column.VarcharLength)
			} else {
				createTableSQL += "VARCHAR(MAX)"
			}
		case "nvarchar":
			if column.VarcharLength != nil {
				createTableSQL += fmt.Sprintf("NVARCHAR(%d)", *column.VarcharLength)
			} else {
				createTableSQL += "NVARCHAR(MAX)"
			}
		default:
			createTableSQL += column.DataType
		}

		// Handle auto-increment (identity)
		if column.IsAutoIncrement {
			createTableSQL += " IDENTITY(1,1)"
		}

		// Handle nullability
		if !column.IsNullable {
			createTableSQL += " NOT NULL"
		} else {
			createTableSQL += " NULL"
		}

		// Handle default value
		if column.ColumnDefault != nil {
			createTableSQL += fmt.Sprintf(" DEFAULT %s", *column.ColumnDefault)
		}
	}

	// Add primary key constraint if defined
	if len(tableInfo.PrimaryKey) > 0 {
		createTableSQL += fmt.Sprintf(", CONSTRAINT [PK_%s] PRIMARY KEY (%s)",
			tableInfo.Name, strings.Join(quoteIdentifiers(tableInfo.PrimaryKey), ", "))
	}

	createTableSQL += ")"

	// Print the SQL statement for debugging
	fmt.Printf("Creating table %s.%s with SQL: %s\n", tableInfo.Schema, tableInfo.Name, createTableSQL)

	_, err = tx.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Add indexes
	for _, index := range tableInfo.Indexes {
		// Skip primary key indexes as they are handled in table creation
		if strings.HasPrefix(index.Name, "PK_") {
			continue
		}

		indexSQL := "CREATE"
		if index.IsUnique {
			indexSQL += " UNIQUE"
		}
		indexSQL += fmt.Sprintf(" INDEX [%s] ON [%s].[%s] (%s)",
			index.Name, tableInfo.Schema, tableInfo.Name, strings.Join(quoteIdentifiers(index.Columns), ", "))

		_, err = tx.Exec(indexSQL)
		if err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

func AddTableConstraints(tx *sql.Tx, tableInfo common.TableInfo) error {
	addedConstraints := make(map[string]bool)

	for _, constraint := range tableInfo.Constraints {
		// Skip if constraint has already been added
		if addedConstraints[constraint.Name] {
			continue
		}

		// Skip primary key constraints as they are handled during table creation
		if constraint.Type == "PRIMARY KEY" {
			continue
		}

		var constraintSQL string
		switch constraint.Type {
		case "FOREIGN KEY":
			constraintSQL = fmt.Sprintf(`
				ALTER TABLE [%s].[%s] 
				ADD CONSTRAINT [%s] FOREIGN KEY ([%s]) 
				REFERENCES [%s].[%s] ([%s])`,
				tableInfo.Schema, tableInfo.Name,
				constraint.Name, constraint.Column,
				tableInfo.Schema, constraint.ForeignTable, constraint.ForeignColumn)

			// Add ON DELETE and ON UPDATE if specified
			if constraint.OnDelete != "" {
				constraintSQL += fmt.Sprintf(" ON DELETE %s", constraint.OnDelete)
			}
			if constraint.OnUpdate != "" {
				constraintSQL += fmt.Sprintf(" ON UPDATE %s", constraint.OnUpdate)
			}
		case "CHECK":
			constraintSQL = fmt.Sprintf(`
				ALTER TABLE [%s].[%s] 
				ADD CONSTRAINT [%s] CHECK (%s)`,
				tableInfo.Schema, tableInfo.Name,
				constraint.Name, constraint.Definition)
		case "UNIQUE":
			constraintSQL = fmt.Sprintf(`
				ALTER TABLE [%s].[%s] 
				ADD CONSTRAINT [%s] UNIQUE ([%s])`,
				tableInfo.Schema, tableInfo.Name,
				constraint.Name, constraint.Column)
		default:
			return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
		}

		// Print the SQL statement for debugging
		fmt.Printf("Executing constraint SQL: %s\n", constraintSQL)

		_, err := tx.Exec(constraintSQL)
		if err != nil {
			return fmt.Errorf("error adding constraint %s: %v", constraint.Name, err)
		}
		addedConstraints[constraint.Name] = true
	}

	return nil
}

// Helper function to quote identifiers
func quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = fmt.Sprintf("[%s]", id)
	}
	return quoted
}
