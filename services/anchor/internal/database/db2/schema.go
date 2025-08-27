package db2

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

	var err error

	// Get tables and their columns
	tablesMap, _, err := discoverTablesAndColumns(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Convert tables to unified model
	for _, table := range tablesMap {
		convertedTable := ConvertDb2Table(*table)
		um.Tables[table.Name] = convertedTable
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
			Language:   "sql", // DB2 functions are SQL-based
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

	// Get procedures
	procedures, err := getProcedures(db)
	if err != nil {
		return nil, fmt.Errorf("error getting procedures: %v", err)
	}

	// Convert procedures to unified model
	for _, procedure := range procedures {
		um.Procedures[procedure.Name] = unifiedmodel.Procedure{
			Name:       procedure.Name,
			Language:   "sql", // DB2 procedures are SQL-based
			Definition: procedure.Body,
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
			t.TABSCHEMA,
			t.TABNAME,
			c.COLNAME,
			c.TYPENAME,
			CASE WHEN c.NULLS = 'Y' THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,
			c.DEFAULT,
			CASE WHEN k.COLNAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY,
			CASE WHEN c.TYPENAME LIKE '%ARRAY%' THEN 1 ELSE 0 END AS IS_ARRAY,
			CASE WHEN u.COLNAME IS NOT NULL THEN 1 ELSE 0 END AS IS_UNIQUE,
			CASE WHEN c.IDENTITY = 'Y' THEN 1 ELSE 0 END AS IS_AUTO_INCREMENT,
			CASE 
				WHEN t.TYPE = 'T' AND t.STATUS = 'N' THEN 'db2.standard'
				WHEN t.TYPE = 'T' AND t.STATUS = 'C' THEN 'db2.check_pending'
				WHEN t.TYPE = 'V' THEN 'db2.view'
				WHEN t.TYPE = 'A' THEN 'db2.alias'
				WHEN t.TYPE = 'G' THEN 'db2.global_temporary'
				ELSE 'db2.other'
			END AS TABLE_TYPE,
			c.LENGTH,
			c.SCALE
		FROM 
			SYSCAT.TABLES t
		JOIN 
			SYSCAT.COLUMNS c ON t.TABSCHEMA = c.TABSCHEMA AND t.TABNAME = c.TABNAME
		LEFT JOIN 
			(SELECT TABSCHEMA, TABNAME, COLNAME FROM SYSCAT.KEYCOLUSE WHERE COLSEQ = 1) k
			ON c.TABSCHEMA = k.TABSCHEMA AND c.TABNAME = k.TABNAME AND c.COLNAME = k.COLNAME
		LEFT JOIN 
			(SELECT TABSCHEMA, TABNAME, COLNAME FROM SYSCAT.UNIQUEKEYS) u
			ON c.TABSCHEMA = u.TABSCHEMA AND c.TABNAME = u.TABNAME AND c.COLNAME = u.COLNAME
		WHERE 
			t.TABSCHEMA NOT LIKE 'SYS%'
			AND t.TABSCHEMA NOT LIKE 'IBMDB%'
		ORDER BY 
			t.TABSCHEMA, t.TABNAME, c.COLNO
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching table and column information: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*common.TableInfo)
	tableNames := make([]string, 0)
	for rows.Next() {
		var schemaName, tableName, columnName, dataType, isNullable string
		var columnDefault sql.NullString
		var isPrimaryKey, isArray, isUnique, isAutoIncrement int
		var tableType string
		var length, scale sql.NullInt64

		if err := rows.Scan(
			&schemaName, &tableName, &columnName, &dataType, &isNullable, &columnDefault,
			&isPrimaryKey, &isArray, &isUnique, &isAutoIncrement, &tableType, &length, &scale,
		); err != nil {
			return nil, nil, fmt.Errorf("error scanning table and column row: %v", err)
		}

		// Create a unique key for the table
		tableKey := fmt.Sprintf("%s.%s", schemaName, tableName)

		if _, exists := tables[tableKey]; !exists {
			tables[tableKey] = &common.TableInfo{
				Name:      tableName,
				Schema:    schemaName,
				TableType: tableType,
			}
			tableNames = append(tableNames, tableKey)
		}

		var defaultValue *string
		if columnDefault.Valid {
			defaultValue = &columnDefault.String
		}

		columnInfo := common.ColumnInfo{
			Name:            columnName,
			DataType:        dataType,
			IsNullable:      isNullable == "YES",
			ColumnDefault:   defaultValue,
			IsPrimaryKey:    isPrimaryKey == 1,
			IsArray:         isArray == 1,
			IsUnique:        isUnique == 1,
			IsAutoIncrement: isAutoIncrement == 1,
		}

		// Handle varchar length
		if (dataType == "VARCHAR" || dataType == "CHARACTER VARYING") && length.Valid {
			varcharLength := int(length.Int64)
			columnInfo.VarcharLength = &varcharLength
		}

		// Handle numeric precision and scale
		if (dataType == "DECIMAL" || dataType == "NUMERIC") && length.Valid {
			precision := strconv.FormatInt(length.Int64, 10)
			columnInfo.NumericPrecision = &precision

			if scale.Valid {
				scaleStr := strconv.FormatInt(scale.Int64, 10)
				columnInfo.NumericScale = &scaleStr
			}
		}

		tables[tableKey].Columns = append(tables[tableKey].Columns, columnInfo)
	}

	// Get primary keys for each table
	for tableKey, table := range tables {
		primaryKeys, err := getPrimaryKeys(db, table.Schema, table.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting primary keys for table %s: %v", tableKey, err)
		}
		table.PrimaryKey = primaryKeys

		// Get indexes
		indexes, err := getIndexes(db, table.Schema, table.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting indexes for table %s: %v", tableKey, err)
		}
		table.Indexes = indexes

		// Get constraints
		constraints, err := getConstraints(db, table.Schema, table.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting constraints for table %s: %v", tableKey, err)
		}
		table.Constraints = constraints
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func getPrimaryKeys(db *sql.DB, schema, tableName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.KEYCOLUSE
		WHERE TABSCHEMA = ? AND TABNAME = ?
		ORDER BY COLSEQ
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying primary keys: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("error scanning primary key: %v", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	return primaryKeys, nil
}

func getIndexes(db *sql.DB, schema, tableName string) ([]common.IndexInfo, error) {
	query := `
		SELECT 
			i.INDNAME,
			i.UNIQUERULE = 'U' AS IS_UNIQUE
		FROM 
			SYSCAT.INDEXES i
		WHERE 
			i.TABSCHEMA = ? AND i.TABNAME = ?
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	var indexes []common.IndexInfo
	for rows.Next() {
		var indexName string
		var isUnique bool
		if err := rows.Scan(&indexName, &isUnique); err != nil {
			return nil, fmt.Errorf("error scanning index: %v", err)
		}

		// Get columns for this index
		columns, err := getIndexColumns(db, schema, indexName)
		if err != nil {
			return nil, fmt.Errorf("error getting columns for index %s: %v", indexName, err)
		}

		indexes = append(indexes, common.IndexInfo{
			Name:     indexName,
			Columns:  columns,
			IsUnique: isUnique,
		})
	}

	return indexes, nil
}

func getIndexColumns(db *sql.DB, schema, indexName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.INDEXCOLUSE
		WHERE INDSCHEMA = ? AND INDNAME = ?
		ORDER BY COLSEQ
	`

	rows, err := db.Query(query, schema, indexName)
	if err != nil {
		return nil, fmt.Errorf("error querying index columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("error scanning index column: %v", err)
		}
		columns = append(columns, columnName)
	}

	return columns, nil
}

func getConstraints(db *sql.DB, schema, tableName string) ([]common.Constraint, error) {
	query := `
		SELECT 
			c.CONSTNAME,
			c.TYPE,
			c.TABSCHEMA,
			c.TABNAME,
			k.COLNAME,
			r.REFTABSCHEMA,
			r.REFTABNAME,
			r.REFKEYNAME,
			c.DELETERULE,
			c.UPDATERULE
		FROM 
			SYSCAT.TABCONST c
		LEFT JOIN 
			SYSCAT.KEYCOLUSE k ON c.CONSTNAME = k.CONSTNAME AND c.TABSCHEMA = k.TABSCHEMA AND c.TABNAME = k.TABNAME
		LEFT JOIN 
			SYSCAT.REFERENCES r ON c.CONSTNAME = r.CONSTNAME AND c.TABSCHEMA = r.TABSCHEMA AND c.TABNAME = r.TABNAME
		WHERE 
			c.TABSCHEMA = ? AND c.TABNAME = ?
	`

	rows, err := db.Query(query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	var constraints []common.Constraint
	for rows.Next() {
		var constraintName, constraintType, tabSchema, tabName, colName string
		var refSchema, refTable, refKey, deleteRule, updateRule sql.NullString

		if err := rows.Scan(
			&constraintName, &constraintType, &tabSchema, &tabName, &colName,
			&refSchema, &refTable, &refKey, &deleteRule, &updateRule,
		); err != nil {
			return nil, fmt.Errorf("error scanning constraint: %v", err)
		}

		var constraint common.Constraint
		constraint.Name = constraintName
		constraint.Table = tabName
		constraint.Column = colName

		// Map Db2 constraint types to common types
		switch constraintType {
		case "P":
			constraint.Type = "PRIMARY KEY"
		case "U":
			constraint.Type = "UNIQUE"
		case "F":
			constraint.Type = "FOREIGN KEY"
			if refSchema.Valid && refTable.Valid {
				constraint.ForeignTable = refTable.String
				constraint.ReferencedTable = refTable.String

				// Get the referenced column
				refColumns, err := getReferencedColumns(db, refSchema.String, refKey.String)
				if err == nil && len(refColumns) > 0 {
					constraint.ForeignColumn = refColumns[0]
					constraint.ReferencedColumn = refColumns[0]
				}

				if deleteRule.Valid {
					constraint.OnDelete = deleteRule.String
				}
				if updateRule.Valid {
					constraint.OnUpdate = updateRule.String
				}

				// Create foreign key info
				constraint.ForeignKey = &common.ForeignKeyInfo{
					Table:    refTable.String,
					Column:   constraint.ForeignColumn,
					OnDelete: constraint.OnDelete,
					OnUpdate: constraint.OnUpdate,
				}

				// Create definition for foreign key
				constraint.Definition = fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)",
					colName, refTable.String, constraint.ForeignColumn)

				if constraint.OnDelete != "" {
					constraint.Definition += fmt.Sprintf(" ON DELETE %s", constraint.OnDelete)
				}
				if constraint.OnUpdate != "" {
					constraint.Definition += fmt.Sprintf(" ON UPDATE %s", constraint.OnUpdate)
				}
			}
		case "C":
			constraint.Type = "CHECK"
			// Get check constraint definition
			var definition string
			err := db.QueryRow(`
				SELECT TEXT FROM SYSCAT.CHECKS 
				WHERE CONSTNAME = ? AND TABSCHEMA = ? AND TABNAME = ?`,
				constraintName, schema, tableName).Scan(&definition)
			if err == nil {
				constraint.Definition = fmt.Sprintf("CHECK (%s)", definition)
			}
		default:
			constraint.Type = constraintType
		}

		constraints = append(constraints, constraint)
	}

	return constraints, nil
}

func getReferencedColumns(db *sql.DB, schema, keyName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.KEYCOLUSE
		WHERE TABSCHEMA = ? AND CONSTNAME = ?
		ORDER BY COLSEQ
	`

	rows, err := db.Query(query, schema, keyName)
	if err != nil {
		return nil, fmt.Errorf("error querying referenced columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("error scanning referenced column: %v", err)
		}
		columns = append(columns, columnName)
	}

	return columns, nil
}

func getSchemas(db *sql.DB) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT 
			SCHEMANAME,
			REMARKS
		FROM 
			SYSCAT.SCHEMATA
		WHERE 
			SCHEMANAME NOT LIKE 'SYS%'
			AND SCHEMANAME NOT LIKE 'IBMDB%'
		ORDER BY 
			SCHEMANAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	var schemas []common.DatabaseSchemaInfo
	for rows.Next() {
		var schema common.DatabaseSchemaInfo
		var description sql.NullString
		if err := rows.Scan(&schema.Name, &description); err != nil {
			return nil, fmt.Errorf("error scanning schema: %v", err)
		}
		if description.Valid {
			schema.Description = description.String
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func getFunctions(db *sql.DB) ([]common.FunctionInfo, error) {
	query := `
		SELECT 
			ROUTINESCHEMA,
			ROUTINENAME,
			SPECIFICNAME,
			RETURN_TYPE,
			TEXT
		FROM 
			SYSCAT.ROUTINES
		WHERE 
			ROUTINETYPE = 'F'
			AND ROUTINESCHEMA NOT LIKE 'SYS%'
			AND ROUTINESCHEMA NOT LIKE 'IBMDB%'
		ORDER BY 
			ROUTINESCHEMA, ROUTINENAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	var functions []common.FunctionInfo
	for rows.Next() {
		var function common.FunctionInfo
		var specificName string
		var body sql.NullString
		if err := rows.Scan(
			&function.Schema,
			&function.Name,
			&specificName,
			&function.ReturnType,
			&body,
		); err != nil {
			return nil, fmt.Errorf("error scanning function: %v", err)
		}

		// Get function parameters
		params, err := getFunctionParameters(db, function.Schema, specificName)
		if err != nil {
			return nil, fmt.Errorf("error getting parameters for function %s: %v", function.Name, err)
		}
		function.Arguments = params

		if body.Valid {
			function.Body = body.String
		}

		functions = append(functions, function)
	}

	return functions, nil
}

func getFunctionParameters(db *sql.DB, schema, specificName string) (string, error) {
	query := `
		SELECT 
			PARMNAME,
			TYPENAME,
			LENGTH,
			SCALE,
			PARM_MODE
		FROM 
			SYSCAT.ROUTINEPARMS
		WHERE 
			ROUTINESCHEMA = ? 
			AND SPECIFICNAME = ?
			AND PARM_MODE IN ('IN', 'INOUT', 'OUT')
		ORDER BY 
			ORDINAL
	`

	rows, err := db.Query(query, schema, specificName)
	if err != nil {
		return "", fmt.Errorf("error querying function parameters: %v", err)
	}
	defer rows.Close()

	var params []string
	for rows.Next() {
		var name, typeName, mode string
		var length, scale sql.NullInt64
		if err := rows.Scan(&name, &typeName, &length, &scale, &mode); err != nil {
			return "", fmt.Errorf("error scanning function parameter: %v", err)
		}

		// Format the parameter type
		paramType := typeName
		if (typeName == "VARCHAR" || typeName == "CHARACTER VARYING") && length.Valid {
			paramType = fmt.Sprintf("%s(%d)", typeName, length.Int64)
		} else if (typeName == "DECIMAL" || typeName == "NUMERIC") && length.Valid && scale.Valid {
			paramType = fmt.Sprintf("%s(%d,%d)", typeName, length.Int64, scale.Int64)
		}

		// Format the parameter
		param := fmt.Sprintf("%s %s %s", mode, name, paramType)
		params = append(params, param)
	}

	return strings.Join(params, ", "), nil
}

func getTriggers(db *sql.DB) ([]common.TriggerInfo, error) {
	query := `
		SELECT 
			TRIGSCHEMA,
			TRIGNAME,
			TABSCHEMA,
			TABNAME,
			TRIGTIME,
			TRIGEVENT,
			TEXT
		FROM 
			SYSCAT.TRIGGERS
		WHERE 
			TRIGSCHEMA NOT LIKE 'SYS%'
			AND TRIGSCHEMA NOT LIKE 'IBMDB%'
		ORDER BY 
			TRIGSCHEMA, TRIGNAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	var triggers []common.TriggerInfo
	for rows.Next() {
		var trigger common.TriggerInfo
		var body sql.NullString
		if err := rows.Scan(
			&trigger.Schema,
			&trigger.Name,
			&trigger.Schema, // Table schema
			&trigger.Table,
			&trigger.Timing,
			&trigger.Event,
			&body,
		); err != nil {
			return nil, fmt.Errorf("error scanning trigger: %v", err)
		}

		if body.Valid {
			trigger.Statement = body.String
		}

		triggers = append(triggers, trigger)
	}

	return triggers, nil
}

func getSequences(db *sql.DB) ([]common.SequenceInfo, error) {
	query := `
		SELECT 
			SEQSCHEMA,
			SEQNAME,
			DATATYPEID,
			START,
			INCREMENT,
			MAXVALUE,
			MINVALUE,
			CACHE,
			CYCLE
		FROM 
			SYSCAT.SEQUENCES
		WHERE 
			SEQSCHEMA NOT LIKE 'SYS%'
			AND SEQSCHEMA NOT LIKE 'IBMDB%'
		ORDER BY 
			SEQSCHEMA, SEQNAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	var sequences []common.SequenceInfo
	for rows.Next() {
		var seq common.SequenceInfo
		var dataTypeID int
		var cycle string
		if err := rows.Scan(
			&seq.Schema,
			&seq.Name,
			&dataTypeID,
			&seq.Start,
			&seq.Increment,
			&seq.MaxValue,
			&seq.MinValue,
			&seq.CacheSize,
			&cycle,
		); err != nil {
			return nil, fmt.Errorf("error scanning sequence: %v", err)
		}

		// Map dataTypeID to data type name
		switch dataTypeID {
		case 496:
			seq.DataType = "SMALLINT"
		case 492:
			seq.DataType = "INTEGER"
		case 484:
			seq.DataType = "BIGINT"
		default:
			seq.DataType = "INTEGER"
		}

		seq.Cycle = cycle == "Y"

		sequences = append(sequences, seq)
	}

	return sequences, nil
}

func getProcedures(db *sql.DB) ([]common.ProcedureInfo, error) {
	query := `
		SELECT 
			ROUTINESCHEMA,
			ROUTINENAME,
			SPECIFICNAME,
			ORIGIN,
			CREATETIME,
			ALTERTIME,
			DETERMINISTIC,
			FENCED,
			TEXT
		FROM 
			SYSCAT.ROUTINES
		WHERE 
			ROUTINETYPE = 'P'
			AND ROUTINESCHEMA NOT LIKE 'SYS%'
			AND ROUTINESCHEMA NOT LIKE 'IBMDB%'
		ORDER BY 
			ROUTINESCHEMA, ROUTINENAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	var procedures []common.ProcedureInfo
	for rows.Next() {
		var proc common.ProcedureInfo
		var specificName, origin, deterministic, fenced string
		var createTime, alterTime sql.NullString
		var body sql.NullString
		if err := rows.Scan(
			&proc.Schema,
			&proc.Name,
			&specificName,
			&origin,
			&createTime,
			&alterTime,
			&deterministic,
			&fenced,
			&body,
		); err != nil {
			return nil, fmt.Errorf("error scanning procedure: %v", err)
		}

		// Get procedure parameters
		params, err := getFunctionParameters(db, proc.Schema, specificName)
		if err != nil {
			return nil, fmt.Errorf("error getting parameters for procedure %s: %v", proc.Name, err)
		}
		proc.Arguments = params
		proc.ParameterList = params

		if createTime.Valid {
			proc.Created = createTime.String
		}
		if alterTime.Valid {
			proc.Modified = alterTime.String
		}

		proc.IsDeterministic = deterministic
		proc.SecurityType = fenced
		proc.RoutineBody = "SQL"

		if body.Valid {
			proc.Body = body.String
			proc.RoutineDefinition = body.String
		}

		procedures = append(procedures, proc)
	}

	return procedures, nil
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
	err := tx.QueryRow("SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?",
		tableInfo.Schema, tableInfo.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists > 0 {
		return fmt.Errorf("table '%s.%s' already exists", tableInfo.Schema, tableInfo.Name)
	}

	// Create table
	var createTableSQL strings.Builder

	// If schema is specified, use it
	if tableInfo.Schema != "" {
		fmt.Fprintf(&createTableSQL, "CREATE TABLE %s.%s (", tableInfo.Schema, tableInfo.Name)
	} else {
		fmt.Fprintf(&createTableSQL, "CREATE TABLE %s (", tableInfo.Name)
	}

	for i, column := range tableInfo.Columns {
		if i > 0 {
			createTableSQL.WriteString(", ")
		}
		fmt.Fprintf(&createTableSQL, "%s ", column.Name)

		// Handle auto-increment columns
		isIdentity := column.IsAutoIncrement

		if isIdentity {
			createTableSQL.WriteString("INTEGER GENERATED BY DEFAULT AS IDENTITY")
		} else {
			// Handle data types
			switch strings.ToUpper(column.DataType) {
			case "VARCHAR", "CHARACTER VARYING":
				if column.VarcharLength == nil {
					createTableSQL.WriteString("VARCHAR(255)")
				} else {
					fmt.Fprintf(&createTableSQL, "VARCHAR(%d)", *column.VarcharLength)
				}
			case "DECIMAL", "NUMERIC":
				if column.NumericPrecision != nil && column.NumericScale != nil {
					fmt.Fprintf(&createTableSQL, "DECIMAL(%s,%s)", *column.NumericPrecision, *column.NumericScale)
				} else if column.NumericPrecision != nil {
					fmt.Fprintf(&createTableSQL, "DECIMAL(%s)", *column.NumericPrecision)
				} else {
					createTableSQL.WriteString("DECIMAL(10,0)")
				}
			default:
				createTableSQL.WriteString(column.DataType)
			}
		}

		if !column.IsNullable {
			createTableSQL.WriteString(" NOT NULL")
		}
		if column.ColumnDefault != nil && !isIdentity {
			fmt.Fprintf(&createTableSQL, " DEFAULT %s", *column.ColumnDefault)
		}
	}
	createTableSQL.WriteString(")")

	// Print the SQL statement
	fmt.Printf("Creating table %s with SQL: %s\n", tableInfo.Name, createTableSQL.String())

	_, err = tx.Exec(createTableSQL.String())
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Add primary key
	if len(tableInfo.PrimaryKey) > 0 {
		var primaryKeySQL strings.Builder

		if tableInfo.Schema != "" {
			fmt.Fprintf(&primaryKeySQL, "ALTER TABLE %s.%s ADD CONSTRAINT %s_PK PRIMARY KEY (%s)",
				tableInfo.Schema, tableInfo.Name, tableInfo.Name, strings.Join(tableInfo.PrimaryKey, ", "))
		} else {
			fmt.Fprintf(&primaryKeySQL, "ALTER TABLE %s ADD CONSTRAINT %s_PK PRIMARY KEY (%s)",
				tableInfo.Name, tableInfo.Name, strings.Join(tableInfo.PrimaryKey, ", "))
		}

		_, err = tx.Exec(primaryKeySQL.String())
		if err != nil {
			return fmt.Errorf("error adding primary key: %v", err)
		}
	}

	// Add indexes
	for _, index := range tableInfo.Indexes {
		// Skip primary key indexes
		if index.Name == tableInfo.Name+"_PK" {
			continue
		}

		var indexSQL strings.Builder
		indexSQL.WriteString("CREATE")
		if index.IsUnique {
			indexSQL.WriteString(" UNIQUE")
		}

		if tableInfo.Schema != "" {
			fmt.Fprintf(&indexSQL, " INDEX %s ON %s.%s (%s)",
				index.Name, tableInfo.Schema, tableInfo.Name, strings.Join(index.Columns, ", "))
		} else {
			fmt.Fprintf(&indexSQL, " INDEX %s ON %s (%s)",
				index.Name, tableInfo.Name, strings.Join(index.Columns, ", "))
		}

		_, err = tx.Exec(indexSQL.String())
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

		// Skip primary key constraints as they are handled separately during table creation
		if constraint.Type == "PRIMARY KEY" {
			continue
		}

		var constraintSQL strings.Builder

		// Use schema if provided
		var tableName string
		if tableInfo.Schema != "" {
			tableName = fmt.Sprintf("%s.%s", tableInfo.Schema, tableInfo.Name)
		} else {
			tableName = tableInfo.Name
		}

		switch constraint.Type {
		case "FOREIGN KEY":
			if constraint.Definition == "" {
				fmt.Printf("Warning: Skipping empty foreign key constraint definition %s for table %s\n", constraint.Name, tableInfo.Name)
				continue
			}

			fmt.Fprintf(&constraintSQL, "ALTER TABLE %s ADD CONSTRAINT %s %s",
				tableName, constraint.Name, constraint.Definition)

		case "CHECK":
			fmt.Fprintf(&constraintSQL, "ALTER TABLE %s ADD CONSTRAINT %s %s",
				tableName, constraint.Name, constraint.Definition)

		case "UNIQUE":
			fmt.Fprintf(&constraintSQL, "ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
				tableName, constraint.Name, constraint.Column)

		case "":
			// Skip constraints with empty type
			fmt.Printf("Warning: Skipping constraint with empty type for table %s\n", tableInfo.Name)
			continue

		default:
			return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
		}

		// Print the SQL statement for debugging
		fmt.Printf("Executing constraint SQL: %s\n", constraintSQL.String())

		_, err := tx.Exec(constraintSQL.String())
		if err != nil {
			// If an error occurs, print the error message and full SQL statement
			fmt.Printf("Error adding constraint %s: %v\n", constraint.Name, err)
			fmt.Printf("Full SQL statement: %s\n", constraintSQL.String())
			return fmt.Errorf("error adding constraint %s: %v", constraint.Name, err)
		}
		addedConstraints[constraint.Name] = true
	}

	return nil
}
