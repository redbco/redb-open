package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"

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

	// Get tables and their columns
	tablesMap, _, err := discoverTablesAndColumns(pool)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Convert tables to unified model
	for _, table := range tablesMap {
		unifiedTable := ConvertCockroachTable(*table)
		um.Tables[table.Name] = unifiedTable
	}

	// Get enum types (CockroachDB enums are stored as custom types)
	enumTypes, err := discoverEnumTypes(pool)
	if err != nil {
		return nil, fmt.Errorf("error discovering enum types: %v", err)
	}

	// Convert enums to unified model as custom types
	for _, enumType := range enumTypes {
		um.Types[enumType.Name] = unifiedmodel.Type{
			Name:     enumType.Name,
			Category: "enum",
		}
	}

	// Get schemas
	schemas, err := getSchemas(pool)
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
	functions, err := getFunctions(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Convert functions to unified model
	for _, function := range functions {
		um.Functions[function.Name] = unifiedmodel.Function{
			Name:       function.Name,
			Language:   "sql", // CockroachDB functions are SQL-based
			Definition: function.Body,
			Returns:    function.ReturnType,
		}
	}

	// Get triggers
	triggers, err := getTriggers(pool)
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
	sequences, err := getSequences(pool)
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

	// Get extensions
	extensions, err := getExtensions(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting extensions: %v", err)
	}

	// Convert extensions to unified model
	for _, extension := range extensions {
		um.Extensions[extension.Name] = unifiedmodel.Extension{
			Name:    extension.Name,
			Version: extension.Version,
		}
	}

	return um, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(pool *pgxpool.Pool, params common.StructureParams) error {
	// Start a transaction
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback(context.Background())

	// Create enum types first
	for _, enum := range params.EnumTypes {
		if err := createEnumType(tx, enum.Name, enum.Values); err != nil {
			return fmt.Errorf("error creating enum type %s: %v", enum.Name, err)
		}
	}

	// Sort tables based on dependencies
	sortedTables, err := common.TopologicalSort(params.Tables)
	if err != nil {
		return fmt.Errorf("error sorting tables: %v", err)
	}

	// Create tables
	for _, table := range sortedTables {
		if err := CreateTable(tx, table, params.EnumTypes); err != nil {
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
	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}

func discoverTablesAndColumns(pool *pgxpool.Pool) (map[string]*common.TableInfo, []string, error) {
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
                    SELECT 1 FROM crdb_internal.tables ct
                    WHERE ct.name = t.table_name AND ct.is_temporary = false
                ) THEN 'cockroach.standard'
                WHEN t.table_type = 'LOCAL TEMPORARY' THEN 'cockroach.temporary'
                ELSE 'cockroach.standard'
            END as table_type,
            NULL as parent_table,
            NULL as partition_value
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
		return nil, nil, fmt.Errorf("error fetching table and column information: %v", err)
	}
	defer rows.Close()

	tables := make(map[string]*common.TableInfo)
	tableNames := make([]string, 0)
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
			return nil, nil, fmt.Errorf("error scanning table and column row: %v", err)
		}

		if _, exists := tables[tableName]; !exists {
			tables[tableName] = &common.TableInfo{
				Name:           tableName,
				Schema:         schemaName,
				TableType:      tableType,
				ParentTable:    parentTable.String,
				PartitionValue: partitionValue.String,
			}
			tableNames = append(tableNames, tableName)
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
			IsPrimaryKey:    isPrimaryKey,
			IsArray:         isArray,
			IsUnique:        isUnique,
			IsAutoIncrement: isAutoIncrement,
		}

		if dataType == "ARRAY" && arrayElementType.Valid {
			columnInfo.ArrayElementType = &arrayElementType.String
			columnInfo.CustomTypeName = &customTypeName.String
		} else if dataType == "USER-DEFINED" && customTypeName.Valid {
			columnInfo.CustomTypeName = &customTypeName.String
		}

		if dataType == "character varying" && atttypmod.Valid {
			if atttypmod.Int64 != -1 {
				varcharLength := int(atttypmod.Int64) - 4
				columnInfo.VarcharLength = &varcharLength
			} else {
				columnInfo.VarcharLength = nil
			}
		}

		tables[tableName].Columns = append(tables[tableName].Columns, columnInfo)
	}

	// Fetch primary keys for each table
	for _, table := range tables {
		err := fetchPrimaryKeyInfo(pool, table)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching primary key info for table %s: %v", table.Name, err)
		}

		// Fetch indexes for each table
		err = fetchIndexInfo(pool, table)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching index info for table %s: %v", table.Name, err)
		}
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func fetchPrimaryKeyInfo(pool *pgxpool.Pool, table *common.TableInfo) error {
	query := `
    SELECT 
        kcu.column_name
    FROM 
        information_schema.table_constraints tc
    JOIN 
        information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE 
        tc.constraint_type = 'PRIMARY KEY' 
        AND tc.table_name = $1
    ORDER BY 
        kcu.ordinal_position;
    `

	rows, err := pool.Query(context.Background(), query, table.Name)
	if err != nil {
		return fmt.Errorf("error querying primary key info: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return fmt.Errorf("error scanning primary key column: %v", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	table.PrimaryKey = primaryKeys
	return nil
}

func fetchIndexInfo(pool *pgxpool.Pool, table *common.TableInfo) error {
	query := `
    SELECT 
        i.relname as index_name,
        array_agg(a.attname) as column_names,
        ix.indisunique as is_unique
    FROM 
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a
    WHERE 
        t.oid = ix.indrelid
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relname = $1
    GROUP BY 
        i.relname, ix.indisunique
    ORDER BY 
        i.relname;
    `

	rows, err := pool.Query(context.Background(), query, table.Name)
	if err != nil {
		return fmt.Errorf("error querying index info: %v", err)
	}
	defer rows.Close()

	var indexes []common.IndexInfo
	for rows.Next() {
		var indexName string
		var columnNames []string
		var isUnique bool

		if err := rows.Scan(&indexName, &columnNames, &isUnique); err != nil {
			return fmt.Errorf("error scanning index info: %v", err)
		}

		// Skip primary key index which is already handled
		if indexName == fmt.Sprintf("%s_pkey", table.Name) {
			continue
		}

		indexes = append(indexes, common.IndexInfo{
			Name:     indexName,
			Columns:  columnNames,
			IsUnique: isUnique,
		})
	}

	table.Indexes = indexes
	return nil
}

func getSchemas(pool *pgxpool.Pool) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT 
			nspname as schema_name,
			pg_catalog.obj_description(oid, 'pg_namespace') as description
		FROM pg_catalog.pg_namespace
		WHERE nspname !~ '^pg_'
		AND nspname != 'information_schema'
		AND nspname != 'crdb_internal'
		ORDER BY nspname;`

	rows, err := pool.Query(context.Background(), query)
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

func discoverEnumTypes(pool *pgxpool.Pool) ([]common.EnumInfo, error) {
	// CockroachDB supports enums similar to PostgreSQL
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
		return nil, fmt.Errorf("error fetching enum types: %v", err)
	}
	defer rows.Close()

	var enums []common.EnumInfo

	enumMap := make(map[string][]string)
	for rows.Next() {
		var enumName, enumValue string
		if err := rows.Scan(&enumName, &enumValue); err != nil {
			return nil, fmt.Errorf("error scanning enum row: %v", err)
		}
		enumMap[enumName] = append(enumMap[enumName], enumValue)
	}

	for enumName, enumValues := range enumMap {
		enums = append(enums, common.EnumInfo{
			Name:   enumName,
			Values: enumValues,
		})
	}

	return enums, rows.Err()
}

func getFunctions(pool *pgxpool.Pool) ([]common.FunctionInfo, error) {
	// CockroachDB has limited support for user-defined functions compared to PostgreSQL
	query := `SELECT n.nspname AS schema_name, p.proname AS function_name, 
              pg_get_function_arguments(p.oid) AS argument_data_types,
              t.typname AS return_type,
              p.prosrc AS function_body
              FROM pg_proc p
              LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
              LEFT JOIN pg_type t ON p.prorettype = t.oid
              WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')`
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []common.FunctionInfo

	for rows.Next() {
		var function common.FunctionInfo
		if err := rows.Scan(&function.Schema, &function.Name, &function.Arguments, &function.ReturnType, &function.Body); err != nil {
			return nil, err
		}
		functions = append(functions, function)
	}
	return functions, rows.Err()
}

func getTriggers(pool *pgxpool.Pool) ([]common.TriggerInfo, error) {
	// CockroachDB has limited support for triggers compared to PostgreSQL
	// This is a simplified query that may return fewer results
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
		// If the query fails, return an empty slice instead of an error
		// since CockroachDB might not support all trigger functionality
		return []common.TriggerInfo{}, nil
	}
	defer rows.Close()

	var triggers []common.TriggerInfo
	for rows.Next() {
		var trigger common.TriggerInfo
		if err := rows.Scan(
			&trigger.Name,
			&trigger.Schema,
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

func getSequences(pool *pgxpool.Pool) ([]common.SequenceInfo, error) {
	// Check CockroachDB version
	var version string
	err := pool.QueryRow(context.Background(), "SHOW crdb_version").Scan(&version)
	if err != nil {
		// Fallback to standard version query if crdb_version is not available
		err = pool.QueryRow(context.Background(), "SHOW server_version").Scan(&version)
		if err != nil {
			return nil, fmt.Errorf("error fetching CockroachDB version: %v", err)
		}
	}

	// CockroachDB supports sequences similar to PostgreSQL
	query := `
        SELECT 
            n.nspname AS schema,
            s.relname AS name,
            format_type(s.reltype, NULL) AS data_type,
            seq.start_value AS seqstart,
            seq.increment_by AS seqincrement,
            seq.max_value AS seqmax,
            seq.min_value AS seqmin,
            seq.cache_size AS seqcache,
            seq.cycle AS seqcycle
        FROM pg_class s
        JOIN pg_namespace n ON n.oid = s.relnamespace
        JOIN pg_sequences seq ON seq.sequencename = s.relname AND seq.schemaname = n.nspname
        WHERE s.relkind = 'S'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')
        ORDER BY n.nspname, s.relname
    `

	var sequences []common.SequenceInfo

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		// If the query fails, try a simpler fallback query
		fallbackQuery := `
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
            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')
            ORDER BY n.nspname, s.relname
        `
		rows, err = pool.Query(context.Background(), fallbackQuery)
		if err != nil {
			return nil, fmt.Errorf("error querying sequences: %v", err)
		}
	}
	defer rows.Close()

	for rows.Next() {
		var seq common.SequenceInfo
		var startValue, incrementBy, maxValue, minValue, cacheSize sql.NullInt64
		var isCycled sql.NullBool

		err = rows.Scan(
			&seq.Schema,
			&seq.Name,
			&seq.DataType,
			&startValue,
			&incrementBy,
			&maxValue,
			&minValue,
			&cacheSize,
			&isCycled,
		)

		if err != nil {
			return nil, fmt.Errorf("error scanning sequence row: %v", err)
		}

		// Set values only if they are not NULL
		if startValue.Valid {
			seq.Start = startValue.Int64
		}
		if incrementBy.Valid {
			seq.Increment = incrementBy.Int64
		}
		if maxValue.Valid {
			seq.MaxValue = maxValue.Int64
		}
		if minValue.Valid {
			seq.MinValue = minValue.Int64
		}
		if cacheSize.Valid {
			seq.CacheSize = cacheSize.Int64
		}
		if isCycled.Valid {
			seq.Cycle = isCycled.Bool
		}

		sequences = append(sequences, seq)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sequence rows: %v", err)
	}

	return sequences, nil
}

func getExtensions(pool *pgxpool.Pool) ([]common.ExtensionInfo, error) {
	// CockroachDB has limited support for extensions compared to PostgreSQL
	// This query might return fewer results
	query := `
		SELECT 
			extname as extension_name,
			extversion as version,
			n.nspname as schema_name,
			NULL as tables,
			pg_catalog.obj_description(e.oid, 'pg_extension') as description
		FROM pg_extension e
		JOIN pg_namespace n ON e.extnamespace = n.oid
		ORDER BY extname;`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		// If the query fails, return an empty slice instead of an error
		// since CockroachDB might not support all extension functionality
		return []common.ExtensionInfo{}, nil
	}
	defer rows.Close()

	var extensions []common.ExtensionInfo
	for rows.Next() {
		var ext common.ExtensionInfo
		var tables []string
		var description sql.NullString
		if err := rows.Scan(
			&ext.Name,
			&ext.Version,
			&ext.Schema,
			&tables,
			&description,
		); err != nil {
			return nil, fmt.Errorf("error scanning extension: %v", err)
		}
		ext.Tables = tables
		if description.Valid {
			ext.Description = description.String
		}
		extensions = append(extensions, ext)
	}

	return extensions, nil
}

func CreateTable(tx pgx.Tx, tableInfo common.TableInfo, enumTypes []common.EnumInfo) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	if tableInfo.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Check if the table already exists
	var exists bool
	err := tx.QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)", tableInfo.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists {
		return fmt.Errorf("table '%s' already exists", tableInfo.Name)
	}

	// Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", tableInfo.Name)
	for i, column := range tableInfo.Columns {
		if i > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s ", column.Name)

		isSerial := column.ColumnDefault != nil && strings.Contains(*column.ColumnDefault, "nextval")

		if isSerial {
			// CockroachDB uses SERIAL type differently than PostgreSQL
			createTableSQL += "INT"
		} else {
			switch strings.ToUpper(column.DataType) {
			case "USER-DEFINED":
				if column.CustomTypeName == nil || *column.CustomTypeName == "" {
					return fmt.Errorf("custom type name not specified for column %s", column.Name)
				}
				createTableSQL += *column.CustomTypeName
			case "ARRAY":
				if column.ArrayElementType == nil {
					return fmt.Errorf("array element type not specified for column %s", column.Name)
				}
				if strings.ToUpper(*column.ArrayElementType) == "USER-DEFINED" {
					if column.CustomTypeName == nil || *column.CustomTypeName == "" {
						return fmt.Errorf("custom type name not specified for array column %s", column.Name)
					}
					// Remove the leading underscore from CustomTypeName for array types
					elementType := strings.TrimPrefix(*column.CustomTypeName, "_")
					createTableSQL += elementType + "[]"
				} else {
					createTableSQL += fmt.Sprintf("%s[]", *column.ArrayElementType)
				}
			case "CHARACTER VARYING":
				if column.VarcharLength == nil {
					createTableSQL += column.DataType
				} else {
					createTableSQL += fmt.Sprintf("VARCHAR(%d)", *column.VarcharLength)
				}
			case "TIMESTAMP WITHOUT TIME ZONE":
				// CockroachDB uses TIMESTAMP instead of TIMESTAMP WITHOUT TIME ZONE
				createTableSQL += "TIMESTAMP"
			case "TIMESTAMP WITH TIME ZONE":
				// CockroachDB uses TIMESTAMPTZ instead of TIMESTAMP WITH TIME ZONE
				createTableSQL += "TIMESTAMPTZ"
			default:
				createTableSQL += column.DataType
			}
		}

		if !column.IsNullable {
			createTableSQL += " NOT NULL"
		}
		if column.ColumnDefault != nil {
			// Handle serial types in CockroachDB
			if isSerial {
				createTableSQL += " DEFAULT unique_rowid()"
			} else {
				createTableSQL += fmt.Sprintf(" DEFAULT %s", *column.ColumnDefault)
			}
		}
	}
	createTableSQL += ")"

	// Print the SQL statement
	fmt.Printf("Creating table %s with SQL: %s\n", tableInfo.Name, createTableSQL)

	_, err = tx.Exec(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Add primary key
	if len(tableInfo.PrimaryKey) > 0 {
		primaryKeySQL := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_pkey PRIMARY KEY (%s)",
			tableInfo.Name, tableInfo.Name, strings.Join(tableInfo.PrimaryKey, ", "))
		_, err = tx.Exec(context.Background(), primaryKeySQL)
		if err != nil {
			return fmt.Errorf("error adding primary key: %v", err)
		}
	}

	// Add indexes
	for _, index := range tableInfo.Indexes {
		// Skip primary key indexes
		if index.Name == tableInfo.Name+"_pkey" {
			continue
		}
		indexSQL := "CREATE"
		if index.IsUnique {
			indexSQL += " UNIQUE"
		}
		indexSQL += fmt.Sprintf(" INDEX %s ON %s (%s)",
			index.Name, tableInfo.Name, strings.Join(index.Columns, ", "))
		_, err = tx.Exec(context.Background(), indexSQL)
		if err != nil {
			return fmt.Errorf("error creating index %s: %v", index.Name, err)
		}
	}

	return nil
}

func AddTableConstraints(tx pgx.Tx, tableInfo common.TableInfo) error {
	addedConstraints := make(map[string]bool)
	for _, constraint := range tableInfo.Constraints {
		// Skip if constraint has already been added
		if addedConstraints[constraint.Name] {
			continue
		}

		var constraintSQL string
		switch constraint.Type {
		case "FOREIGN KEY":
			if constraint.Definition == "" {
				fmt.Printf("Warning: Skipping empty foreign key constraint definition %s for table %s\n", constraint.Name, tableInfo.Name)
				continue
			}
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
				tableInfo.Name, constraint.Name, constraint.Definition)
		case "CHECK":
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
				tableInfo.Name, constraint.Name, constraint.Definition)
		case "UNIQUE":
			constraintSQL = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
				tableInfo.Name, constraint.Name, constraint.Column)
		case "PRIMARY KEY":
			// Skip primary key constraints as they are handled separately during table creation
			continue
		case "":
			// Skip constraints with empty type
			fmt.Printf("Warning: Skipping constraint with empty type for table %s\n", tableInfo.Name)
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

func createEnumType(tx pgx.Tx, enumName string, enumValues []string) error {
	enumSQL := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s)",
		enumName,
		strings.Join(common.QuoteStringSlice(enumValues), ", "))
	_, err := tx.Exec(context.Background(), enumSQL)
	if err != nil {
		return fmt.Errorf("error creating enum type %s: %v", enumName, err)
	}
	return nil
}

// GetTableConstraints retrieves all constraints for a table
func GetTableConstraints(pool *pgxpool.Pool, tableName string) ([]common.Constraint, error) {
	query := `
        SELECT 
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table,
        ccu.column_name AS foreign_column,
        rc.update_rule AS on_update,
        rc.delete_rule AS on_delete,
        pgc.consrc AS definition
    FROM 
        information_schema.table_constraints tc
    LEFT JOIN 
        information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    LEFT JOIN 
        information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
    LEFT JOIN 
        information_schema.referential_constraints rc
        ON tc.constraint_name = rc.constraint_name
    LEFT JOIN 
        pg_constraint pgc
        ON tc.constraint_name = pgc.conname
    WHERE 
        tc.table_name = $1
        AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')
    ORDER BY 
        tc.constraint_name, kcu.ordinal_position;
    `

	rows, err := pool.Query(context.Background(), query, tableName)
	if err != nil {
		// CockroachDB might not support all constraint-related queries
		// Try a simpler fallback query
		fallbackQuery := `
        SELECT 
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column,
            NULL AS on_update,
            NULL AS on_delete,
            NULL AS definition
        FROM 
            information_schema.table_constraints tc
        LEFT JOIN 
            information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN 
            information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE 
            tc.table_name = $1
            AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')
        ORDER BY 
            tc.constraint_name, kcu.ordinal_position;
        `
		rows, err = pool.Query(context.Background(), fallbackQuery, tableName)
		if err != nil {
			return nil, fmt.Errorf("error querying table constraints: %v", err)
		}
	}
	defer rows.Close()

	var constraints []common.Constraint
	for rows.Next() {
		var constraint common.Constraint
		var constraintType, onUpdate, onDelete, definition sql.NullString
		var foreignTable, foreignColumn sql.NullString

		err := rows.Scan(
			&constraint.Name,
			&constraintType,
			&constraint.Column,
			&foreignTable,
			&foreignColumn,
			&onUpdate,
			&onDelete,
			&definition,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning constraint row: %v", err)
		}

		constraint.Table = tableName
		if constraintType.Valid {
			constraint.Type = constraintType.String
		}
		if foreignTable.Valid {
			constraint.ForeignTable = foreignTable.String
		}
		if foreignColumn.Valid {
			constraint.ForeignColumn = foreignColumn.String
		}
		if onUpdate.Valid {
			constraint.OnUpdate = onUpdate.String
		}
		if onDelete.Valid {
			constraint.OnDelete = onDelete.String
		}
		if definition.Valid {
			constraint.Definition = definition.String
		}

		// For foreign keys, create a ForeignKeyInfo
		if constraint.Type == "FOREIGN KEY" {
			constraint.ForeignKey = &common.ForeignKeyInfo{
				Table:    constraint.ForeignTable,
				Column:   constraint.ForeignColumn,
				OnUpdate: constraint.OnUpdate,
				OnDelete: constraint.OnDelete,
			}
		}

		constraints = append(constraints, constraint)
	}

	return constraints, nil
}

// GetTableIndexes retrieves all indexes for a table
func GetTableIndexes(pool *pgxpool.Pool, tableName string) ([]common.IndexInfo, error) {
	query := `
    SELECT 
        i.relname as index_name,
        array_agg(a.attname) as column_names,
        ix.indisunique as is_unique
    FROM 
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a
    WHERE 
        t.oid = ix.indrelid
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relname = $1
    GROUP BY 
        i.relname, ix.indisunique
    ORDER BY 
        i.relname;
    `

	rows, err := pool.Query(context.Background(), query, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying table indexes: %v", err)
	}
	defer rows.Close()

	var indexes []common.IndexInfo
	for rows.Next() {
		var indexName string
		var columnNames []string
		var isUnique bool

		if err := rows.Scan(&indexName, &columnNames, &isUnique); err != nil {
			return nil, fmt.Errorf("error scanning index row: %v", err)
		}

		indexes = append(indexes, common.IndexInfo{
			Name:     indexName,
			Columns:  columnNames,
			IsUnique: isUnique,
		})
	}

	return indexes, nil
}

// GetTableSize retrieves the estimated size of a table
func GetTableSize(pool *pgxpool.Pool, tableName string) (int64, error) {
	// CockroachDB has a different way to get table size compared to PostgreSQL
	query := `
    SELECT 
        sum(range_size_bytes) as table_size
    FROM 
        crdb_internal.ranges_no_leases
    WHERE 
        table_name = $1;
    `

	var size int64
	err := pool.QueryRow(context.Background(), query, tableName).Scan(&size)
	if err != nil {
		// Fallback to PostgreSQL compatible method
		fallbackQuery := `
        SELECT 
            pg_total_relation_size($1) as table_size;
        `
		err = pool.QueryRow(context.Background(), fallbackQuery, tableName).Scan(&size)
		if err != nil {
			return 0, fmt.Errorf("error getting table size: %v", err)
		}
	}

	return size, nil
}

// GetTableRowCount retrieves the estimated row count of a table
func GetTableRowCount(pool *pgxpool.Pool, tableName string) (int64, error) {
	query := `
    SELECT 
        count(*) as row_count
    FROM 
        ` + tableName + `;
    `

	var count int64
	err := pool.QueryRow(context.Background(), query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error getting table row count: %v", err)
	}

	return count, nil
}

// GetDatabaseSize retrieves the total size of the database
func GetDatabaseSize(pool *pgxpool.Pool) (int64, error) {
	query := `
    SELECT 
        pg_database_size(current_database()) as db_size;
    `

	var size int64
	err := pool.QueryRow(context.Background(), query).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("error getting database size: %v", err)
	}

	return size, nil
}

// GetTableWithSizes retrieves all tables with their estimated sizes
func GetTablesWithSizes(pool *pgxpool.Pool) ([]common.TableInfoWithSize, error) {
	// First get all tables
	tablesMap, _, err := discoverTablesAndColumns(pool)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	var tablesWithSize []common.TableInfoWithSize
	for _, table := range tablesMap {
		// Get table size
		size, err := GetTableSize(pool, table.Name)
		if err != nil {
			// If we can't get the size, just use 0
			size = 0
		}

		// Get row count
		rowCount, err := GetTableRowCount(pool, table.Name)
		if err != nil {
			// If we can't get the row count, just use 0
			rowCount = 0
		}

		tableWithSize := common.TableInfoWithSize{
			Schema:        table.Schema,
			Name:          table.Name,
			Columns:       table.Columns,
			PrimaryKey:    table.PrimaryKey,
			Indexes:       table.Indexes,
			Constraints:   table.Constraints,
			EstimatedSize: size,
			EstimatedRows: rowCount,
		}

		tablesWithSize = append(tablesWithSize, tableWithSize)
	}

	return tablesWithSize, nil
}
