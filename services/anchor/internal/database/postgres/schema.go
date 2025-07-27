package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DiscoverSchema fetches the current schema of a PostgreSQL database
func DiscoverSchema(pool *pgxpool.Pool) (*PostgresSchema, error) {
	schema := &PostgresSchema{}
	var err error

	// Get tables and their columns
	tablesMap, _, err := discoverTablesAndColumns(pool)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Convert map to slice
	tables := make([]common.TableInfo, 0, len(tablesMap))
	for _, table := range tablesMap {
		tables = append(tables, *table)
	}
	schema.Tables = tables

	// Get enum types
	schema.EnumTypes, err = discoverEnumTypes(pool)
	if err != nil {
		return nil, fmt.Errorf("error discovering enum types: %v", err)
	}

	// Get schemas
	schema.Schemas, err = getSchemas(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Get functions
	schema.Functions, err = getFunctions(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Get triggers
	schema.Triggers, err = getTriggers(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Get sequences
	schema.Sequences, err = getSequences(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get extensions
	schema.Extensions, err = getExtensions(pool)
	if err != nil {
		return nil, fmt.Errorf("error getting extensions: %v", err)
	}

	return schema, nil
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

	for _, table := range tables {
		if table.TableType == "postgres.partitioned" {
			err := fetchPartitioningInfo(pool, table)
			if err != nil {
				return nil, nil, fmt.Errorf("error fetching partitioning info for table %s: %v", table.Name, err)
			}
		}
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func fetchPartitioningInfo(pool *pgxpool.Pool, table *common.TableInfo) error {
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
	var partitions []string

	row := pool.QueryRow(context.Background(), query, table.Name)
	err := row.Scan(&partitionKey, &partitionStrategy, &partitions)
	if err != nil {
		return fmt.Errorf("error querying partitioning info: %v", err)
	}

	if partitionStrategy.Valid {
		table.PartitionStrategy = partitionStrategy.String
	}

	if partitionKey.Valid {
		// Extract column names from partition key definition
		keyDef := partitionKey.String
		startIndex := strings.Index(keyDef, "(")
		endIndex := strings.LastIndex(keyDef, ")")
		if startIndex != -1 && endIndex != -1 && endIndex > startIndex {
			keyDef = keyDef[startIndex+1 : endIndex]
			// Split the key definition and trim each part
			keys := strings.Split(keyDef, ",")
			for i, key := range keys {
				keys[i] = strings.TrimSpace(key)
			}
			table.PartitionKey = keys
		}
	}

	table.Partitions = partitions

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
		return nil, fmt.Errorf("error querying triggers: %v", err)
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
	// First, check the PostgreSQL version
	var version string
	err := pool.QueryRow(context.Background(), "SHOW server_version_num").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching PostgreSQL version: %v", err)
	}

	versionNum, err := strconv.Atoi(version)
	if err != nil {
		return nil, fmt.Errorf("error parsing PostgreSQL version: %v", err)
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

	var sequences []common.SequenceInfo

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error querying sequences: %v", err)
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
		return nil, fmt.Errorf("error querying extensions: %v", err)
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
			createTableSQL += "INTEGER"
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
			default:
				createTableSQL += column.DataType
			}
		}

		if !column.IsNullable {
			createTableSQL += " NOT NULL"
		}
		if column.ColumnDefault != nil {
			createTableSQL += fmt.Sprintf(" DEFAULT %s", *column.ColumnDefault)
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
