package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of an Oracle database
func DiscoverSchema(db *sql.DB) (*OracleSchema, error) {
	schema := &OracleSchema{}
	var err error

	// Get tables and their columns
	tablesMap, _, err := discoverTablesAndColumns(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering tables: %v", err)
	}

	// Convert map to slice
	tables := make([]common.TableInfo, 0, len(tablesMap))
	for _, table := range tablesMap {
		tables = append(tables, *table)
	}
	schema.Tables = tables

	// Get user-defined types
	schema.Types, err = discoverTypes(db)
	if err != nil {
		return nil, fmt.Errorf("error discovering types: %v", err)
	}

	// Get schemas (users in Oracle)
	schema.Schemas, err = getSchemas(db)
	if err != nil {
		return nil, fmt.Errorf("error getting schemas: %v", err)
	}

	// Get functions
	schema.Functions, err = getFunctions(db)
	if err != nil {
		return nil, fmt.Errorf("error getting functions: %v", err)
	}

	// Get procedures
	schema.Procedures, err = getProcedures(db)
	if err != nil {
		return nil, fmt.Errorf("error getting procedures: %v", err)
	}

	// Get triggers
	schema.Triggers, err = getTriggers(db)
	if err != nil {
		return nil, fmt.Errorf("error getting triggers: %v", err)
	}

	// Get sequences
	schema.Sequences, err = getSequences(db)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get packages
	schema.Packages, err = getPackages(db)
	if err != nil {
		return nil, fmt.Errorf("error getting packages: %v", err)
	}

	return schema, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db *sql.DB, params common.StructureParams) error {
	// Start a transaction
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Create user-defined types first
	for _, typeInfo := range params.Types {
		if err := createType(tx, typeInfo); err != nil {
			return fmt.Errorf("error creating type %s: %v", typeInfo.Name, err)
		}
	}

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
	// Query to get tables
	tableQuery := `
		SELECT 
			owner AS schema_name,
			table_name
		FROM 
			all_tables
		WHERE 
			owner = USER
		ORDER BY 
			table_name
	`

	tableRows, err := db.Query(tableQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("error fetching tables: %v", err)
	}
	defer tableRows.Close()

	tables := make(map[string]*common.TableInfo)
	tableNames := make([]string, 0)

	for tableRows.Next() {
		var schemaName, tableName string
		if err := tableRows.Scan(&schemaName, &tableName); err != nil {
			return nil, nil, fmt.Errorf("error scanning table row: %v", err)
		}

		tables[tableName] = &common.TableInfo{
			Name:   tableName,
			Schema: schemaName,
			// Oracle doesn't have the same table types as PostgreSQL
			TableType: "oracle.standard",
		}
		tableNames = append(tableNames, tableName)
	}

	if err := tableRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating table rows: %v", err)
	}

	// Query to get columns for each table
	for tableName, tableInfo := range tables {
		columnQuery := `
			SELECT 
				column_name,
				data_type,
				CASE WHEN nullable = 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
				data_default,
				data_length,
				data_precision,
				data_scale,
				CASE WHEN identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity
			FROM 
				all_tab_columns
			WHERE 
				owner = USER AND
				table_name = :1
			ORDER BY 
				column_id
		`

		columnRows, err := db.Query(columnQuery, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching columns for table %s: %v", tableName, err)
		}

		for columnRows.Next() {
			var columnName, dataType, isNullable string
			var dataDefault, dataPrecision, dataScale sql.NullString
			var dataLength int
			var isIdentity int

			if err := columnRows.Scan(
				&columnName,
				&dataType,
				&isNullable,
				&dataDefault,
				&dataLength,
				&dataPrecision,
				&dataScale,
				&isIdentity,
			); err != nil {
				columnRows.Close()
				return nil, nil, fmt.Errorf("error scanning column row: %v", err)
			}

			var defaultValue *string
			if dataDefault.Valid {
				defaultValue = &dataDefault.String
			}

			columnInfo := common.ColumnInfo{
				Name:            columnName,
				DataType:        dataType,
				IsNullable:      isNullable == "YES",
				ColumnDefault:   defaultValue,
				IsAutoIncrement: isIdentity == 1,
			}

			// Handle VARCHAR2 length
			if dataType == "VARCHAR2" || dataType == "CHAR" {
				columnInfo.VarcharLength = &dataLength
			}

			// Handle NUMBER precision and scale
			if dataType == "NUMBER" && dataPrecision.Valid {
				precision, _ := dataPrecision.String, dataScale.String
				columnInfo.NumericPrecision = &precision
				if dataScale.Valid {
					scale := dataScale.String
					columnInfo.NumericScale = &scale
				}
			}

			tableInfo.Columns = append(tableInfo.Columns, columnInfo)
		}
		columnRows.Close()

		if err := columnRows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating column rows: %v", err)
		}

		// Get primary key information
		pkQuery := `
			SELECT 
				cols.column_name
			FROM 
				all_constraints cons
			JOIN 
				all_cons_columns cols ON cons.constraint_name = cols.constraint_name
			WHERE 
				cons.owner = USER AND
				cons.table_name = :1 AND
				cons.constraint_type = 'P'
			ORDER BY 
				cols.position
		`

		pkRows, err := db.Query(pkQuery, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching primary key for table %s: %v", tableName, err)
		}

		var primaryKeyColumns []string
		for pkRows.Next() {
			var columnName string
			if err := pkRows.Scan(&columnName); err != nil {
				pkRows.Close()
				return nil, nil, fmt.Errorf("error scanning primary key row: %v", err)
			}
			primaryKeyColumns = append(primaryKeyColumns, columnName)

			// Mark the column as primary key
			for i := range tableInfo.Columns {
				if tableInfo.Columns[i].Name == columnName {
					tableInfo.Columns[i].IsPrimaryKey = true
					break
				}
			}
		}
		pkRows.Close()

		if err := pkRows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating primary key rows: %v", err)
		}

		tableInfo.PrimaryKey = primaryKeyColumns

		// Get unique constraints
		uniqueQuery := `
			SELECT 
				cols.column_name,
				cons.constraint_name
			FROM 
				all_constraints cons
			JOIN 
				all_cons_columns cols ON cons.constraint_name = cols.constraint_name
			WHERE 
				cons.owner = USER AND
				cons.table_name = :1 AND
				cons.constraint_type = 'U'
			ORDER BY 
				cons.constraint_name, cols.position
		`

		uniqueRows, err := db.Query(uniqueQuery, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching unique constraints for table %s: %v", tableName, err)
		}

		uniqueConstraints := make(map[string][]string)
		for uniqueRows.Next() {
			var columnName, constraintName string
			if err := uniqueRows.Scan(&columnName, &constraintName); err != nil {
				uniqueRows.Close()
				return nil, nil, fmt.Errorf("error scanning unique constraint row: %v", err)
			}
			uniqueConstraints[constraintName] = append(uniqueConstraints[constraintName], columnName)

			// Mark the column as unique
			for i := range tableInfo.Columns {
				if tableInfo.Columns[i].Name == columnName {
					tableInfo.Columns[i].IsUnique = true
					break
				}
			}
		}
		uniqueRows.Close()

		if err := uniqueRows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating unique constraint rows: %v", err)
		}

		// Add unique constraints to table info
		for constraintName, columns := range uniqueConstraints {
			tableInfo.Indexes = append(tableInfo.Indexes, common.IndexInfo{
				Name:     constraintName,
				Columns:  columns,
				IsUnique: true,
			})
		}

		// Get foreign key constraints
		fkQuery := `
			SELECT 
				cons.constraint_name,
				cols.column_name,
				r_cons.table_name AS referenced_table,
				r_cols.column_name AS referenced_column
			FROM 
				all_constraints cons
			JOIN 
				all_cons_columns cols ON cons.constraint_name = cols.constraint_name
			JOIN 
				all_constraints r_cons ON cons.r_constraint_name = r_cons.constraint_name
			JOIN 
				all_cons_columns r_cols ON r_cons.constraint_name = r_cols.constraint_name
			WHERE 
				cons.owner = USER AND
				cons.table_name = :1 AND
				cons.constraint_type = 'R'
			ORDER BY 
				cons.constraint_name, cols.position
		`

		fkRows, err := db.Query(fkQuery, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching foreign keys for table %s: %v", tableName, err)
		}

		fkConstraints := make(map[string]common.Constraint)
		for fkRows.Next() {
			var constraintName, columnName, referencedTable, referencedColumn string
			if err := fkRows.Scan(&constraintName, &columnName, &referencedTable, &referencedColumn); err != nil {
				fkRows.Close()
				return nil, nil, fmt.Errorf("error scanning foreign key row: %v", err)
			}

			if _, exists := fkConstraints[constraintName]; !exists {
				fkConstraints[constraintName] = common.Constraint{
					Name:            constraintName,
					Type:            "FOREIGN KEY",
					Column:          columnName,
					ReferencedTable: referencedTable,
					Definition:      fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)", columnName, referencedTable, referencedColumn),
				}
			} else {
				// For multi-column foreign keys, append to the definition
				constraint := fkConstraints[constraintName]
				constraint.Definition = fmt.Sprintf("FOREIGN KEY (%s, %s) REFERENCES %s(%s, %s)",
					constraint.Column, columnName, constraint.ReferencedTable, constraint.ReferencedColumn, referencedColumn)
				constraint.Column = fmt.Sprintf("%s, %s", constraint.Column, columnName)
				constraint.ReferencedColumn = fmt.Sprintf("%s, %s", constraint.ReferencedColumn, referencedColumn)
				fkConstraints[constraintName] = constraint
			}
		}
		fkRows.Close()

		if err := fkRows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating foreign key rows: %v", err)
		}

		// Add foreign key constraints to table info
		for _, constraint := range fkConstraints {
			tableInfo.Constraints = append(tableInfo.Constraints, constraint)
		}

		// Get check constraints
		checkQuery := `
			SELECT 
				constraint_name,
				search_condition
			FROM 
				all_constraints
			WHERE 
				owner = USER AND
				table_name = :1 AND
				constraint_type = 'C' AND
				constraint_name NOT LIKE 'SYS%'
			ORDER BY 
				constraint_name
		`

		checkRows, err := db.Query(checkQuery, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching check constraints for table %s: %v", tableName, err)
		}

		for checkRows.Next() {
			var constraintName, searchCondition string
			if err := checkRows.Scan(&constraintName, &searchCondition); err != nil {
				checkRows.Close()
				return nil, nil, fmt.Errorf("error scanning check constraint row: %v", err)
			}

			tableInfo.Constraints = append(tableInfo.Constraints, common.Constraint{
				Name:       constraintName,
				Type:       "CHECK",
				Definition: fmt.Sprintf("CHECK (%s)", searchCondition),
			})
		}
		checkRows.Close()

		if err := checkRows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating check constraint rows: %v", err)
		}

		// Get indexes
		indexQuery := `
			SELECT 
				index_name,
				column_name,
				CASE WHEN uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique
			FROM 
				all_indexes i
			JOIN 
				all_ind_columns c ON i.index_name = c.index_name
			WHERE 
				i.owner = USER AND
				i.table_name = :1 AND
				i.index_type != 'LOB' AND
				i.index_name NOT LIKE 'SYS%'
			ORDER BY 
				i.index_name, c.column_position
		`

		indexRows, err := db.Query(indexQuery, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching indexes for table %s: %v", tableName, err)
		}

		indexMap := make(map[string]common.IndexInfo)
		for indexRows.Next() {
			var indexName, columnName string
			var isUnique int
			if err := indexRows.Scan(&indexName, &columnName, &isUnique); err != nil {
				indexRows.Close()
				return nil, nil, fmt.Errorf("error scanning index row: %v", err)
			}

			if _, exists := indexMap[indexName]; !exists {
				indexMap[indexName] = common.IndexInfo{
					Name:     indexName,
					Columns:  []string{columnName},
					IsUnique: isUnique == 1,
				}
			} else {
				index := indexMap[indexName]
				index.Columns = append(index.Columns, columnName)
				indexMap[indexName] = index
			}
		}
		indexRows.Close()

		if err := indexRows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating index rows: %v", err)
		}

		// Add indexes to table info (excluding those already added as unique constraints)
		for indexName, index := range indexMap {
			// Skip if this index is already added as a unique constraint
			alreadyAdded := false
			for _, existingIndex := range tableInfo.Indexes {
				if existingIndex.Name == indexName {
					alreadyAdded = true
					break
				}
			}
			if !alreadyAdded {
				tableInfo.Indexes = append(tableInfo.Indexes, index)
			}
		}

		// Check if table is partitioned
		partitionQuery := `
			SELECT 
				partitioning_type,
				partition_count
			FROM 
				all_part_tables
			WHERE 
				owner = USER AND
				table_name = :1
		`

		var partitioningType string
		var partitionCount int
		err = db.QueryRow(partitionQuery, tableName).Scan(&partitioningType, &partitionCount)
		if err == nil {
			// Table is partitioned
			tableInfo.TableType = "oracle.partitioned"
			tableInfo.PartitionStrategy = partitioningType

			// Get partition key columns
			partKeyQuery := `
				SELECT 
					column_name
				FROM 
					all_part_key_columns
				WHERE 
					owner = USER AND
					name = :1
				ORDER BY 
					column_position
			`

			partKeyRows, err := db.Query(partKeyQuery, tableName)
			if err != nil {
				return nil, nil, fmt.Errorf("error fetching partition key for table %s: %v", tableName, err)
			}

			var partitionKey []string
			for partKeyRows.Next() {
				var columnName string
				if err := partKeyRows.Scan(&columnName); err != nil {
					partKeyRows.Close()
					return nil, nil, fmt.Errorf("error scanning partition key row: %v", err)
				}
				partitionKey = append(partitionKey, columnName)
			}
			partKeyRows.Close()

			if err := partKeyRows.Err(); err != nil {
				return nil, nil, fmt.Errorf("error iterating partition key rows: %v", err)
			}

			tableInfo.PartitionKey = partitionKey

			// Get partition names
			partNamesQuery := `
				SELECT 
					partition_name
				FROM 
					all_tab_partitions
				WHERE 
					table_owner = USER AND
					table_name = :1
				ORDER BY 
					partition_position
			`

			partNamesRows, err := db.Query(partNamesQuery, tableName)
			if err != nil {
				return nil, nil, fmt.Errorf("error fetching partition names for table %s: %v", tableName, err)
			}

			var partitions []string
			for partNamesRows.Next() {
				var partitionName string
				if err := partNamesRows.Scan(&partitionName); err != nil {
					partNamesRows.Close()
					return nil, nil, fmt.Errorf("error scanning partition name row: %v", err)
				}
				partitions = append(partitions, partitionName)
			}
			partNamesRows.Close()

			if err := partNamesRows.Err(); err != nil {
				return nil, nil, fmt.Errorf("error iterating partition name rows: %v", err)
			}

			tableInfo.Partitions = partitions
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("error checking if table %s is partitioned: %v", tableName, err)
		}
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func discoverTypes(db *sql.DB) ([]common.TypeInfo, error) {
	query := `
		SELECT 
			type_name,
			typecode
		FROM 
			all_types
		WHERE 
			owner = USER
		ORDER BY 
			type_name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error fetching types: %v", err)
	}
	defer rows.Close()

	var types []common.TypeInfo
	for rows.Next() {
		var typeName, typeCode string
		if err := rows.Scan(&typeName, &typeCode); err != nil {
			return nil, fmt.Errorf("error scanning type row: %v", err)
		}

		typeInfo := common.TypeInfo{
			Name:     typeName,
			TypeCode: typeCode,
		}

		// For object types, get attributes
		if typeCode == "OBJECT" {
			attrQuery := `
				SELECT 
					attr_name,
					attr_type_name
				FROM 
					all_type_attrs
				WHERE 
					owner = USER AND
					type_name = :1
				ORDER BY 
					attr_no
			`

			attrRows, err := db.Query(attrQuery, typeName)
			if err != nil {
				return nil, fmt.Errorf("error fetching type attributes: %v", err)
			}

			var attributes []common.TypeAttribute
			for attrRows.Next() {
				var attrName, attrTypeName string
				if err := attrRows.Scan(&attrName, &attrTypeName); err != nil {
					attrRows.Close()
					return nil, fmt.Errorf("error scanning type attribute row: %v", err)
				}
				attributes = append(attributes, common.TypeAttribute{
					Name:     attrName,
					DataType: attrTypeName,
				})
			}
			attrRows.Close()

			if err := attrRows.Err(); err != nil {
				return nil, fmt.Errorf("error iterating type attribute rows: %v", err)
			}

			typeInfo.Attributes = attributes
		}

		types = append(types, typeInfo)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating type rows: %v", err)
	}

	return types, nil
}

func getSchemas(db *sql.DB) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT 
			username AS schema_name,
			created
		FROM 
			all_users
		WHERE 
			username NOT LIKE 'SYS%' AND
			username NOT LIKE 'APEX%' AND
			username NOT LIKE 'DBSNMP' AND
			username NOT LIKE 'OUTLN' AND
			username NOT LIKE 'SYSTEM' AND
			username NOT LIKE 'XDB'
		ORDER BY 
			username
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	var schemas []common.DatabaseSchemaInfo
	for rows.Next() {
		var schema common.DatabaseSchemaInfo
		var created time.Time
		if err := rows.Scan(&schema.Name, &created); err != nil {
			return nil, fmt.Errorf("error scanning schema: %v", err)
		}
		schema.Description = fmt.Sprintf("Created on %s", created.Format("2006-01-02"))
		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schema rows: %v", err)
	}

	return schemas, nil
}

func getFunctions(db *sql.DB) ([]common.FunctionInfo, error) {
	query := `
		SELECT 
			owner AS schema_name,
			object_name AS function_name,
			TO_CLOB('') AS argument_data_types,
			TO_CLOB('') AS return_type,
			TO_CLOB('') AS function_body
		FROM 
			all_objects
		WHERE 
			owner = USER AND
			object_type = 'FUNCTION'
		ORDER BY 
			object_name
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

		// Get function details
		detailsQuery := `
			SELECT 
				text
			FROM 
				all_source
			WHERE 
				owner = :1 AND
				name = :2 AND
				type = 'FUNCTION'
			ORDER BY 
				line
		`

		detailsRows, err := db.Query(detailsQuery, function.Schema, function.Name)
		if err != nil {
			return nil, fmt.Errorf("error querying function details: %v", err)
		}

		var bodyBuilder strings.Builder
		for detailsRows.Next() {
			var text string
			if err := detailsRows.Scan(&text); err != nil {
				detailsRows.Close()
				return nil, fmt.Errorf("error scanning function details: %v", err)
			}
			bodyBuilder.WriteString(text)
		}
		detailsRows.Close()

		if err := detailsRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating function details rows: %v", err)
		}

		function.Body = bodyBuilder.String()

		// Extract return type and arguments from function body
		if bodyParts := strings.Split(function.Body, "RETURN"); len(bodyParts) > 1 {
			returnTypeParts := strings.Split(bodyParts[1], "IS")
			if len(returnTypeParts) > 0 {
				function.ReturnType = strings.TrimSpace(returnTypeParts[0])
			}
		}

		if bodyParts := strings.Split(function.Body, "("); len(bodyParts) > 1 {
			argsParts := strings.Split(bodyParts[1], ")")
			if len(argsParts) > 0 {
				function.Arguments = strings.TrimSpace(argsParts[0])
			}
		}

		functions = append(functions, function)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating function rows: %v", err)
	}

	return functions, nil
}

func getProcedures(db *sql.DB) ([]common.ProcedureInfo, error) {
	query := `
		SELECT 
			owner AS schema_name,
			object_name AS procedure_name,
			TO_CLOB('') AS argument_data_types,
			TO_CLOB('') AS procedure_body
		FROM 
			all_objects
		WHERE 
			owner = USER AND
			object_type = 'PROCEDURE'
		ORDER BY 
			object_name
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
			&procedure.Arguments,
			&procedure.Body,
		); err != nil {
			return nil, fmt.Errorf("error scanning procedure: %v", err)
		}

		// Get procedure details
		detailsQuery := `
			SELECT 
				text
			FROM 
				all_source
			WHERE 
				owner = :1 AND
				name = :2 AND
				type = 'PROCEDURE'
			ORDER BY 
				line
		`

		detailsRows, err := db.Query(detailsQuery, procedure.Schema, procedure.Name)
		if err != nil {
			return nil, fmt.Errorf("error querying procedure details: %v", err)
		}

		var bodyBuilder strings.Builder
		for detailsRows.Next() {
			var text string
			if err := detailsRows.Scan(&text); err != nil {
				detailsRows.Close()
				return nil, fmt.Errorf("error scanning procedure details: %v", err)
			}
			bodyBuilder.WriteString(text)
		}
		detailsRows.Close()

		if err := detailsRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating procedure details rows: %v", err)
		}

		procedure.Body = bodyBuilder.String()

		// Extract arguments from procedure body
		if bodyParts := strings.Split(procedure.Body, "("); len(bodyParts) > 1 {
			argsParts := strings.Split(bodyParts[1], ")")
			if len(argsParts) > 0 {
				procedure.Arguments = strings.TrimSpace(argsParts[0])
			}
		}

		procedures = append(procedures, procedure)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating procedure rows: %v", err)
	}

	return procedures, nil
}

func getTriggers(db *sql.DB) ([]common.TriggerInfo, error) {
	query := `
		SELECT 
			owner AS schema_name,
			trigger_name,
			table_owner || '.' || table_name AS table_name,
			trigger_type,
			triggering_event,
			trigger_body
		FROM 
			all_triggers
		WHERE 
			owner = USER
		ORDER BY 
			trigger_name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	var triggers []common.TriggerInfo
	for rows.Next() {
		var trigger common.TriggerInfo
		var triggerType, triggeringEvent, triggerBody string
		if err := rows.Scan(
			&trigger.Schema,
			&trigger.Name,
			&trigger.Table,
			&triggerType,
			&triggeringEvent,
			&triggerBody,
		); err != nil {
			return nil, fmt.Errorf("error scanning trigger: %v", err)
		}

		// Parse trigger type to get timing
		if strings.Contains(triggerType, "BEFORE") {
			trigger.Timing = "BEFORE"
		} else if strings.Contains(triggerType, "AFTER") {
			trigger.Timing = "AFTER"
		} else if strings.Contains(triggerType, "INSTEAD OF") {
			trigger.Timing = "INSTEAD OF"
		} else {
			trigger.Timing = "UNKNOWN"
		}

		// Parse triggering event
		if strings.Contains(triggeringEvent, "INSERT") {
			trigger.Event = "INSERT"
		} else if strings.Contains(triggeringEvent, "UPDATE") {
			trigger.Event = "UPDATE"
		} else if strings.Contains(triggeringEvent, "DELETE") {
			trigger.Event = "DELETE"
		} else {
			trigger.Event = triggeringEvent
		}

		// Construct the full trigger statement
		trigger.Statement = fmt.Sprintf("CREATE OR REPLACE TRIGGER %s\n%s %s ON %s\n%s",
			trigger.Name, trigger.Timing, trigger.Event, trigger.Table, triggerBody)

		triggers = append(triggers, trigger)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trigger rows: %v", err)
	}

	return triggers, nil
}

func getSequences(db *sql.DB) ([]common.SequenceInfo, error) {
	query := `
		SELECT 
			sequence_owner AS schema,
			sequence_name AS name,
			'NUMBER' AS data_type,
			min_value,
			max_value,
			increment_by,
			cycle_flag,
			cache_size,
			last_number
		FROM 
			all_sequences
		WHERE 
			sequence_owner = USER
		ORDER BY 
			sequence_name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	var sequences []common.SequenceInfo
	for rows.Next() {
		var seq common.SequenceInfo
		var minValue, maxValue, incrementBy, cacheSize, lastNumber int64
		var cycleFlag string

		if err := rows.Scan(
			&seq.Schema,
			&seq.Name,
			&seq.DataType,
			&minValue,
			&maxValue,
			&incrementBy,
			&cycleFlag,
			&cacheSize,
			&lastNumber,
		); err != nil {
			return nil, fmt.Errorf("error scanning sequence: %v", err)
		}

		seq.MinValue = minValue
		seq.MaxValue = maxValue
		seq.Increment = incrementBy
		seq.CacheSize = cacheSize
		seq.Start = lastNumber
		seq.Cycle = cycleFlag == "Y"

		sequences = append(sequences, seq)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sequence rows: %v", err)
	}

	return sequences, nil
}

func getPackages(db *sql.DB) ([]common.PackageInfo, error) {
	query := `
		SELECT 
			owner AS schema_name,
			object_name AS package_name
		FROM 
			all_objects
		WHERE 
			owner = USER AND
			object_type = 'PACKAGE'
		ORDER BY 
			object_name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying packages: %v", err)
	}
	defer rows.Close()

	var packages []common.PackageInfo
	for rows.Next() {
		var pkg common.PackageInfo
		if err := rows.Scan(
			&pkg.Schema,
			&pkg.Name,
		); err != nil {
			return nil, fmt.Errorf("error scanning package: %v", err)
		}

		// Get package specification
		specQuery := `
			SELECT 
				text
			FROM 
				all_source
			WHERE 
				owner = :1 AND
				name = :2 AND
				type = 'PACKAGE'
			ORDER BY 
				line
		`

		specRows, err := db.Query(specQuery, pkg.Schema, pkg.Name)
		if err != nil {
			return nil, fmt.Errorf("error querying package specification: %v", err)
		}

		var specBuilder strings.Builder
		for specRows.Next() {
			var text string
			if err := specRows.Scan(&text); err != nil {
				specRows.Close()
				return nil, fmt.Errorf("error scanning package specification: %v", err)
			}
			specBuilder.WriteString(text)
		}
		specRows.Close()

		if err := specRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating package specification rows: %v", err)
		}

		pkg.Specification = specBuilder.String()

		// Get package body
		bodyQuery := `
			SELECT 
				text
			FROM 
				all_source
			WHERE 
				owner = :1 AND
				name = :2 AND
				type = 'PACKAGE BODY'
			ORDER BY 
				line
		`

		bodyRows, err := db.Query(bodyQuery, pkg.Schema, pkg.Name)
		if err != nil {
			return nil, fmt.Errorf("error querying package body: %v", err)
		}

		var bodyBuilder strings.Builder
		for bodyRows.Next() {
			var text string
			if err := bodyRows.Scan(&text); err != nil {
				bodyRows.Close()
				return nil, fmt.Errorf("error scanning package body: %v", err)
			}
			bodyBuilder.WriteString(text)
		}
		bodyRows.Close()

		if err := bodyRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating package body rows: %v", err)
		}

		pkg.Body = bodyBuilder.String()

		packages = append(packages, pkg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating package rows: %v", err)
	}

	return packages, nil
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
	err := tx.QueryRow("SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(?)", tableInfo.Name).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists > 0 {
		return fmt.Errorf("table '%s' already exists", tableInfo.Name)
	}

	// Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (", tableInfo.Name)
	for i, column := range tableInfo.Columns {
		if i > 0 {
			createTableSQL += ", "
		}
		createTableSQL += fmt.Sprintf("%s ", column.Name)

		// Map PostgreSQL data types to Oracle data types
		oracleType := mapToOracleDataType(column)
		createTableSQL += oracleType

		if !column.IsNullable {
			createTableSQL += " NOT NULL"
		}
		if column.ColumnDefault != nil {
			// Oracle uses different default value syntax
			defaultValue := *column.ColumnDefault
			// Handle sequence-based defaults
			if strings.Contains(strings.ToLower(defaultValue), "nextval") {
				// For auto-increment columns in Oracle, we'll use identity columns
				if column.IsAutoIncrement {
					createTableSQL += " GENERATED ALWAYS AS IDENTITY"
				}
			} else {
				createTableSQL += fmt.Sprintf(" DEFAULT %s", defaultValue)
			}
		} else if column.IsAutoIncrement {
			createTableSQL += " GENERATED ALWAYS AS IDENTITY"
		}
	}
	createTableSQL += ")"

	// Add tablespace if specified
	if tableInfo.Tablespace != "" {
		createTableSQL += fmt.Sprintf(" TABLESPACE %s", tableInfo.Tablespace)
	}

	// Print the SQL statement
	fmt.Printf("Creating table %s with SQL: %s\n", tableInfo.Name, createTableSQL)

	_, err = tx.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Add primary key
	if len(tableInfo.PrimaryKey) > 0 {
		primaryKeySQL := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_PK PRIMARY KEY (%s)",
			tableInfo.Name, tableInfo.Name, strings.Join(tableInfo.PrimaryKey, ", "))
		_, err = tx.Exec(primaryKeySQL)
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
		indexSQL := "CREATE"
		if index.IsUnique {
			indexSQL += " UNIQUE"
		}
		indexSQL += fmt.Sprintf(" INDEX %s ON %s (%s)",
			index.Name, tableInfo.Name, strings.Join(index.Columns, ", "))
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

		var constraintSQL string
		switch constraint.Type {
		case "FOREIGN KEY":
			if constraint.Definition == "" {
				fmt.Printf("Warning: Skipping empty foreign key constraint definition %s for table %s\n", constraint.Name, tableInfo.Name)
				continue
			}
			// Oracle uses a slightly different syntax for foreign keys
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

		_, err := tx.Exec(constraintSQL)
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

func createType(tx *sql.Tx, typeInfo common.TypeInfo) error {
	var typeSQL string

	switch typeInfo.TypeCode {
	case "OBJECT":
		typeSQL = fmt.Sprintf("CREATE OR REPLACE TYPE %s AS OBJECT (", typeInfo.Name)
		for i, attr := range typeInfo.Attributes {
			if i > 0 {
				typeSQL += ", "
			}
			typeSQL += fmt.Sprintf("%s %s", attr.Name, attr.DataType)
		}
		typeSQL += ")"
	default:
		return fmt.Errorf("unsupported type code: %s", typeInfo.TypeCode)
	}

	_, err := tx.Exec(typeSQL)
	if err != nil {
		return fmt.Errorf("error creating type %s: %v", typeInfo.Name, err)
	}
	return nil
}

// mapToOracleDataType converts PostgreSQL data types to Oracle data types
func mapToOracleDataType(column common.ColumnInfo) string {
	switch strings.ToUpper(column.DataType) {
	case "INTEGER", "INT", "INT4":
		return "NUMBER(10)"
	case "BIGINT", "INT8":
		return "NUMBER(19)"
	case "SMALLINT", "INT2":
		return "NUMBER(5)"
	case "NUMERIC", "DECIMAL":
		if column.NumericPrecision != nil && column.NumericScale != nil {
			return fmt.Sprintf("NUMBER(%s,%s)", *column.NumericPrecision, *column.NumericScale)
		} else if column.NumericPrecision != nil {
			return fmt.Sprintf("NUMBER(%s)", *column.NumericPrecision)
		}
		return "NUMBER"
	case "REAL", "FLOAT4":
		return "BINARY_FLOAT"
	case "DOUBLE PRECISION", "FLOAT8":
		return "BINARY_DOUBLE"
	case "CHARACTER VARYING", "VARCHAR":
		if column.VarcharLength != nil {
			return fmt.Sprintf("VARCHAR2(%d)", *column.VarcharLength)
		}
		return "VARCHAR2(4000)"
	case "CHARACTER", "CHAR":
		if column.VarcharLength != nil {
			return fmt.Sprintf("CHAR(%d)", *column.VarcharLength)
		}
		return "CHAR(1)"
	case "TEXT":
		return "CLOB"
	case "BYTEA":
		return "BLOB"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "TIMESTAMP WITH TIME ZONE":
		return "TIMESTAMP WITH TIME ZONE"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIMESTAMP"
	case "TIME WITH TIME ZONE":
		return "TIMESTAMP WITH TIME ZONE"
	case "BOOLEAN":
		return "NUMBER(1)"
	case "JSON", "JSONB":
		return "CLOB"
	case "UUID":
		return "VARCHAR2(36)"
	case "ARRAY":
		// Oracle doesn't have direct array support, would need to use nested tables or VARRAYs
		return "CLOB"
	case "USER-DEFINED":
		if column.CustomTypeName != nil {
			return *column.CustomTypeName
		}
		return "VARCHAR2(4000)"
	default:
		return "VARCHAR2(4000)"
	}
}
