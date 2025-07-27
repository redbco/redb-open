package snowflake

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of a Snowflake database
func DiscoverSchema(db *sql.DB) (*SnowflakeSchema, error) {
	schema := &SnowflakeSchema{}
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

	// Get schemas
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

	// Get sequences
	schema.Sequences, err = getSequences(db)
	if err != nil {
		return nil, fmt.Errorf("error getting sequences: %v", err)
	}

	// Get views
	schema.Views, err = getViews(db)
	if err != nil {
		return nil, fmt.Errorf("error getting views: %v", err)
	}

	// Get stages
	schema.Stages, err = getStages(db)
	if err != nil {
		return nil, fmt.Errorf("error getting stages: %v", err)
	}

	// Get warehouses
	schema.Warehouses, err = getWarehouses(db)
	if err != nil {
		return nil, fmt.Errorf("error getting warehouses: %v", err)
	}

	// Get pipes
	schema.Pipes, err = getPipes(db)
	if err != nil {
		return nil, fmt.Errorf("error getting pipes: %v", err)
	}

	return schema, nil
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
			t.TABLE_SCHEMA,
			t.TABLE_NAME,
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.IS_NULLABLE,
			c.COLUMN_DEFAULT,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE,
			c.ORDINAL_POSITION,
			CASE 
				WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES'
				ELSE 'NO'
			END as IS_PRIMARY_KEY,
			CASE 
				WHEN c.DATA_TYPE LIKE 'ARRAY%' THEN 'YES'
				ELSE 'NO'
			END as IS_ARRAY,
			CASE 
				WHEN uk.COLUMN_NAME IS NOT NULL THEN 'YES'
				ELSE 'NO'
			END as IS_UNIQUE,
			CASE 
				WHEN c.IDENTITY_GENERATION IS NOT NULL THEN 'YES'
				ELSE 'NO'
			END as IS_AUTO_INCREMENT,
			t.TABLE_TYPE
		FROM 
			INFORMATION_SCHEMA.TABLES t
		JOIN 
			INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
		LEFT JOIN 
			(SELECT k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME
			 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			 JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
				ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
				AND k.TABLE_SCHEMA = tc.TABLE_SCHEMA 
				AND k.TABLE_NAME = tc.TABLE_NAME
			 WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY') pk
		ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
		LEFT JOIN 
			(SELECT k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME
			 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
			 JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
				ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
				AND k.TABLE_SCHEMA = tc.TABLE_SCHEMA 
				AND k.TABLE_NAME = tc.TABLE_NAME
			 WHERE tc.CONSTRAINT_TYPE = 'UNIQUE') uk
		ON c.TABLE_SCHEMA = uk.TABLE_SCHEMA AND c.TABLE_NAME = uk.TABLE_NAME AND c.COLUMN_NAME = uk.COLUMN_NAME
		WHERE 
			t.TABLE_SCHEMA = CURRENT_SCHEMA() AND
			t.TABLE_TYPE = 'BASE TABLE'
		ORDER BY 
			t.TABLE_NAME, c.ORDINAL_POSITION
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
		var columnDefault, isArray, isUnique, isAutoIncrement, isPrimaryKey, tableType string
		var charMaxLength, numericPrecision, numericScale, ordinalPosition sql.NullInt64

		if err := rows.Scan(
			&schemaName, &tableName, &columnName, &dataType, &isNullable, &columnDefault,
			&charMaxLength, &numericPrecision, &numericScale, &ordinalPosition,
			&isPrimaryKey, &isArray, &isUnique, &isAutoIncrement, &tableType,
		); err != nil {
			return nil, nil, fmt.Errorf("error scanning table and column row: %v", err)
		}

		if _, exists := tables[tableName]; !exists {
			tables[tableName] = &common.TableInfo{
				Name:      tableName,
				Schema:    schemaName,
				TableType: fmt.Sprintf("snowflake.%s", strings.ToLower(tableType)),
			}
			tableNames = append(tableNames, tableName)
		}

		var defaultValue *string
		if columnDefault != "" {
			defaultValue = &columnDefault
		}

		columnInfo := common.ColumnInfo{
			Name:            columnName,
			DataType:        dataType,
			IsNullable:      isNullable == "YES",
			ColumnDefault:   defaultValue,
			IsPrimaryKey:    isPrimaryKey == "YES",
			IsArray:         isArray == "YES",
			IsUnique:        isUnique == "YES",
			IsAutoIncrement: isAutoIncrement == "YES",
		}

		// Handle VARCHAR length
		if charMaxLength.Valid && (strings.Contains(strings.ToUpper(dataType), "VARCHAR") ||
			strings.Contains(strings.ToUpper(dataType), "CHAR") ||
			strings.Contains(strings.ToUpper(dataType), "STRING")) {
			varcharLength := int(charMaxLength.Int64)
			columnInfo.VarcharLength = &varcharLength
		}

		// Handle numeric precision and scale
		if numericPrecision.Valid && (strings.Contains(strings.ToUpper(dataType), "NUMBER") ||
			strings.Contains(strings.ToUpper(dataType), "DECIMAL") ||
			strings.Contains(strings.ToUpper(dataType), "NUMERIC")) {
			precision := strconv.FormatInt(numericPrecision.Int64, 10)
			columnInfo.NumericPrecision = &precision

			if numericScale.Valid {
				scale := strconv.FormatInt(numericScale.Int64, 10)
				columnInfo.NumericScale = &scale
			}
		}

		// Handle array types
		if isArray == "YES" {
			// Extract the element type from the array type
			elementType := strings.TrimSuffix(strings.TrimPrefix(dataType, "ARRAY("), ")")
			columnInfo.ArrayElementType = &elementType
		}

		tables[tableName].Columns = append(tables[tableName].Columns, columnInfo)

		// Add column to primary key if it's a primary key
		if isPrimaryKey == "YES" {
			tables[tableName].PrimaryKey = append(tables[tableName].PrimaryKey, columnName)
		}
	}

	// Get table constraints
	for _, table := range tables {
		if err := fetchTableConstraints(db, table); err != nil {
			return nil, nil, fmt.Errorf("error fetching constraints for table %s: %v", table.Name, err)
		}
	}

	// Get table indexes
	for _, table := range tables {
		if err := fetchTableIndexes(db, table); err != nil {
			return nil, nil, fmt.Errorf("error fetching indexes for table %s: %v", table.Name, err)
		}
	}

	if len(tables) == 0 {
		return tables, []string{}, nil
	}

	sort.Strings(tableNames)

	return tables, tableNames, nil
}

func fetchTableConstraints(db *sql.DB, table *common.TableInfo) error {
	query := `
		SELECT 
			tc.CONSTRAINT_NAME,
			tc.CONSTRAINT_TYPE,
			kcu.COLUMN_NAME,
			rc.UNIQUE_CONSTRAINT_NAME,
			rc.UPDATE_RULE,
			rc.DELETE_RULE,
			rc.MATCH_OPTION,
			kcu2.TABLE_NAME as REFERENCED_TABLE,
			kcu2.COLUMN_NAME as REFERENCED_COLUMN
		FROM 
			INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA 
			AND tc.TABLE_NAME = kcu.TABLE_NAME
		LEFT JOIN 
			INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
			ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
			AND tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
		LEFT JOIN 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2 
			ON rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME 
			AND rc.UNIQUE_CONSTRAINT_SCHEMA = kcu2.CONSTRAINT_SCHEMA
		WHERE 
			tc.TABLE_SCHEMA = ? AND 
			tc.TABLE_NAME = ? AND
			tc.CONSTRAINT_TYPE IN ('FOREIGN KEY', 'UNIQUE', 'CHECK')
		ORDER BY 
			tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
	`

	rows, err := db.Query(query, table.Schema, table.Name)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	constraintMap := make(map[string]common.Constraint)
	for rows.Next() {
		var constraintName, constraintType, columnName string
		var uniqueConstraintName, updateRule, deleteRule, matchOption sql.NullString
		var referencedTable, referencedColumn sql.NullString

		if err := rows.Scan(
			&constraintName, &constraintType, &columnName,
			&uniqueConstraintName, &updateRule, &deleteRule, &matchOption,
			&referencedTable, &referencedColumn,
		); err != nil {
			return fmt.Errorf("error scanning constraint row: %v", err)
		}

		// Skip if we've already processed this constraint
		if _, exists := constraintMap[constraintName]; exists {
			continue
		}

		constraint := common.Constraint{
			Name:   constraintName,
			Type:   constraintType,
			Table:  table.Name,
			Column: columnName,
		}

		if constraintType == "FOREIGN KEY" && referencedTable.Valid && referencedColumn.Valid {
			constraint.ForeignTable = referencedTable.String
			constraint.ForeignColumn = referencedColumn.String
			constraint.ReferencedTable = referencedTable.String
			constraint.ReferencedColumn = referencedColumn.String

			if updateRule.Valid {
				constraint.OnUpdate = updateRule.String
			}
			if deleteRule.Valid {
				constraint.OnDelete = deleteRule.String
			}

			// Build the foreign key definition
			constraint.Definition = fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)",
				columnName, referencedTable.String, referencedColumn.String)

			if updateRule.Valid {
				constraint.Definition += fmt.Sprintf(" ON UPDATE %s", updateRule.String)
			}
			if deleteRule.Valid {
				constraint.Definition += fmt.Sprintf(" ON DELETE %s", deleteRule.String)
			}

			// Create ForeignKeyInfo
			constraint.ForeignKey = &common.ForeignKeyInfo{
				Table:    referencedTable.String,
				Column:   referencedColumn.String,
				OnUpdate: updateRule.String,
				OnDelete: deleteRule.String,
			}
		} else if constraintType == "UNIQUE" {
			constraint.Definition = fmt.Sprintf("UNIQUE (%s)", columnName)
		}

		constraintMap[constraintName] = constraint
		table.Constraints = append(table.Constraints, constraint)
	}

	return nil
}

func fetchTableIndexes(db *sql.DB, table *common.TableInfo) error {
	// Snowflake doesn't have traditional indexes like PostgreSQL
	// It uses micro-partitions and clustering keys instead
	// We'll query for clustering keys as they're the closest equivalent to indexes

	query := `
		SELECT 
			CLUSTERING_KEY,
			CLUSTER_BY
		FROM 
			INFORMATION_SCHEMA.TABLES
		WHERE 
			TABLE_SCHEMA = ? AND 
			TABLE_NAME = ? AND
			CLUSTERING_KEY IS NOT NULL
	`

	rows, err := db.Query(query, table.Schema, table.Name)
	if err != nil {
		return fmt.Errorf("error querying clustering keys: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var clusteringKey, clusterBy sql.NullString

		if err := rows.Scan(&clusteringKey, &clusterBy); err != nil {
			return fmt.Errorf("error scanning clustering key row: %v", err)
		}

		if clusteringKey.Valid {
			// Parse the clustering key to extract column names
			columns := parseClusteringKey(clusteringKey.String)

			// Create an index info for the clustering key
			indexInfo := common.IndexInfo{
				Name:     fmt.Sprintf("%s_clustering_key", table.Name),
				Columns:  columns,
				IsUnique: false, // Clustering keys are not unique indexes
			}

			table.Indexes = append(table.Indexes, indexInfo)
		}
	}

	return nil
}

func parseClusteringKey(clusteringKey string) []string {
	// Remove parentheses and split by comma
	key := strings.TrimPrefix(strings.TrimSuffix(clusteringKey, ")"), "(")
	parts := strings.Split(key, ",")

	// Clean up each part
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := strings.TrimSpace(part)
		// Remove any ASC/DESC indicators
		column = strings.Split(column, " ")[0]
		columns = append(columns, column)
	}

	return columns
}

func getSchemas(db *sql.DB) ([]common.DatabaseSchemaInfo, error) {
	query := `
		SELECT 
			SCHEMA_NAME,
			COMMENT
		FROM 
			INFORMATION_SCHEMA.SCHEMATA
		WHERE 
			CATALOG_NAME = CURRENT_DATABASE()
		ORDER BY 
			SCHEMA_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %v", err)
	}
	defer rows.Close()

	var schemas []common.DatabaseSchemaInfo
	for rows.Next() {
		var schema common.DatabaseSchemaInfo
		var comment sql.NullString
		if err := rows.Scan(&schema.Name, &comment); err != nil {
			return nil, fmt.Errorf("error scanning schema: %v", err)
		}
		if comment.Valid {
			schema.Description = comment.String
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

func getFunctions(db *sql.DB) ([]common.FunctionInfo, error) {
	query := `
		SELECT 
			FUNCTION_SCHEMA,
			FUNCTION_NAME,
			ARGUMENT_SIGNATURE,
			DATA_TYPE,
			FUNCTION_DEFINITION
		FROM 
			INFORMATION_SCHEMA.FUNCTIONS
		WHERE 
			FUNCTION_CATALOG = CURRENT_DATABASE()
		ORDER BY 
			FUNCTION_SCHEMA, FUNCTION_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	var functions []common.FunctionInfo
	for rows.Next() {
		var function common.FunctionInfo
		var argSignature, definition sql.NullString
		if err := rows.Scan(
			&function.Schema,
			&function.Name,
			&argSignature,
			&function.ReturnType,
			&definition,
		); err != nil {
			return nil, fmt.Errorf("error scanning function: %v", err)
		}

		if argSignature.Valid {
			function.Arguments = argSignature.String
		}
		if definition.Valid {
			function.Body = definition.String
		}

		functions = append(functions, function)
	}

	return functions, nil
}

func getProcedures(db *sql.DB) ([]common.ProcedureInfo, error) {
	query := `
		SELECT 
			PROCEDURE_SCHEMA,
			PROCEDURE_NAME,
			ARGUMENT_SIGNATURE,
			PROCEDURE_DEFINITION,
			CREATED,
			LAST_ALTERED
		FROM 
			INFORMATION_SCHEMA.PROCEDURES
		WHERE 
			PROCEDURE_CATALOG = CURRENT_DATABASE()
		ORDER BY 
			PROCEDURE_SCHEMA, PROCEDURE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying procedures: %v", err)
	}
	defer rows.Close()

	var procedures []common.ProcedureInfo
	for rows.Next() {
		var procedure common.ProcedureInfo
		var argSignature, definition, created, lastAltered sql.NullString

		if err := rows.Scan(
			&procedure.Schema,
			&procedure.Name,
			&argSignature,
			&definition,
			&created,
			&lastAltered,
		); err != nil {
			return nil, fmt.Errorf("error scanning procedure: %v", err)
		}

		if argSignature.Valid {
			procedure.Arguments = argSignature.String
			procedure.ParameterList = argSignature.String
		}
		if definition.Valid {
			procedure.Body = definition.String
			procedure.RoutineDefinition = definition.String
		}
		if created.Valid {
			procedure.Created = created.String
		}
		if lastAltered.Valid {
			procedure.Modified = lastAltered.String
		}

		// Set default values for required fields
		procedure.DefinerUser = "UNKNOWN"
		procedure.DefinerHost = "UNKNOWN"
		procedure.SecurityType = "DEFINER"
		procedure.RoutineBody = "SQL"

		procedures = append(procedures, procedure)
	}

	return procedures, nil
}

func getSequences(db *sql.DB) ([]common.SequenceInfo, error) {
	query := `
		SELECT 
			SEQUENCE_SCHEMA,
			SEQUENCE_NAME,
			DATA_TYPE,
			START_VALUE,
			INCREMENT,
			MAXIMUM_VALUE,
			MINIMUM_VALUE,
			CYCLE_OPTION
		FROM 
			INFORMATION_SCHEMA.SEQUENCES
		WHERE 
			SEQUENCE_CATALOG = CURRENT_DATABASE()
		ORDER BY 
			SEQUENCE_SCHEMA, SEQUENCE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	var sequences []common.SequenceInfo
	for rows.Next() {
		var seq common.SequenceInfo
		var startValue, increment, maxValue, minValue string
		var cycleOption string

		if err := rows.Scan(
			&seq.Schema,
			&seq.Name,
			&seq.DataType,
			&startValue,
			&increment,
			&maxValue,
			&minValue,
			&cycleOption,
		); err != nil {
			return nil, fmt.Errorf("error scanning sequence: %v", err)
		}

		// Convert string values to int64
		seq.Start, _ = strconv.ParseInt(startValue, 10, 64)
		seq.Increment, _ = strconv.ParseInt(increment, 10, 64)
		seq.MaxValue, _ = strconv.ParseInt(maxValue, 10, 64)
		seq.MinValue, _ = strconv.ParseInt(minValue, 10, 64)
		seq.Cycle = cycleOption == "YES"
		seq.CacheSize = 1 // Snowflake doesn't expose cache size, default to 1

		sequences = append(sequences, seq)
	}

	return sequences, nil
}

func getViews(db *sql.DB) ([]common.ViewInfo, error) {
	query := `
		SELECT 
			TABLE_SCHEMA,
			TABLE_NAME,
			VIEW_DEFINITION
		FROM 
			INFORMATION_SCHEMA.VIEWS
		WHERE 
			TABLE_CATALOG = CURRENT_DATABASE()
		ORDER BY 
			TABLE_SCHEMA, TABLE_NAME
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	var views []common.ViewInfo
	for rows.Next() {
		var view common.ViewInfo
		var definition sql.NullString

		if err := rows.Scan(
			&view.Schema,
			&view.Name,
			&definition,
		); err != nil {
			return nil, fmt.Errorf("error scanning view: %v", err)
		}

		if definition.Valid {
			view.Definition = definition.String
		}

		views = append(views, view)
	}

	return views, nil
}

func getStages(db *sql.DB) ([]SnowflakeStageInfo, error) {
	query := `
		SHOW STAGES IN DATABASE
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying stages: %v", err)
	}
	defer rows.Close()

	var stages []SnowflakeStageInfo
	for rows.Next() {
		var created, name, database, schema, owner, comment, region, url, hasCredentials, hasEncryptionKey, stageType sql.NullString
		var cloudProvider, notificationChannel, stageSize sql.NullString

		// SHOW STAGES returns a result set with these columns
		if err := rows.Scan(
			&created,
			&name,
			&database,
			&schema,
			&owner,
			&comment,
			&region,
			&url,
			&hasCredentials,
			&hasEncryptionKey,
			&stageType,
			&cloudProvider,
			&notificationChannel,
			&stageSize,
		); err != nil {
			return nil, fmt.Errorf("error scanning stage: %v", err)
		}

		stage := SnowflakeStageInfo{
			Name:      name.String,
			Schema:    schema.String,
			Database:  database.String,
			StageType: stageType.String,
		}

		if url.Valid {
			stage.URL = url.String
		}

		if hasCredentials.Valid && hasCredentials.String == "true" {
			stage.Credentials = "CREDENTIALS STORED"
		}

		stages = append(stages, stage)
	}

	return stages, nil
}

func getWarehouses(db *sql.DB) ([]SnowflakeWarehouseInfo, error) {
	query := `
		SHOW WAREHOUSES
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying warehouses: %v", err)
	}
	defer rows.Close()

	var warehouses []SnowflakeWarehouseInfo
	for rows.Next() {
		var name, state, stateChangeReason, type_, size, minClusterCount, maxClusterCount, startedClusters, running sql.NullString
		var queued, isDefault, isCurrent, autoSuspend, autoResume, available, provisioning, quiescing, other sql.NullString
		var createdOn, resumedOn, updatedOn, owner, comment, resourceMonitor sql.NullString

		// SHOW WAREHOUSES returns a result set with these columns
		if err := rows.Scan(
			&name,
			&state,
			&stateChangeReason,
			&type_,
			&size,
			&minClusterCount,
			&maxClusterCount,
			&startedClusters,
			&running,
			&queued,
			&isDefault,
			&isCurrent,
			&autoSuspend,
			&autoResume,
			&available,
			&provisioning,
			&quiescing,
			&other,
			&createdOn,
			&resumedOn,
			&updatedOn,
			&owner,
			&comment,
			&resourceMonitor,
		); err != nil {
			return nil, fmt.Errorf("error scanning warehouse: %v", err)
		}

		warehouse := SnowflakeWarehouseInfo{
			Name:  name.String,
			Size:  size.String,
			State: state.String,
		}

		if minClusterCount.Valid {
			warehouse.MinClusterCount, _ = strconv.Atoi(minClusterCount.String)
		}

		if maxClusterCount.Valid {
			warehouse.MaxClusterCount, _ = strconv.Atoi(maxClusterCount.String)
		}

		if autoSuspend.Valid {
			warehouse.AutoSuspend, _ = strconv.Atoi(autoSuspend.String)
		}

		if autoResume.Valid {
			warehouse.AutoResume = autoResume.String == "true"
		}

		warehouses = append(warehouses, warehouse)
	}

	return warehouses, nil
}

func getPipes(db *sql.DB) ([]SnowflakePipeInfo, error) {
	query := `
		SHOW PIPES IN DATABASE
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying pipes: %v", err)
	}
	defer rows.Close()

	var pipes []SnowflakePipeInfo
	for rows.Next() {
		var created, name, database, schema, owner, comment, definition, pattern, notificationChannel, integration sql.NullString
		var errorIntegration, lastRefreshed, pendingFileCount, pipeType, state, lastExecutedTime sql.NullString

		// SHOW PIPES returns a result set with these columns
		if err := rows.Scan(
			&created,
			&name,
			&database,
			&schema,
			&owner,
			&comment,
			&definition,
			&pattern,
			&notificationChannel,
			&integration,
			&errorIntegration,
			&lastRefreshed,
			&pendingFileCount,
			&pipeType,
			&state,
			&lastExecutedTime,
		); err != nil {
			return nil, fmt.Errorf("error scanning pipe: %v", err)
		}

		pipe := SnowflakePipeInfo{
			Name:     name.String,
			Schema:   schema.String,
			Database: database.String,
			Owner:    owner.String,
		}

		if definition.Valid {
			pipe.Definition = definition.String
		}

		if pipeType.Valid {
			pipe.PipeType = pipeType.String
		}

		if notificationChannel.Valid {
			pipe.NotificationChannel = notificationChannel.String
		}

		pipes = append(pipes, pipe)
	}

	return pipes, nil
}

// CreateTable creates a new table in Snowflake
func CreateTable(tx *sql.Tx, tableInfo common.TableInfo) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}

	if tableInfo.Name == "" {
		return fmt.Errorf("table name is empty")
	}

	// Check if the table already exists
	var exists bool
	err := tx.QueryRow("SELECT EXISTS (SELECT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = CURRENT_SCHEMA())", tableInfo.Name).Scan(&exists)
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

		// Handle identity columns (auto-increment)
		isIdentity := column.IsAutoIncrement || (column.ColumnDefault != nil && strings.Contains(*column.ColumnDefault, "IDENTITY"))

		if isIdentity {
			createTableSQL += "INTEGER IDENTITY"
		} else {
			switch strings.ToUpper(column.DataType) {
			case "ARRAY":
				if column.ArrayElementType == nil {
					return fmt.Errorf("array element type not specified for column %s", column.Name)
				}
				createTableSQL += fmt.Sprintf("ARRAY(%s)", *column.ArrayElementType)
			case "VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER", "CHARACTER VARYING":
				if column.VarcharLength == nil {
					createTableSQL += "VARCHAR"
				} else {
					createTableSQL += fmt.Sprintf("VARCHAR(%d)", *column.VarcharLength)
				}
			case "NUMBER", "DECIMAL", "NUMERIC":
				if column.NumericPrecision != nil && column.NumericScale != nil {
					createTableSQL += fmt.Sprintf("%s(%s,%s)", column.DataType, *column.NumericPrecision, *column.NumericScale)
				} else if column.NumericPrecision != nil {
					createTableSQL += fmt.Sprintf("%s(%s)", column.DataType, *column.NumericPrecision)
				} else {
					createTableSQL += column.DataType
				}
			default:
				createTableSQL += column.DataType
			}
		}

		if !column.IsNullable {
			createTableSQL += " NOT NULL"
		}
		if column.ColumnDefault != nil && !isIdentity {
			createTableSQL += fmt.Sprintf(" DEFAULT %s", *column.ColumnDefault)
		}
	}

	// Add primary key constraint if specified
	if len(tableInfo.PrimaryKey) > 0 {
		createTableSQL += fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(tableInfo.PrimaryKey, ", "))
	}

	createTableSQL += ")"

	// Print the SQL statement for debugging
	fmt.Printf("Creating table %s with SQL: %s\n", tableInfo.Name, createTableSQL)

	_, err = tx.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	// Add indexes (Snowflake doesn't support traditional indexes, but we can add clustering keys)
	if len(tableInfo.Indexes) > 0 {
		// Find non-primary key indexes that should be converted to clustering keys
		var clusteringColumns []string
		for _, index := range tableInfo.Indexes {
			// Skip primary key indexes as they're already handled
			if index.Name == fmt.Sprintf("%s_pkey", tableInfo.Name) {
				continue
			}

			// Add columns to clustering key list
			clusteringColumns = append(clusteringColumns, index.Columns...)
		}

		// If we have columns to cluster by, add a clustering key
		if len(clusteringColumns) > 0 {
			// Remove duplicates
			uniqueColumns := make(map[string]bool)
			var uniqueClusteringColumns []string
			for _, col := range clusteringColumns {
				if !uniqueColumns[col] {
					uniqueColumns[col] = true
					uniqueClusteringColumns = append(uniqueClusteringColumns, col)
				}
			}

			// Limit to 4 columns (Snowflake recommendation)
			if len(uniqueClusteringColumns) > 4 {
				uniqueClusteringColumns = uniqueClusteringColumns[:4]
			}

			clusterSQL := fmt.Sprintf("ALTER TABLE %s CLUSTER BY (%s)",
				tableInfo.Name, strings.Join(uniqueClusteringColumns, ", "))

			_, err = tx.Exec(clusterSQL)
			if err != nil {
				return fmt.Errorf("error adding clustering key: %v", err)
			}
		}
	}

	return nil
}

// AddTableConstraints adds constraints to an existing table
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
