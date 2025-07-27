package adapters

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// MariaDBIngester implements the Ingester interface for MariaDB
type MariaDBIngester struct{}

// IngestSchema converts a MariaDB schema to a UnifiedModel
func (m *MariaDBIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var mariadbSchema models.MariaDBModel
	if err := json.Unmarshal(rawSchema, &mariadbSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal MariaDB schema: %w", err)
	}

	// Convert MariaDB model to UnifiedModel
	unifiedModel := mariadbSchema.ToUnifiedModel()
	warnings := []string{}

	return unifiedModel, warnings, nil
}

// MariaDBExporter implements the Exporter interface for MariaDB
type MariaDBExporter struct{}

// ExportSchema converts a UnifiedModel to a MariaDB schema
func (m *MariaDBExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	mariadbSchema := models.MariaDBModel{
		SchemaType: "mariadb",
		Schemas:    model.Schemas,
		Enums:      model.Enums,
		Functions:  model.Functions,
		Triggers:   model.Triggers,
		Extensions: model.Extensions,
		Indexes:    model.Indexes,
	}

	// Convert unified tables to MariaDB tables
	for _, table := range model.Tables {
		mariadbTable := table.ToMariaDBTable()
		mariadbSchema.Tables = append(mariadbSchema.Tables, *mariadbTable)
	}

	warnings := []string{}
	return mariadbSchema, warnings, nil
}

// MariaDBAdapter implements the DatabaseAdapter interface for MariaDB
type MariaDBAdapter struct {
	db *sql.DB
}

// NewMariaDBAdapter creates a new MariaDB adapter
func NewMariaDBAdapter(db *sql.DB) *MariaDBAdapter {
	return &MariaDBAdapter{db: db}
}

// DiscoverSchema discovers the schema of a MariaDB database
func (a *MariaDBAdapter) DiscoverSchema() (*models.MariaDBModel, error) {
	model := &models.MariaDBModel{
		SchemaType: "mariadb",
	}

	// Discover schemas
	schemas, err := a.discoverSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to discover schemas: %w", err)
	}
	model.Schemas = schemas

	// Discover tables for each schema
	for _, schema := range schemas {
		tables, err := a.discoverTables(schema.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to discover tables for schema %s: %w", schema.Name, err)
		}
		model.Tables = append(model.Tables, tables...)
	}

	// Discover enums
	enums, err := a.discoverEnums()
	if err != nil {
		return nil, fmt.Errorf("failed to discover enums: %w", err)
	}
	model.Enums = enums

	// Discover functions
	functions, err := a.discoverFunctions()
	if err != nil {
		return nil, fmt.Errorf("failed to discover functions: %w", err)
	}
	model.Functions = functions

	// Discover triggers
	triggers, err := a.discoverTriggers()
	if err != nil {
		return nil, fmt.Errorf("failed to discover triggers: %w", err)
	}
	model.Triggers = triggers

	return model, nil
}

// discoverSchemas discovers all schemas in the database
func (a *MariaDBAdapter) discoverSchemas() ([]models.Schema, error) {
	query := `
		SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
		FROM information_schema.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql')
	`
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []models.Schema
	for rows.Next() {
		var schema models.Schema
		var charset, collation sql.NullString
		if err := rows.Scan(&schema.Name, &charset, &collation); err != nil {
			return nil, err
		}
		schema.CharacterSet = charset.String
		schema.Collation = collation.String
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

// discoverTables discovers all tables in a schema
func (a *MariaDBAdapter) discoverTables(schema string) ([]models.MariaDBTable, error) {
	query := `
		SELECT 
			TABLE_NAME,
			TABLE_TYPE,
			ENGINE,
			AUTO_INCREMENT,
			TABLE_COLLATION,
			TABLE_COMMENT,
			ROW_FORMAT,
			CREATE_OPTIONS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
	`
	rows, err := a.db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []models.MariaDBTable
	for rows.Next() {
		var table models.MariaDBTable
		var autoIncrement sql.NullInt64
		var createOptions sql.NullString
		if err := rows.Scan(
			&table.Name,
			&table.TableType,
			&table.Engine,
			&autoIncrement,
			&table.Collation,
			&table.Comment,
			&table.RowFormat,
			&createOptions,
		); err != nil {
			return nil, err
		}
		table.Schema = schema
		table.AutoIncrement = autoIncrement.Int64

		// Discover columns for this table
		columns, err := a.discoverColumns(schema, table.Name)
		if err != nil {
			return nil, err
		}
		table.Columns = columns

		// Discover indexes for this table
		indexes, err := a.discoverIndexes(schema, table.Name)
		if err != nil {
			return nil, err
		}
		table.Indexes = indexes

		// Discover constraints for this table
		constraints, err := a.discoverConstraints(schema, table.Name)
		if err != nil {
			return nil, err
		}
		table.Constraints = constraints

		// Discover partitioning information
		if table.TableType == "BASE TABLE" {
			partitionInfo, err := a.discoverPartitionInfo(schema, table.Name)
			if err != nil {
				return nil, err
			}
			table.PartitionInfo = partitionInfo
		}

		tables = append(tables, table)
	}
	return tables, nil
}

// discoverColumns discovers all columns in a table
func (a *MariaDBAdapter) discoverColumns(schema, table string) ([]models.MariaDBColumn, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			EXTRA,
			COLUMN_COMMENT,
			CHARACTER_SET_NAME,
			COLLATION_NAME,
			GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := a.db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []models.MariaDBColumn
	for rows.Next() {
		var col models.MariaDBColumn
		var isNullable, extra, comment, charset, collation, genExpr sql.NullString
		var defaultValue sql.NullString
		if err := rows.Scan(
			&col.Name,
			&col.DataType.Name,
			&isNullable,
			&defaultValue,
			&extra,
			&comment,
			&charset,
			&collation,
			&genExpr,
		); err != nil {
			return nil, err
		}

		col.IsNullable = isNullable.String == "YES"
		col.DefaultValue = &defaultValue.String
		col.Comment = comment.String
		col.CharacterSet = charset.String
		col.Collation = collation.String
		col.GenerationExpr = genExpr.String

		// Parse EXTRA field
		extraParts := strings.Split(extra.String, " ")
		for _, part := range extraParts {
			switch part {
			case "auto_increment":
				col.IsAutoIncrement = true
			case "VIRTUAL":
				col.IsVirtual = true
			case "STORED":
				col.IsStored = true
			}
		}

		columns = append(columns, col)
	}
	return columns, nil
}

// discoverIndexes discovers all indexes in a table
func (a *MariaDBAdapter) discoverIndexes(schema, table string) ([]models.Index, error) {
	query := `
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			SEQ_IN_INDEX,
			NON_UNIQUE,
			INDEX_TYPE
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`
	rows, err := a.db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*models.Index)
	for rows.Next() {
		var idxName, colName, idxType string
		var seqInIndex int
		var nonUnique bool
		if err := rows.Scan(&idxName, &colName, &seqInIndex, &nonUnique, &idxType); err != nil {
			return nil, err
		}

		idx, exists := indexMap[idxName]
		if !exists {
			idx = &models.Index{
				Name:        idxName,
				IsUnique:    !nonUnique,
				IndexMethod: idxType,
			}
			indexMap[idxName] = idx
		}
		idx.Columns = append(idx.Columns, models.IndexColumn{ColumnName: colName})
	}

	indexes := make([]models.Index, 0, len(indexMap))
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}
	return indexes, nil
}

// discoverConstraints discovers all constraints in a table
func (a *MariaDBAdapter) discoverConstraints(schema, table string) ([]models.Constraint, error) {
	query := `
		SELECT 
			CONSTRAINT_NAME,
			CONSTRAINT_TYPE,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		AND CONSTRAINT_NAME != 'PRIMARY'
	`
	rows, err := a.db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraintMap := make(map[string]*models.Constraint)
	for rows.Next() {
		var constraint models.Constraint
		var refTable, refColumn sql.NullString
		if err := rows.Scan(&constraint.Name, &constraint.Type, &refTable, &refColumn); err != nil {
			return nil, err
		}

		if refTable.Valid {
			constraint.ReferencedTable = refTable.String
			constraint.ReferencedColumns = []string{refColumn.String}
		}

		constraintMap[constraint.Name] = &constraint
	}

	constraints := make([]models.Constraint, 0, len(constraintMap))
	for _, constraint := range constraintMap {
		constraints = append(constraints, *constraint)
	}
	return constraints, nil
}

// discoverPartitionInfo discovers partitioning information for a table
func (a *MariaDBAdapter) discoverPartitionInfo(schema, table string) (*models.MariaDBPartitionInfo, error) {
	query := `
		SELECT 
			PARTITION_METHOD,
			PARTITION_EXPRESSION,
			PARTITION_DESCRIPTION,
			SUBPARTITION_METHOD,
			SUBPARTITION_EXPRESSION
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		LIMIT 1
	`
	rows, err := a.db.Query(query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var info models.MariaDBPartitionInfo
	var partitionDesc, subPartitionExpr sql.NullString
	if err := rows.Scan(
		&info.Type,
		&info.Expression,
		&partitionDesc,
		&info.SubPartitionBy,
		&subPartitionExpr,
	); err != nil {
		return nil, err
	}

	if partitionDesc.Valid {
		info.Partitions = strings.Split(partitionDesc.String, ",")
	}

	return &info, nil
}

// discoverEnums discovers all enum types in the database
func (a *MariaDBAdapter) discoverEnums() ([]models.Enum, error) {
	query := `
		SELECT DISTINCT 
			TABLE_SCHEMA,
			COLUMN_NAME,
			COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE DATA_TYPE = 'enum'
	`
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var enums []models.Enum
	for rows.Next() {
		var enum models.Enum
		var columnType string
		if err := rows.Scan(&enum.Schema, &enum.Name, &columnType); err != nil {
			return nil, err
		}

		// Parse enum values from column type
		values := strings.Trim(columnType, "enum()")
		enum.Values = strings.Split(values, ",")
		for i, v := range enum.Values {
			enum.Values[i] = strings.Trim(v, "'")
		}

		enums = append(enums, enum)
	}
	return enums, nil
}

// discoverFunctions discovers all functions in the database
func (a *MariaDBAdapter) discoverFunctions() ([]models.Function, error) {
	query := `
		SELECT 
			ROUTINE_SCHEMA,
			ROUTINE_NAME,
			ROUTINE_DEFINITION,
			DATA_TYPE,
			IS_DETERMINISTIC,
			SQL_DATA_ACCESS,
			SECURITY_TYPE
		FROM information_schema.ROUTINES
		WHERE ROUTINE_TYPE = 'FUNCTION'
	`
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []models.Function
	for rows.Next() {
		var fn models.Function
		var isDeterministic, sqlDataAccess, securityType sql.NullString
		if err := rows.Scan(
			&fn.Schema,
			&fn.Name,
			&fn.Definition,
			&fn.ReturnType,
			&isDeterministic,
			&sqlDataAccess,
			&securityType,
		); err != nil {
			return nil, err
		}

		fn.IsDeterministic = isDeterministic.String == "YES"
		functions = append(functions, fn)
	}
	return functions, nil
}

// discoverTriggers discovers all triggers in the database
func (a *MariaDBAdapter) discoverTriggers() ([]models.Trigger, error) {
	query := `
		SELECT 
			TRIGGER_SCHEMA,
			TRIGGER_NAME,
			EVENT_MANIPULATION,
			EVENT_OBJECT_TABLE,
			ACTION_STATEMENT,
			ACTION_TIMING,
			CREATED,
			SQL_MODE,
			DEFINER,
			CHARACTER_SET_CLIENT,
			COLLATION_CONNECTION,
			DATABASE_COLLATION
		FROM information_schema.TRIGGERS
	`
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []models.Trigger
	for rows.Next() {
		var trigger models.Trigger
		var created, sqlMode, definer, charsetClient, collationConn, dbCollation sql.NullString
		if err := rows.Scan(
			&trigger.Schema,
			&trigger.Name,
			&trigger.Event,
			&trigger.Table,
			&trigger.Definition,
			&trigger.Timing,
			&created,
			&sqlMode,
			&definer,
			&charsetClient,
			&collationConn,
			&dbCollation,
		); err != nil {
			return nil, err
		}

		triggers = append(triggers, trigger)
	}
	return triggers, nil
}
