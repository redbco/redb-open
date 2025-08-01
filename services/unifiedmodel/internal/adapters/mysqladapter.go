package adapters

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// MySQLIngester implements the Ingester interface for MySQL
type MySQLIngester struct{}

// IngestSchema converts a MySQL schema to a UnifiedModel
func (m *MySQLIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	// First try to unmarshal as MySQLModel (unifiedmodel format)
	var mysqlSchema models.MySQLModel
	if err := json.Unmarshal(rawSchema, &mysqlSchema); err == nil {
		// Successfully unmarshaled as MySQLModel, convert to UnifiedModel
		unifiedModel := mysqlSchema.ToUnifiedModel()
		warnings := []string{}
		return unifiedModel, warnings, nil
	}

	// If that fails, try to unmarshal as anchor service format and convert
	var anchorSchema struct {
		Tables []struct {
			Name      string `json:"name"`
			Schema    string `json:"schema"`
			TableType string `json:"tableType"`
			Columns   []struct {
				Name                 string  `json:"name"`
				DataType             string  `json:"dataType"`
				IsNullable           bool    `json:"isNullable"`
				IsPrimaryKey         bool    `json:"isPrimaryKey"`
				IsArray              bool    `json:"isArray"`
				IsUnique             bool    `json:"isUnique"`
				IsAutoIncrement      bool    `json:"isAutoIncrement"`
				IsGenerated          bool    `json:"isGenerated"`
				GenerationExpression *string `json:"generationExpression,omitempty"`
				ColumnDefault        *string `json:"columnDefault,omitempty"`
				ArrayElementType     *string `json:"arrayElementType,omitempty"`
				CustomTypeName       *string `json:"customTypeName,omitempty"`
				VarcharLength        *int    `json:"varcharLength,omitempty"`
				NumericPrecision     *string `json:"numericPrecision,omitempty"`
				NumericScale         *string `json:"numericScale,omitempty"`
			} `json:"columns"`
			PrimaryKey []string `json:"primaryKey"`
		} `json:"tables"`
		EnumTypes []struct {
			Name   string   `json:"name"`
			Values []string `json:"values"`
		} `json:"enumTypes"`
		Schemas []struct {
			Name        string `json:"name"`
			Description string `json:"description,omitempty"`
		} `json:"schemas"`
		Functions []struct {
			Name string `json:"name"`
		} `json:"functions"`
		Triggers []struct {
			Name string `json:"name"`
		} `json:"triggers"`
		Sequences []struct {
			Name string `json:"name"`
		} `json:"sequences"`
		Extensions []struct {
			Name string `json:"name"`
		} `json:"extensions"`
	}

	if err := json.Unmarshal(rawSchema, &anchorSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal MySQL schema: %w", err)
	}

	// Convert anchor schema to unified model
	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}

	// Convert schemas
	for _, schema := range anchorSchema.Schemas {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name:    schema.Name,
			Comment: schema.Description,
		})
	}

	// Convert enums
	for _, enum := range anchorSchema.EnumTypes {
		unifiedModel.Enums = append(unifiedModel.Enums, models.Enum{
			Name:   enum.Name,
			Values: enum.Values,
		})
	}

	// Convert functions
	for _, function := range anchorSchema.Functions {
		unifiedModel.Functions = append(unifiedModel.Functions, models.Function{
			Name: function.Name,
		})
	}

	// Convert triggers
	for _, trigger := range anchorSchema.Triggers {
		unifiedModel.Triggers = append(unifiedModel.Triggers, models.Trigger{
			Name: trigger.Name,
		})
	}

	// Convert sequences
	for _, sequence := range anchorSchema.Sequences {
		unifiedModel.Sequences = append(unifiedModel.Sequences, models.Sequence{
			Name: sequence.Name,
		})
	}

	// Convert extensions
	for _, extension := range anchorSchema.Extensions {
		unifiedModel.Extensions = append(unifiedModel.Extensions, models.Extension{
			Name: extension.Name,
		})
	}

	// Convert tables
	for _, table := range anchorSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: table.TableType,
		}

		// Convert columns
		for _, col := range table.Columns {
			// Convert string DataType to models.DataType struct
			dataType := convertStringDataTypeToStruct(col.DataType, col.IsArray, col.ArrayElementType, col.CustomTypeName, col.VarcharLength)

			unifiedCol := models.Column{
				Name:            col.Name,
				DataType:        dataType,
				IsNullable:      col.IsNullable,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsUnique:        col.IsUnique,
				IsAutoIncrement: col.IsAutoIncrement,
				IsGenerated:     col.IsGenerated,
				DefaultValue:    col.ColumnDefault,
			}

			// Handle generation expression
			if col.GenerationExpression != nil {
				unifiedCol.DefaultValueFunction = *col.GenerationExpression
			}

			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Add primary key constraint
		if len(table.PrimaryKey) > 0 {
			unifiedTable.Constraints = append(unifiedTable.Constraints, models.Constraint{
				Type:    "PRIMARY KEY",
				Columns: table.PrimaryKey,
			})
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	warnings := []string{}
	return unifiedModel, warnings, nil
}

// convertStringDataTypeToStruct converts a string data type to a models.DataType struct
func convertStringDataTypeToStruct(dataType string, isArray bool, arrayElementType, customTypeName *string, varcharLength *int) models.DataType {
	dt := models.DataType{
		Name:         dataType,
		TypeCategory: "basic",
		BaseType:     dataType,
		IsArray:      isArray,
	}

	// Handle array types
	if isArray {
		dt.TypeCategory = "array"
		dt.ArrayDimensions = 1
		if arrayElementType != nil {
			dt.BaseType = *arrayElementType
		}
	}

	// Handle custom types
	if customTypeName != nil && *customTypeName != "" {
		dt.CustomTypeName = *customTypeName
		dt.TypeCategory = "custom"
	}

	// Handle varchar length
	if varcharLength != nil && *varcharLength > 0 {
		dt.Length = *varcharLength
	}

	// Handle specific MySQL types
	switch strings.ToLower(dataType) {
	case "varchar", "char":
		dt.TypeCategory = "string"
	case "int", "integer", "bigint", "smallint", "tinyint":
		dt.TypeCategory = "numeric"
	case "decimal", "numeric", "float", "double":
		dt.TypeCategory = "numeric"
	case "date", "datetime", "timestamp", "time":
		dt.TypeCategory = "datetime"
	case "text", "longtext", "mediumtext", "tinytext":
		dt.TypeCategory = "string"
	case "blob", "longblob", "mediumblob", "tinyblob":
		dt.TypeCategory = "binary"
	case "json":
		dt.TypeCategory = "json"
	case "enum":
		dt.TypeCategory = "enum"
		dt.IsEnum = true
	}

	return dt
}

// MySQLExporter implements the Exporter interface for MySQL
type MySQLExporter struct{}

// ExportSchema converts a UnifiedModel to a MySQL schema
func (m *MySQLExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	mysqlSchema := models.MySQLModel{
		SchemaType: "mysql",
		Schemas:    model.Schemas,
		Enums:      model.Enums,
		Functions:  model.Functions,
		Triggers:   model.Triggers,
		Extensions: model.Extensions,
		Indexes:    model.Indexes,
	}

	// Convert unified tables to MySQL tables
	for _, table := range model.Tables {
		mysqlTable := table.ToMySQLTable()
		mysqlSchema.Tables = append(mysqlSchema.Tables, *mysqlTable)
	}

	warnings := []string{}
	return mysqlSchema, warnings, nil
}

// MySQLAdapter implements the conversion between MySQL and unified model
type MySQLAdapter struct {
	db *sql.DB
}

// NewMySQLAdapter creates a new MySQL adapter
func NewMySQLAdapter(db *sql.DB) *MySQLAdapter {
	return &MySQLAdapter{db: db}
}

// DiscoverSchema discovers the MySQL schema and converts it to a unified model
func (a *MySQLAdapter) DiscoverSchema() (*models.UnifiedModel, error) {
	mysqlModel := &models.MySQLModel{
		SchemaType: "mysql",
	}

	// Get schemas (databases in MySQL)
	if err := a.discoverSchemas(mysqlModel); err != nil {
		return nil, fmt.Errorf("error discovering schemas: %w", err)
	}

	// Get tables
	if err := a.discoverTables(mysqlModel); err != nil {
		return nil, fmt.Errorf("error discovering tables: %w", err)
	}

	// Get enums
	if err := a.discoverEnums(mysqlModel); err != nil {
		return nil, fmt.Errorf("error discovering enums: %w", err)
	}

	// Get functions
	if err := a.discoverFunctions(mysqlModel); err != nil {
		return nil, fmt.Errorf("error discovering functions: %w", err)
	}

	// Get triggers
	if err := a.discoverTriggers(mysqlModel); err != nil {
		return nil, fmt.Errorf("error discovering triggers: %w", err)
	}

	// Get extensions (plugins in MySQL)
	if err := a.discoverExtensions(mysqlModel); err != nil {
		return nil, fmt.Errorf("error discovering extensions: %w", err)
	}

	// Convert to unified model
	return mysqlModel.ToUnifiedModel(), nil
}

// discoverSchemas discovers MySQL databases (schemas)
func (a *MySQLAdapter) discoverSchemas(model *models.MySQLModel) error {
	query := `SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`
	rows, err := a.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var schema models.Schema
		if err := rows.Scan(&schema.Name); err != nil {
			return err
		}
		model.Schemas = append(model.Schemas, schema)
	}

	return rows.Err()
}

// discoverTables discovers MySQL tables and their structure
func (a *MySQLAdapter) discoverTables(model *models.MySQLModel) error {
	query := `
		SELECT 
			t.TABLE_SCHEMA,
			t.TABLE_NAME,
			t.TABLE_TYPE,
			t.ENGINE,
			t.AUTO_INCREMENT,
			t.ROW_FORMAT,
			t.TABLE_COLLATION,
			t.TABLE_COMMENT
		FROM information_schema.TABLES t
		WHERE t.TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
		ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME`

	rows, err := a.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var table models.MySQLTable
		var tableType string
		err := rows.Scan(
			&table.Schema,
			&table.Name,
			&tableType,
			&table.Engine,
			&table.AutoIncrement,
			&table.RowFormat,
			&table.Collation,
			&table.Comment,
		)
		if err != nil {
			return err
		}

		// Set table type
		switch strings.ToUpper(tableType) {
		case "BASE TABLE":
			table.TableType = "standard"
		case "VIEW":
			table.TableType = "view"
		}

		// Get columns
		if err := a.discoverColumns(&table); err != nil {
			return fmt.Errorf("error discovering columns for table %s: %w", table.Name, err)
		}

		// Get indexes
		if err := a.discoverIndexes(&table); err != nil {
			return fmt.Errorf("error discovering indexes for table %s: %w", table.Name, err)
		}

		// Get constraints
		if err := a.discoverConstraints(&table); err != nil {
			return fmt.Errorf("error discovering constraints for table %s: %w", table.Name, err)
		}

		// Get partition info
		if err := a.discoverPartitionInfo(&table); err != nil {
			return fmt.Errorf("error discovering partition info for table %s: %w", table.Name, err)
		}

		model.Tables = append(model.Tables, table)
	}

	return rows.Err()
}

// discoverColumns discovers columns for a MySQL table
func (a *MySQLAdapter) discoverColumns(table *models.MySQLTable) error {
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
		ORDER BY ORDINAL_POSITION`

	rows, err := a.db.Query(query, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var col models.MySQLColumn
		var isNullable, extra, charSet, collation, genExpr sql.NullString
		var colDefault sql.NullString
		var dataType string

		err := rows.Scan(
			&col.Name,
			&dataType,
			&isNullable,
			&colDefault,
			&extra,
			&col.Comment,
			&charSet,
			&collation,
			&genExpr,
		)
		if err != nil {
			return err
		}

		// Set column properties
		col.IsNullable = isNullable.String == "YES"
		col.DefaultValue = &colDefault.String
		col.CharacterSet = charSet.String
		col.Collation = collation.String
		col.GenerationExpr = genExpr.String

		// Parse EXTRA field
		if strings.Contains(extra.String, "auto_increment") {
			col.IsAutoIncrement = true
		}
		if strings.Contains(extra.String, "STORED GENERATED") {
			col.IsGenerated = true
			col.IsStored = true
		}
		if strings.Contains(extra.String, "VIRTUAL GENERATED") {
			col.IsGenerated = true
			col.IsVirtual = true
		}

		// Set data type properties
		col.DataType.Name = dataType
		col.DataType.BaseType = dataType
		col.DataType.TypeCategory = "basic"
		if strings.Contains(dataType, "enum") {
			col.DataType.TypeCategory = "enum"
		}

		table.Columns = append(table.Columns, col)
	}

	return rows.Err()
}

// discoverIndexes discovers indexes for a MySQL table
func (a *MySQLAdapter) discoverIndexes(table *models.MySQLTable) error {
	query := `
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			SEQ_IN_INDEX,
			NON_UNIQUE,
			INDEX_TYPE
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`

	rows, err := a.db.Query(query, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	indexMap := make(map[string]*models.Index)
	for rows.Next() {
		var idxName, colName string
		var seqInIndex int
		var nonUnique int
		var idxType string

		err := rows.Scan(&idxName, &colName, &seqInIndex, &nonUnique, &idxType)
		if err != nil {
			return err
		}

		idx, exists := indexMap[idxName]
		if !exists {
			idx = &models.Index{
				Name:        idxName,
				Schema:      table.Schema,
				Table:       table.Name,
				IndexMethod: idxType,
				IsUnique:    nonUnique == 0,
			}
			indexMap[idxName] = idx
		}

		idx.Columns = append(idx.Columns, models.IndexColumn{
			ColumnName: colName,
			Order:      seqInIndex,
		})
	}

	// Add indexes to table
	for _, idx := range indexMap {
		table.Indexes = append(table.Indexes, *idx)
	}

	return rows.Err()
}

// discoverConstraints discovers constraints for a MySQL table
func (a *MySQLAdapter) discoverConstraints(table *models.MySQLTable) error {
	// Primary Key
	pkQuery := `
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`

	rows, err := a.db.Query(pkQuery, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return err
		}
		pkColumns = append(pkColumns, colName)
	}

	if len(pkColumns) > 0 {
		table.Constraints = append(table.Constraints, models.Constraint{
			Type:    "PRIMARY KEY",
			Name:    "PRIMARY",
			Table:   table.Name,
			Columns: pkColumns,
		})
	}

	// Foreign Keys
	fkQuery := `
		SELECT 
			CONSTRAINT_NAME,
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`

	rows, err = a.db.Query(fkQuery, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	fkMap := make(map[string]*models.Constraint)
	for rows.Next() {
		var constraintName, columnName, refTable, refColumn string
		err := rows.Scan(&constraintName, &columnName, &refTable, &refColumn)
		if err != nil {
			return err
		}

		fk, exists := fkMap[constraintName]
		if !exists {
			fk = &models.Constraint{
				Type:              "FOREIGN KEY",
				Name:              constraintName,
				Table:             table.Name,
				ReferencedTable:   refTable,
				ReferencedColumns: make([]string, 0),
			}
			fkMap[constraintName] = fk
		}

		fk.Columns = append(fk.Columns, columnName)
		fk.ReferencedColumns = append(fk.ReferencedColumns, refColumn)
	}

	// Add foreign key constraints to table
	for _, fk := range fkMap {
		table.Constraints = append(table.Constraints, *fk)
	}

	return rows.Err()
}

// discoverPartitionInfo discovers partitioning information for a MySQL table
func (a *MySQLAdapter) discoverPartitionInfo(table *models.MySQLTable) error {
	query := `
		SELECT 
			PARTITION_METHOD,
			PARTITION_EXPRESSION,
			PARTITION_DESCRIPTION,
			SUBPARTITION_METHOD,
			SUBPARTITION_EXPRESSION
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		LIMIT 1`

	var partitionInfo models.MySQLPartitionInfo
	err := a.db.QueryRow(query, table.Schema, table.Name).Scan(
		&partitionInfo.Type,
		&partitionInfo.Expression,
		&partitionInfo.PartitionKeys,
		&partitionInfo.SubPartitionBy,
		&partitionInfo.SubPartitions,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // Table is not partitioned
	}
	if err != nil {
		return err
	}

	table.PartitionInfo = &partitionInfo
	return nil
}

// discoverEnums discovers MySQL ENUM types
func (a *MySQLAdapter) discoverEnums(model *models.MySQLModel) error {
	query := `
		SELECT 
			TABLE_SCHEMA,
			COLUMN_NAME,
			COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE DATA_TYPE = 'enum'
		GROUP BY TABLE_SCHEMA, COLUMN_NAME, COLUMN_TYPE`

	rows, err := a.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var enum models.Enum
		var columnType string
		err := rows.Scan(&enum.Schema, &enum.Name, &columnType)
		if err != nil {
			return err
		}

		// Parse enum values from column type
		enum.Values = parseEnumValues(columnType)
		model.Enums = append(model.Enums, enum)
	}

	return rows.Err()
}

// discoverFunctions discovers MySQL functions
func (a *MySQLAdapter) discoverFunctions(model *models.MySQLModel) error {
	query := `
		SELECT 
			ROUTINE_SCHEMA,
			ROUTINE_NAME,
			ROUTINE_DEFINITION,
			DTD_IDENTIFIER
		FROM information_schema.ROUTINES
		WHERE ROUTINE_TYPE = 'FUNCTION'`

	rows, err := a.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var fn models.Function
		err := rows.Scan(&fn.Schema, &fn.Name, &fn.Definition, &fn.ReturnType)
		if err != nil {
			return err
		}
		model.Functions = append(model.Functions, fn)
	}

	return rows.Err()
}

// discoverTriggers discovers MySQL triggers
func (a *MySQLAdapter) discoverTriggers(model *models.MySQLModel) error {
	query := `
		SELECT 
			TRIGGER_SCHEMA,
			TRIGGER_NAME,
			EVENT_MANIPULATION,
			ACTION_STATEMENT,
			ACTION_TIMING
		FROM information_schema.TRIGGERS`

	rows, err := a.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var trigger models.Trigger
		err := rows.Scan(
			&trigger.Schema,
			&trigger.Name,
			&trigger.Event,
			&trigger.Definition,
			&trigger.Timing,
		)
		if err != nil {
			return err
		}
		model.Triggers = append(model.Triggers, trigger)
	}

	return rows.Err()
}

// discoverExtensions discovers MySQL plugins (extensions)
func (a *MySQLAdapter) discoverExtensions(model *models.MySQLModel) error {
	query := `SHOW PLUGINS`
	rows, err := a.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name, status, type_, library sql.NullString
		var license sql.NullString
		err := rows.Scan(&name, &status, &type_, &library, &license)
		if err != nil {
			return err
		}

		if name.Valid && status.String == "ACTIVE" {
			model.Extensions = append(model.Extensions, models.Extension{
				Name:    name.String,
				Schema:  "mysql",
				Version: "1.0", // MySQL doesn't provide version info in SHOW PLUGINS
			})
		}
	}

	return rows.Err()
}

// parseEnumValues parses enum values from MySQL column type definition
func parseEnumValues(columnType string) []string {
	// Remove 'enum(' and ')' from the type definition
	values := strings.TrimPrefix(columnType, "enum(")
	values = strings.TrimSuffix(values, ")")

	// Split by comma and remove quotes
	var result []string
	for _, v := range strings.Split(values, ",") {
		v = strings.TrimSpace(v)
		v = strings.Trim(v, "'")
		result = append(result, v)
	}

	return result
}
