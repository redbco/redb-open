package adapters

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/pkg/syslog"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type PostgresIngester struct{}

func (p *PostgresIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var pgSchema models.PostgresSchema
	if err := json.Unmarshal(rawSchema, &pgSchema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	unifiedModel := &models.UnifiedModel{}
	warnings := []string{}

	// Initialize all objects as empty arrays
	unifiedModel.Enums = []models.Enum{}
	unifiedModel.CompositeTypes = []models.CompositeType{}
	unifiedModel.Domains = []models.Domain{}
	unifiedModel.Ranges = []models.Range{}
	unifiedModel.Tables = []models.Table{}
	unifiedModel.Indexes = []models.Index{}
	unifiedModel.Extensions = []models.Extension{}
	unifiedModel.Sequences = []models.Sequence{}
	unifiedModel.Functions = []models.Function{}
	unifiedModel.Triggers = []models.Trigger{}

	// Create a map of enum names for quick lookup
	enumMap := make(map[string]models.Enum)

	// Process enums from pgSchema.Enums
	for _, enum := range pgSchema.EnumTypes {
		isExtensible := true
		if enum.IsExtensible != nil { // Check if explicitly set
			isExtensible = *enum.IsExtensible
		}

		schema := "public"
		if enum.Schema != nil {
			schema = *enum.Schema
		}

		unifiedEnum := models.Enum{
			Name:         enum.Name,
			Values:       enum.Values,
			Schema:       schema,
			IsExtensible: isExtensible,
			Collation:    enum.Collation,
			Comment:      enum.Comment,
			Owner:        enum.Owner,
		}
		unifiedModel.Enums = append(unifiedModel.Enums, unifiedEnum)
		key := schema + "." + enum.Name
		if schema == "" {
			key = "public." + enum.Name
		}
		enumMap[key] = unifiedEnum
	}

	// Process enums from pgSchema.EnumTypes
	for _, enum := range pgSchema.EnumTypes {
		isExtensible := true
		if enum.IsExtensible != nil { // Check if explicitly set
			isExtensible = *enum.IsExtensible
		}

		schema := "public"
		if enum.Schema != nil {
			schema = *enum.Schema
		}

		unifiedEnum := models.Enum{
			Name:         enum.Name,
			Values:       enum.Values,
			Schema:       schema,
			IsExtensible: isExtensible,
			Collation:    enum.Collation,
			Comment:      enum.Comment,
			Owner:        enum.Owner,
		}
		unifiedModel.Enums = append(unifiedModel.Enums, unifiedEnum)
		key := schema + "." + enum.Name
		if schema == "" {
			key = "public." + enum.Name
		}
		enumMap[key] = unifiedEnum
	}

	// Create maps for quick lookups
	compositeMap := make(map[string]models.CompositeType)
	domainMap := make(map[string]models.Domain)
	rangeMap := make(map[string]models.Range)
	for _, composite := range pgSchema.CompositeTypes {
		schema := "public"
		if composite.Schema != "" {
			schema = composite.Schema
		}

		unifiedComposite := models.CompositeType{
			Name:    composite.Name,
			Schema:  schema,
			Comment: composite.Comment,
			Owner:   composite.Owner,
		}

		// Convert composite fields
		for _, field := range composite.Fields {
			unifiedField := models.CompositeField{
				Name:         field.Name,
				IsNullable:   field.IsNullable,
				Collation:    field.Collation,
				DefaultValue: "",
			}
			if field.ColumnDefault != nil {
				unifiedField.DefaultValue = *field.ColumnDefault
			}

			// Parse the field's data type
			unifiedField.DataType = parsePostgresType(field.Type, enumMap, compositeMap, domainMap, rangeMap, schema, &warnings)
			unifiedComposite.Fields = append(unifiedComposite.Fields, unifiedField)
		}

		unifiedModel.CompositeTypes = append(unifiedModel.CompositeTypes, unifiedComposite)
		key := schema + "." + composite.Name
		compositeMap[key] = unifiedComposite
	}

	// Build domain map
	for _, domain := range pgSchema.Domains {
		schema := "public"
		if domain.Schema != "" {
			schema = domain.Schema
		}

		unifiedDomain := models.Domain{
			Name:       domain.Name,
			BaseType:   domain.BaseType,
			Schema:     schema,
			Collation:  domain.Collation,
			IsNullable: domain.IsNullable,
			Comment:    domain.Comment,
			Owner:      domain.Owner,
		}

		if domain.ColumnDefault != nil {
			unifiedDomain.ColumnDefault = *domain.ColumnDefault
		}
		if domain.CheckConstraint != nil {
			unifiedDomain.CheckConstraint = *domain.CheckConstraint
		}

		unifiedModel.Domains = append(unifiedModel.Domains, unifiedDomain)
		key := schema + "." + domain.Name
		domainMap[key] = unifiedDomain
	}

	// Build range map
	for _, rng := range pgSchema.Ranges {
		schema := "public"
		if rng.Schema != "" {
			schema = rng.Schema
		}

		unifiedRange := models.Range{
			Name:                rng.Name,
			Schema:              schema,
			Subtype:             rng.Subtype,
			CanonicalFunction:   rng.CanonicalFunction,
			SubtypeDiffFunction: rng.SubtypeDiffFunction,
			MultirangeType:      rng.MultirangeType,
			Comment:             rng.Comment,
			Owner:               rng.Owner,
		}

		unifiedModel.Ranges = append(unifiedModel.Ranges, unifiedRange)
		key := schema + "." + rng.Name
		rangeMap[key] = unifiedRange
	}

	// Convert schemas
	for _, schema := range pgSchema.Schemas {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{Name: schema.Name})
	}

	// Convert tables
	for _, table := range pgSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Schema,
			Indexes:   []models.Index{},
			TableType: convertPGTableType(table.TableType),
		}

		// Convert columns
		for _, col := range table.Columns {
			// Add warning for missing type
			if col.Type == "" {
				warnings = append(warnings, fmt.Sprintf("Column '%s' in table '%s' is missing a type. Defaulting to varchar.", col.Name, table.Name))
				col.Type = "varchar"
			}

			// Handle array types
			var typeToUse string
			if col.IsArray {
				if col.ArrayElementType != "" {
					typeToUse = col.ArrayElementType + "[]"
				} else {
					typeToUse = col.Type + "[]"
				}
			} else {
				typeToUse = col.Type
			}

			// Handle custom types (enums, domains, etc.)
			if col.CustomTypeName != "" {
				typeToUse = col.CustomTypeName
			}

			unifiedCol := models.Column{
				Name:            col.Name,
				IsNullable:      col.IsNullable,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsUnique:        col.IsUnique,
				IsAutoIncrement: col.IsAutoIncrement,
				IsGenerated:     col.IsGenerated,
				DefaultValue:    col.ColumnDefault,
				DataType:        parsePostgresType(typeToUse, enumMap, compositeMap, domainMap, rangeMap, table.Schema, &warnings),
			}

			// Set varchar length if specified
			if col.VarcharLength > 0 && unifiedCol.DataType.TypeCategory == "string" {
				unifiedCol.DataType.Length = col.VarcharLength
			}

			// Handle array element type
			if col.IsArray && col.ArrayElementType != "" {
				unifiedCol.DataType.IsArray = true
				unifiedCol.DataType.ArrayDimensions = 1
				unifiedCol.DataType.TypeCategory = "array"
				unifiedCol.DataType.BaseType = col.ArrayElementType
			}

			// Handle custom type name for USER-DEFINED types
			if col.CustomTypeName != "" {
				unifiedCol.DataType.CustomTypeName = col.CustomTypeName
				// Check if it's an enum
				enumKey := table.Schema + "." + col.CustomTypeName
				if table.Schema == "" {
					enumKey = "public." + col.CustomTypeName
				}
				if enum, exists := enumMap[enumKey]; exists {
					unifiedCol.DataType.TypeCategory = "enum"
					unifiedCol.DataType.IsEnum = true
					unifiedCol.DataType.EnumValues = enum.Values
					unifiedCol.DataType.Schema = enum.Schema
				}
			}

			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Handle table-level constraints
		for _, constraint := range table.Constraints {
			unifiedConstraint := models.Constraint{
				Type:              constraint.Type,
				Name:              constraint.Name,
				Table:             constraint.Table,
				Columns:           constraint.Columns,
				Deferrable:        constraint.Deferrable,
				InitiallyDeferred: constraint.InitiallyDeferred,
				Enabled:           constraint.Enabled,
				Validated:         constraint.Validated,
				CheckExpression:   constraint.CheckExpression,
				ReferencedTable:   constraint.ReferencedTable,
				ReferencedColumns: constraint.ReferencedColumns,
				OnUpdate:          constraint.OnUpdate,
				OnDelete:          constraint.OnDelete,
				UsingIndex:        constraint.UsingIndex,
			}
			unifiedTable.Constraints = append(unifiedTable.Constraints, unifiedConstraint)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	return unifiedModel, warnings, nil
}

type PostgresExporter struct{}

func (p *PostgresExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	pgSchema := models.PostgresSchema{
		SchemaType: "postgres",
	}
	warnings := []string{}

	// Convert tables
	for _, table := range model.Tables {
		pgTable := models.PGTable{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertToPostgresTableType(table.TableType),
		}

		// Set default schema to "public" if not defined
		if pgTable.Schema == "" {
			pgTable.Schema = "public"
		}

		// Convert columns
		for _, col := range table.Columns {
			pgCol := models.PGColumn{
				Name:            col.Name,
				IsNullable:      col.IsNullable,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsUnique:        col.IsUnique,
				IsGenerated:     col.IsGenerated,
				IsAutoIncrement: col.IsAutoIncrement,
				ColumnDefault:   col.DefaultValue,
			}

			// Handle the type conversion
			if col.DataType.IsArray {
				pgCol.IsArray = true
				if col.DataType.ArrayDimensions > 1 {
					// For multi-dimensional arrays
					pgCol.Type = strings.Repeat("[]", col.DataType.ArrayDimensions)
				} else {
					pgCol.Type = col.DataType.BaseType
				}
				pgCol.ArrayElementType = col.DataType.BaseType
			} else {
				pgCol.Type = col.DataType.BaseType
			}

			// Handle special types
			switch col.DataType.TypeCategory {
			case "enum":
				pgCol.CustomTypeName = col.DataType.BaseType
				pgCol.Type = "USER-DEFINED"
			case "domain":
				pgCol.CustomTypeName = col.DataType.BaseType
				pgCol.Type = "USER-DEFINED"
			case "composite":
				pgCol.CustomTypeName = col.DataType.BaseType
				pgCol.Type = "USER-DEFINED"
			case "string":
				if col.DataType.Length > 0 {
					pgCol.VarcharLength = col.DataType.Length
				}
			}

			pgTable.Columns = append(pgTable.Columns, pgCol)
		}

		pgSchema.Tables = append(pgSchema.Tables, pgTable)
	}

	return pgSchema, warnings, nil
}

func convertPGTableType(pgTableType string) string {
	if pgTableType == "" {
		return "unified.standard" // default type
	}

	// Remove the "postgres." prefix if it exists and add "unified." prefix
	tableType := pgTableType
	if len(tableType) > 9 && tableType[:9] == "postgres." {
		tableType = tableType[9:]
	}
	return "unified." + tableType
}

func convertToPostgresTableType(unifiedTableType string) string {
	if unifiedTableType == "" {
		return "postgres.standard"
	}

	// Remove the "unified." prefix if it exists and add "postgres." prefix
	tableType := unifiedTableType
	if len(tableType) > 8 && tableType[:8] == "unified." {
		tableType = tableType[8:]
	}
	return "postgres." + tableType
}

// Helper function to parse Postgres type string
func parsePostgresType(pgType string, enumMap map[string]models.Enum, compositeMap map[string]models.CompositeType, domainMap map[string]models.Domain, rangeMap map[string]models.Range, tableSchema string, warnings *[]string) models.DataType {
	// Initialize the base structure
	dataType := models.DataType{
		TypeCategory: "basic",
		BaseType:     pgType,
		Name:         pgType, // Set the name to match the original type
	}

	// Handle character varying (varchar)
	if pgType == "character varying" {
		dataType.Name = "varchar"
		dataType.BaseType = "varchar"
		dataType.TypeCategory = "string"
		return dataType
	}

	// Handle float8
	if pgType == "_float8" {
		dataType.Name = "float8"
		dataType.BaseType = "float8"
		dataType.TypeCategory = "numeric"
		dataType.IsArray = true
		return dataType
	}

	// Handle timestamp types
	if pgType == "timestamp" || pgType == "timestamp without time zone" {
		dataType.Name = "timestamp"
		dataType.BaseType = "timestamp"
		dataType.TypeCategory = "datetime"
		return dataType
	}
	if pgType == "timestamp with time zone" {
		dataType.Name = "timestamptz"
		dataType.BaseType = "timestamptz"
		dataType.TypeCategory = "datetime"
		return dataType
	}

	// Special handling for USER-DEFINED type
	if pgType == "USER-DEFINED" {
		// This is a placeholder - the actual type will be determined by the customTypeName
		// which is handled in the IngestSchema method
		dataType.TypeCategory = "custom"
		return dataType
	}

	// Special handling for ARRAY type
	if pgType == "ARRAY" {
		dataType.TypeCategory = "array"
		dataType.IsArray = true
		dataType.ArrayDimensions = 1 // Default to 1 dimension
		return dataType
	}

	// First check if this is an array type
	isArray := strings.Contains(pgType, "[]") || strings.Contains(pgType, "ARRAY")
	var baseType string
	var arrayDimensions int

	if isArray {
		// Handle ARRAY syntax (e.g., "ARRAY[2]" or "int ARRAY[2][3]")
		if strings.Contains(pgType, "ARRAY[") {
			// Extract dimensions from ARRAY[] notation
			arrayDimensions = strings.Count(pgType, "[")
			// Remove ARRAY[] notation to get base type
			baseType = strings.Split(pgType, "ARRAY[")[0]
			baseType = strings.TrimSpace(baseType)
		} else {
			// Handle [] syntax (e.g., "int[][]")
			arrayDimensions = strings.Count(pgType, "[]")
			// Remove array notation to get base type
			baseType = strings.TrimSuffix(pgType, strings.Repeat("[]", arrayDimensions))
		}
		dataType.Name = baseType // Update name to base type for arrays
	} else {
		baseType = pgType
	}

	// Helper function to get schema-qualified key
	getKey := func(schema, name string) string {
		if schema == "" {
			return "public." + name
		}
		return schema + "." + name
	}

	// Clean the base type by removing any length/precision specifiers
	cleanType := strings.ToLower(strings.Split(baseType, "(")[0])
	cleanType = strings.TrimSpace(cleanType)

	// Handle common PostgreSQL type aliases
	typeAliases := map[string]string{
		"character varying":           "varchar",
		"varchar":                     "varchar",
		"char varying":                "varchar",
		"character":                   "char",
		"double precision":            "float8",
		"float8":                      "float8",
		"float4":                      "real",
		"real":                        "real",
		"integer":                     "int4",
		"int":                         "int4",
		"int4":                        "int4",
		"bigint":                      "int8",
		"int8":                        "int8",
		"smallint":                    "int2",
		"int2":                        "int2",
		"timestamp without time zone": "timestamp",
		"timestamp with time zone":    "timestamptz",
		"time without time zone":      "time",
		"time with time zone":         "timetz",
	}

	// Apply type aliases
	if alias, exists := typeAliases[cleanType]; exists {
		cleanType = alias
	}

	key := getKey(tableSchema, cleanType)

	// Check if the base type matches an enum
	if enum, exists := enumMap[key]; exists {
		if isArray {
			dataType.TypeCategory = "array"
			dataType.IsArray = true
			dataType.ArrayDimensions = arrayDimensions
		} else {
			dataType.TypeCategory = "enum"
		}
		dataType.IsEnum = true
		dataType.BaseType = cleanType
		dataType.EnumValues = enum.Values
		dataType.Schema = enum.Schema
		return dataType
	}

	// Check if the base type matches a composite type
	if composite, exists := compositeMap[key]; exists {
		if isArray {
			dataType.TypeCategory = "array"
			dataType.IsArray = true
			dataType.ArrayDimensions = arrayDimensions
		} else {
			dataType.TypeCategory = "composite"
		}
		dataType.IsComposite = true
		dataType.BaseType = cleanType
		dataType.Schema = composite.Schema
		dataType.CompositeFields = append(dataType.CompositeFields, composite.Fields...)
		return dataType
	}

	// Check if the base type matches a domain type
	if domain, exists := domainMap[key]; exists {
		if isArray {
			dataType.TypeCategory = "array"
			dataType.IsArray = true
			dataType.ArrayDimensions = arrayDimensions
		} else {
			dataType.TypeCategory = "domain"
		}
		dataType.IsDomain = true
		dataType.BaseType = cleanType
		dataType.Schema = domain.Schema
		// Create a new DataType for the domain's base type
		domainBase := parsePostgresType(domain.BaseType, enumMap, compositeMap, domainMap, rangeMap, domain.Schema, warnings)
		dataType.DomainBase = &domainBase
		return dataType
	}

	// Check if the base type matches a range type
	if rng, exists := rangeMap[key]; exists {
		if isArray {
			dataType.TypeCategory = "array"
			dataType.IsArray = true
			dataType.ArrayDimensions = arrayDimensions
		} else {
			dataType.TypeCategory = "range"
		}
		dataType.IsRange = true
		dataType.BaseType = cleanType
		dataType.Schema = rng.Schema
		// Create a new DataType for the range's subtype
		rangeSubtype := parsePostgresType(rng.Subtype, enumMap, compositeMap, domainMap, rangeMap, rng.Schema, warnings)
		dataType.RangeSubtype = &rangeSubtype
		return dataType
	}

	// Handle explicit enum prefix (legacy support)
	if strings.HasPrefix(cleanType, "enum.") {
		enumBaseType := strings.TrimPrefix(cleanType, "enum.")
		enumKey := getKey(tableSchema, enumBaseType)

		if isArray {
			dataType.TypeCategory = "array"
			dataType.IsArray = true
			dataType.ArrayDimensions = arrayDimensions
		} else {
			dataType.TypeCategory = "enum"
		}
		dataType.IsEnum = true
		dataType.BaseType = enumBaseType

		// Populate enum values if the enum exists
		if enum, exists := enumMap[enumKey]; exists {
			dataType.EnumValues = enum.Values
			dataType.Schema = enum.Schema
		}
		return dataType
	}

	// Define valid PostgreSQL types with their categories
	validTypes := map[string]string{
		"bigint": "numeric", "int8": "numeric", "bigserial": "numeric", "serial8": "numeric",
		"bit": "bit", "varbit": "bit",
		"boolean": "boolean", "bool": "boolean",
		"box": "geometric", "circle": "geometric", "line": "geometric", "lseg": "geometric",
		"path": "geometric", "point": "geometric", "polygon": "geometric",
		"bytea":     "binary",
		"character": "string", "char": "string", "varchar": "string", "text": "string",
		"cidr": "network", "inet": "network", "macaddr": "network", "macaddr8": "network",
		"date": "datetime",
		"time": "datetime", "timetz": "datetime",
		"timestamp": "datetime", "timestamptz": "datetime",
		"double precision": "numeric", "float8": "numeric",
		"integer": "numeric", "int": "numeric", "int4": "numeric",
		"interval": "interval",
		"json":     "json", "jsonb": "json",
		"money":   "numeric",
		"numeric": "numeric", "decimal": "numeric",
		"real": "numeric", "float4": "numeric",
		"smallint": "numeric", "int2": "numeric",
		"smallserial": "numeric", "serial2": "numeric",
		"serial": "numeric", "serial4": "numeric",
		"tsquery": "text_search", "tsvector": "text_search",
		"uuid": "uuid", "xml": "xml",
		"pg_lsn": "system", "pg_snapshot": "system", "txid_snapshot": "system",
	}

	// Check if it's a valid PostgreSQL type
	if category, exists := validTypes[cleanType]; exists {
		dataType.TypeCategory = category
	} else if !strings.HasPrefix(cleanType, "composite.") &&
		!strings.HasPrefix(cleanType, "domain.") &&
		!strings.HasPrefix(cleanType, "range.") &&
		!strings.HasPrefix(cleanType, "enum.") &&
		cleanType != "USER-DEFINED" &&
		cleanType != "ARRAY" &&
		!strings.HasPrefix(cleanType, "_") && // PostgreSQL array types often start with underscore
		!isCustomType(cleanType, enumMap, tableSchema) { // Check if it's a custom type (enum, etc.)
		// Add warning about unknown type
		*warnings = append(*warnings, fmt.Sprintf("Unknown PostgreSQL type: %s. This might cause compatibility issues.", baseType))
	}

	// If we get here and it's an array, process it as a regular array
	if isArray {
		dataType.IsArray = true
		dataType.TypeCategory = "array"
		dataType.ArrayDimensions = arrayDimensions
		dataType.BaseType = cleanType
		return dataType
	}

	// Handle varchar/char length and numeric precision
	if strings.Contains(pgType, "(") {
		base := strings.Split(pgType, "(")[0]
		params := strings.Split(strings.Split(pgType, "(")[1], ")")[0]
		dataType.BaseType = base

		// Handle both length and precision/scale
		if strings.Contains(params, ",") {
			parts := strings.Split(params, ",")
			if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
				dataType.Precision = p
			}
			if s, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				dataType.Scale = s
			}
		} else {
			// Handle length for string types
			if l, err := strconv.Atoi(strings.TrimSpace(params)); err == nil {
				dataType.Length = l
			}
		}
	}

	return dataType
}

// Helper function to check if a type is a custom type (enum, etc.)
func isCustomType(typeName string, enumMap map[string]models.Enum, schema string) bool {
	syslog.Info("unifiedmodel", "Checking if type is custom: "+typeName)
	syslog.Info("unifiedmodel", "Schema: "+schema)
	syslog.Info("unifiedmodel", "Enum map size: "+fmt.Sprintf("%d", len(enumMap)))

	// Log all keys in the enumMap
	enumKeys := make([]string, 0, len(enumMap))
	for k := range enumMap {
		enumKeys = append(enumKeys, k)
	}
	syslog.Info("unifiedmodel", "Enum map keys: "+strings.Join(enumKeys, ", "))

	// Check if it's an enum
	enumKey := schema + "." + typeName
	if schema == "" {
		enumKey = "public." + typeName
	}
	syslog.Info("unifiedmodel", "Looking for enum key: "+enumKey)

	if _, exists := enumMap[enumKey]; exists {
		syslog.Info("unifiedmodel", "Found enum in map: "+enumKey)
		return true
	}

	// Check if it's a known custom type pattern
	customTypePatterns := []string{
		"_",            // Array types
		"enum.",        // Enum types
		"domain.",      // Domain types
		"composite.",   // Composite types
		"range.",       // Range types
		"USER-DEFINED", // User-defined types
		"ARRAY",        // Array types
	}

	for _, pattern := range customTypePatterns {
		if strings.HasPrefix(typeName, pattern) || typeName == pattern {
			syslog.Info("unifiedmodel", "Matched custom type pattern: "+pattern)
			return true
		}
	}

	return false
}

// Convert unified type to Postgres type string
func convertToPostgresType(dt models.DataType) string {
	var pgType string

	switch dt.TypeCategory {
	case "basic":
		pgType = dt.BaseType
		if dt.Length > 0 {
			pgType = fmt.Sprintf("%s(%d)", dt.BaseType, dt.Length)
		}
		if dt.Precision > 0 {
			if dt.Scale > 0 {
				pgType = fmt.Sprintf("%s(%d,%d)", dt.BaseType, dt.Precision, dt.Scale)
			} else {
				pgType = fmt.Sprintf("%s(%d)", dt.BaseType, dt.Precision)
			}
		}

	case "array":
		pgType = convertToPostgresType(models.DataType{
			TypeCategory: "basic",
			BaseType:     dt.BaseType,
			Length:       dt.Length,
			Precision:    dt.Precision,
			Scale:        dt.Scale,
		})
		pgType += strings.Repeat("[]", dt.ArrayDimensions)

	case "enum":
		pgType = "enum." + dt.BaseType

	case "domain":
		pgType = "domain." + dt.BaseType

	case "composite":
		pgType = "composite." + dt.BaseType

	case "range":
		pgType = "range." + dt.BaseType

	case "extension":
		pgType = dt.ExtensionName + "." + dt.BaseType
	}

	return pgType
}
