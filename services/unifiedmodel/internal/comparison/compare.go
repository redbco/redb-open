package comparison

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/syslog"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/adapters"
	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type SchemaComparator struct{}

func NewSchemaComparator() *SchemaComparator {
	return &SchemaComparator{}
}

type CompareResult struct {
	HasChanges bool
	Changes    []string
	Warnings   []string
}

// getAdapter returns the appropriate adapter based on the schema type
func getAdapter(schemaType string) (adapters.SchemaIngester, error) {
	switch schemaType {
	case "postgres", "postgresql":
		return &adapters.PostgresIngester{}, nil
	case "mysql":
		return &adapters.MySQLIngester{}, nil
	case "mariadb":
		return &adapters.MariaDBIngester{}, nil
	case "mssql", "sqlserver":
		return &adapters.MSSQLIngester{}, nil
	case "oracle":
		return &adapters.OracleIngester{}, nil
	case "db2":
		return &adapters.Db2Ingester{}, nil
	case "cockroach", "cockroachdb":
		return &adapters.CockroachIngester{}, nil
	case "clickhouse":
		return &adapters.ClickhouseIngester{}, nil
	case "cassandra":
		return &adapters.CassandraIngester{}, nil
	case "mongodb":
		return &adapters.MongoDBIngester{}, nil
	case "redis":
		return &adapters.RedisIngester{}, nil
	case "neo4j":
		return &adapters.Neo4jIngester{}, nil
	case "elasticsearch":
		return &adapters.ElasticsearchIngester{}, nil
	case "snowflake":
		return &adapters.SnowflakeIngester{}, nil
	case "pinecone":
		return &adapters.PineconeIngester{}, nil
	case "chroma":
		return &adapters.ChromaIngester{}, nil
	case "edgedb":
		return &adapters.EdgeDBIngester{}, nil
	default:
		return nil, fmt.Errorf("unsupported schema type: %s", schemaType)
	}
}

func (c *SchemaComparator) CompareSchemas(schemaType string, previousSchema, currentSchema []byte) (*CompareResult, error) {
	// If Neo4j, use the specialized comparison
	if schemaType == "neo4j" {
		return c.CompareNeo4jSchemas(previousSchema, currentSchema)
	}

	// Get the appropriate adapter for the schema type
	adapter, err := getAdapter(schemaType)
	if err != nil {
		syslog.Error("unifiedmodel", "Failed to get adapter: "+err.Error())
		return nil, err
	}

	// Convert both schemas to unified models using the appropriate adapter
	prevModel, prevWarnings, err := adapter.IngestSchema(previousSchema)
	if err != nil {
		syslog.Error("unifiedmodel", "Failed to convert previous schema: "+err.Error())
		return nil, fmt.Errorf("failed to convert previous schema: %w", err)
	}

	currModel, currWarnings, err := adapter.IngestSchema(currentSchema)
	if err != nil {
		syslog.Error("unifiedmodel", "Failed to convert current schema: "+err.Error())
		return nil, fmt.Errorf("failed to convert current schema: %w", err)
	}

	result := &CompareResult{
		Changes:  make([]string, 0),
		Warnings: make([]string, 0),
	}

	// Add any warnings from the conversion
	result.Warnings = append(result.Warnings, prevWarnings...)
	result.Warnings = append(result.Warnings, currWarnings...)

	// Ensure we have valid models to compare
	if prevModel == nil {
		prevModel = &models.UnifiedModel{
			SchemaType: "unified",
		}
	}

	if currModel == nil {
		currModel = &models.UnifiedModel{
			SchemaType: "unified",
		}
	}

	// Initialize all slices to prevent nil pointer panics
	c.initializeModelSlices(prevModel)
	c.initializeModelSlices(currModel)

	// Compare all schema components regardless of database type
	c.compareSchemas(prevModel, currModel, result)
	c.compareTables(prevModel, currModel, result)
	c.compareEnums(prevModel, currModel, result)
	c.compareCompositeTypes(prevModel, currModel, result)
	c.compareDomains(prevModel, currModel, result)
	c.compareRanges(prevModel, currModel, result)
	c.compareFunctions(prevModel, currModel, result)
	c.compareTriggers(prevModel, currModel, result)
	c.compareSequences(prevModel, currModel, result)
	c.compareExtensions(prevModel, currModel, result)
	c.compareGlobalIndexes(prevModel, currModel, result)

	result.HasChanges = len(result.Changes) > 0
	return result, nil
}

// CompareNeo4jSchemas is a specialized method for comparing Neo4j schemas
// that handles Neo4j-specific characteristics and warnings more gracefully
func (c *SchemaComparator) CompareNeo4jSchemas(previousSchema, currentSchema []byte) (*CompareResult, error) {
	// Use the same adapter but with Neo4j-specific handling
	adapter := &adapters.Neo4jIngester{}

	// Convert both schemas to unified models using the Neo4j adapter
	prevModel, prevWarnings, err := adapter.IngestSchema(previousSchema)
	if err != nil {
		syslog.Error("unifiedmodel", "Failed to convert previous Neo4j schema: "+err.Error())
		return nil, fmt.Errorf("failed to convert previous Neo4j schema: %w", err)
	}

	currModel, currWarnings, err := adapter.IngestSchema(currentSchema)
	if err != nil {
		syslog.Error("unifiedmodel", "Failed to convert current Neo4j schema: "+err.Error())
		return nil, fmt.Errorf("failed to convert current Neo4j schema: %w", err)
	}

	result := &CompareResult{
		Changes:  make([]string, 0),
		Warnings: make([]string, 0),
	}

	// Add any warnings from the conversion, but don't let them stop the comparison
	result.Warnings = append(result.Warnings, prevWarnings...)
	result.Warnings = append(result.Warnings, currWarnings...)

	// Ensure we have valid models to compare
	if prevModel == nil {
		prevModel = &models.UnifiedModel{
			SchemaType:     "unified",
			Schemas:        []models.Schema{},
			Tables:         []models.Table{},
			Enums:          []models.Enum{},
			CompositeTypes: []models.CompositeType{},
			Domains:        []models.Domain{},
			Ranges:         []models.Range{},
			Functions:      []models.Function{},
			Triggers:       []models.Trigger{},
			Sequences:      []models.Sequence{},
			Extensions:     []models.Extension{},
			Indexes:        []models.Index{},
		}
	}

	if currModel == nil {
		currModel = &models.UnifiedModel{
			SchemaType:     "unified",
			Schemas:        []models.Schema{},
			Tables:         []models.Table{},
			Enums:          []models.Enum{},
			CompositeTypes: []models.CompositeType{},
			Domains:        []models.Domain{},
			Ranges:         []models.Range{},
			Functions:      []models.Function{},
			Triggers:       []models.Trigger{},
			Sequences:      []models.Sequence{},
			Extensions:     []models.Extension{},
			Indexes:        []models.Index{},
		}
	}

	// Initialize all slices to prevent nil pointer panics
	c.initializeModelSlices(prevModel)
	c.initializeModelSlices(currModel)

	// For Neo4j, focus on the most relevant comparisons
	// Neo4j doesn't have traditional schemas, tables, enums, etc.
	// So we'll focus on what Neo4j does have: labels (as tables), relationships, constraints, indexes, and functions

	// Compare tables (which represent Neo4j labels and relationship types)
	c.compareTables(prevModel, currModel, result)

	// Compare functions (Neo4j procedures and functions)
	c.compareFunctions(prevModel, currModel, result)

	// Compare global indexes (Neo4j indexes)
	c.compareGlobalIndexes(prevModel, currModel, result)

	result.HasChanges = len(result.Changes) > 0
	return result, nil
}

// initializeModelSlices ensures all slices in a model are initialized to prevent nil pointer panics
func (c *SchemaComparator) initializeModelSlices(model *models.UnifiedModel) {
	if model.Schemas == nil {
		model.Schemas = []models.Schema{}
	}
	if model.Tables == nil {
		model.Tables = []models.Table{}
	}
	if model.Enums == nil {
		model.Enums = []models.Enum{}
	}
	if model.CompositeTypes == nil {
		model.CompositeTypes = []models.CompositeType{}
	}
	if model.Domains == nil {
		model.Domains = []models.Domain{}
	}
	if model.Ranges == nil {
		model.Ranges = []models.Range{}
	}
	if model.Functions == nil {
		model.Functions = []models.Function{}
	}
	if model.Triggers == nil {
		model.Triggers = []models.Trigger{}
	}
	if model.Sequences == nil {
		model.Sequences = []models.Sequence{}
	}
	if model.Extensions == nil {
		model.Extensions = []models.Extension{}
	}
	if model.Indexes == nil {
		model.Indexes = []models.Index{}
	}
}

func (c *SchemaComparator) compareSchemas(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevSchemaMap := make(map[string]models.Schema)
	currSchemaMap := make(map[string]models.Schema)

	for _, schema := range prev.Schemas {
		prevSchemaMap[schema.Name] = schema
	}

	for _, schema := range curr.Schemas {
		currSchemaMap[schema.Name] = schema
	}

	// Check for removed schemas
	for schemaName := range prevSchemaMap {
		if _, exists := currSchemaMap[schemaName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed schema: %s", schemaName))
		}
	}

	// Check for added schemas and compare existing schemas
	for schemaName, currSchema := range currSchemaMap {
		if prevSchema, exists := prevSchemaMap[schemaName]; exists {
			// Compare schema properties
			if prevSchema.CharacterSet != currSchema.CharacterSet {
				result.Changes = append(result.Changes, fmt.Sprintf("Schema %s character set changed: %s -> %s",
					schemaName, prevSchema.CharacterSet, currSchema.CharacterSet))
			}
			if prevSchema.Collation != currSchema.Collation {
				result.Changes = append(result.Changes, fmt.Sprintf("Schema %s collation changed: %s -> %s",
					schemaName, prevSchema.Collation, currSchema.Collation))
			}
			if prevSchema.Comment != currSchema.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Schema %s comment changed: %s -> %s",
					schemaName, prevSchema.Comment, currSchema.Comment))
			}
		} else {
			// New schema
			result.Changes = append(result.Changes, fmt.Sprintf("Added schema: %s", schemaName))
		}
	}
}

func (c *SchemaComparator) compareTables(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevTableMap := make(map[string]models.Table)
	currTableMap := make(map[string]models.Table)

	for _, table := range prev.Tables {
		key := table.Schema + "." + table.Name
		prevTableMap[key] = table
	}

	for _, table := range curr.Tables {
		key := table.Schema + "." + table.Name
		currTableMap[key] = table
	}

	// Check for removed tables
	for tableKey := range prevTableMap {
		if _, exists := currTableMap[tableKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed table: %s", tableKey))
		}
	}

	// Check for added tables and compare existing tables
	for tableKey, currTable := range currTableMap {
		if prevTable, exists := prevTableMap[tableKey]; exists {
			// Compare table properties
			if prevTable.TableType != currTable.TableType {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s type changed: %s -> %s",
					tableKey, prevTable.TableType, currTable.TableType))
			}
			if prevTable.Schema != currTable.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s schema changed: %s -> %s",
					tableKey, prevTable.Schema, currTable.Schema))
			}
			if prevTable.Owner != currTable.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s owner changed: %s -> %s",
					tableKey, prevTable.Owner, currTable.Owner))
			}
			if prevTable.Comment != currTable.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s comment changed: %s -> %s",
					tableKey, prevTable.Comment, currTable.Comment))
			}

			// Compare columns
			c.compareColumns(prevTable, currTable, tableKey, result)

			// Compare constraints
			c.compareTableConstraints(prevTable, currTable, tableKey, result)

			// Compare indexes
			c.compareTableIndexes(prevTable, currTable, tableKey, result)

			// Compare partitioning info if applicable
			if prevTable.TableType == "partitioned" || currTable.TableType == "partitioned" {
				c.comparePartitioning(prevTable, currTable, tableKey, result)
			}

			// Compare view definition if applicable
			if prevTable.TableType == "view" || currTable.TableType == "view" ||
				prevTable.TableType == "materialized" || currTable.TableType == "materialized" {
				if prevTable.ViewDefinition != currTable.ViewDefinition {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s view definition changed",
						tableKey))
				}
			}
		} else {
			// New table
			result.Changes = append(result.Changes, fmt.Sprintf("Added table: %s", tableKey))
		}
	}
}

func (c *SchemaComparator) compareColumns(prevTable, currTable models.Table, tableKey string, result *CompareResult) {
	// Ensure columns slices are not nil
	if prevTable.Columns == nil {
		prevTable.Columns = []models.Column{}
	}
	if currTable.Columns == nil {
		currTable.Columns = []models.Column{}
	}

	// Create maps for easier lookup
	prevColumnMap := make(map[string]models.Column)
	currColumnMap := make(map[string]models.Column)

	for _, column := range prevTable.Columns {
		prevColumnMap[column.Name] = column
	}

	for _, column := range currTable.Columns {
		currColumnMap[column.Name] = column
	}

	// Check for removed columns
	for columnName := range prevColumnMap {
		if _, exists := currColumnMap[columnName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Removed column: %s", tableKey, columnName))
		}
	}

	// Check for added columns and compare existing columns
	for columnName, currColumn := range currColumnMap {
		if prevColumn, exists := prevColumnMap[columnName]; exists {
			// Compare column properties
			if prevColumn.DataType.Name != currColumn.DataType.Name {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Data type changed: %s -> %s",
					tableKey, columnName, prevColumn.DataType.Name, currColumn.DataType.Name))
			}
			if prevColumn.IsNullable != currColumn.IsNullable {
				nullableStatus := "NOT NULL"
				if currColumn.IsNullable {
					nullableStatus = "NULL"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Nullability changed to %s",
					tableKey, columnName, nullableStatus))
			}
			if prevColumn.IsUnique != currColumn.IsUnique {
				uniqueStatus := "not unique"
				if currColumn.IsUnique {
					uniqueStatus = "unique"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Uniqueness changed to %s",
					tableKey, columnName, uniqueStatus))
			}
			if prevColumn.IsPrimaryKey != currColumn.IsPrimaryKey {
				pkStatus := "not a primary key"
				if currColumn.IsPrimaryKey {
					pkStatus = "a primary key"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Primary key status changed to %s",
					tableKey, columnName, pkStatus))
			}
			if prevColumn.IsAutoIncrement != currColumn.IsAutoIncrement {
				autoIncStatus := "not auto-increment"
				if currColumn.IsAutoIncrement {
					autoIncStatus = "auto-increment"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Auto-increment status changed to %s",
					tableKey, columnName, autoIncStatus))
			}
			if prevColumn.Collation != currColumn.Collation {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Collation changed: %s -> %s",
					tableKey, columnName, prevColumn.Collation, currColumn.Collation))
			}
			if prevColumn.Comment != currColumn.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Comment changed: %s -> %s",
					tableKey, columnName, prevColumn.Comment, currColumn.Comment))
			}

			// Compare default values
			prevDefault := "NULL"
			if prevColumn.DefaultValue != nil {
				prevDefault = *prevColumn.DefaultValue
			}
			currDefault := "NULL"
			if currColumn.DefaultValue != nil {
				currDefault = *currColumn.DefaultValue
			}
			if prevDefault != currDefault {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Default value changed: %s -> %s",
					tableKey, columnName, prevDefault, currDefault))
			}

			// Compare data type details
			c.compareDataType(prevColumn.DataType, currColumn.DataType, tableKey, columnName, result)
		} else {
			// New column
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Added column: %s", tableKey, columnName))
		}
	}
}

func (c *SchemaComparator) compareDataType(prev, curr models.DataType, tableKey, columnName string, result *CompareResult) {
	// Compare basic type properties
	if prev.Length != curr.Length {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Type length changed: %d -> %d",
			tableKey, columnName, prev.Length, curr.Length))
	}
	if prev.Precision != curr.Precision {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Type precision changed: %d -> %d",
			tableKey, columnName, prev.Precision, curr.Precision))
	}
	if prev.Scale != curr.Scale {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Type scale changed: %d -> %d",
			tableKey, columnName, prev.Scale, curr.Scale))
	}

	// Compare array properties
	if prev.IsArray != curr.IsArray {
		arrayStatus := "not an array"
		if curr.IsArray {
			arrayStatus = "an array"
		}
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Array status changed to %s",
			tableKey, columnName, arrayStatus))
	}
	if prev.IsArray && curr.IsArray && prev.ArrayDimensions != curr.ArrayDimensions {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Array dimensions changed: %d -> %d",
			tableKey, columnName, prev.ArrayDimensions, curr.ArrayDimensions))
	}

	// Compare enum properties
	if prev.IsEnum != curr.IsEnum {
		enumStatus := "not an enum"
		if curr.IsEnum {
			enumStatus = "an enum"
		}
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Enum status changed to %s",
			tableKey, columnName, enumStatus))
	}
	if prev.IsEnum && curr.IsEnum {
		// Compare enum values
		if len(prev.EnumValues) != len(curr.EnumValues) {
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Enum values count changed: %d -> %d",
				tableKey, columnName, len(prev.EnumValues), len(curr.EnumValues)))
		} else {
			for i, prevVal := range prev.EnumValues {
				if i < len(curr.EnumValues) && prevVal != curr.EnumValues[i] {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Enum value changed at position %d: %s -> %s",
						tableKey, columnName, i+1, prevVal, curr.EnumValues[i]))
				}
			}
		}
	}

	// Compare domain properties
	if prev.IsDomain != curr.IsDomain {
		domainStatus := "not a domain"
		if curr.IsDomain {
			domainStatus = "a domain"
		}
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Domain status changed to %s",
			tableKey, columnName, domainStatus))
	}
	if prev.IsDomain && curr.IsDomain && prev.DomainBase != nil && curr.DomainBase != nil {
		c.compareDataType(*prev.DomainBase, *curr.DomainBase, tableKey, columnName+" (domain base)", result)
	}

	// Compare composite type properties
	if prev.IsComposite != curr.IsComposite {
		compositeStatus := "not a composite type"
		if curr.IsComposite {
			compositeStatus = "a composite type"
		}
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Composite type status changed to %s",
			tableKey, columnName, compositeStatus))
	}
	if prev.IsComposite && curr.IsComposite {
		// Compare composite fields
		if len(prev.CompositeFields) != len(curr.CompositeFields) {
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Composite fields count changed: %d -> %d",
				tableKey, columnName, len(prev.CompositeFields), len(curr.CompositeFields)))
		} else {
			for i, prevField := range prev.CompositeFields {
				if i < len(curr.CompositeFields) {
					currField := curr.CompositeFields[i]
					if prevField.Name != currField.Name {
						result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Composite field name changed at position %d: %s -> %s",
							tableKey, columnName, i+1, prevField.Name, currField.Name))
					}
					c.compareDataType(prevField.DataType, currField.DataType, tableKey, columnName+"."+currField.Name, result)
				}
			}
		}
	}

	// Compare range type properties
	if prev.IsRange != curr.IsRange {
		rangeStatus := "not a range type"
		if curr.IsRange {
			rangeStatus = "a range type"
		}
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Range type status changed to %s",
			tableKey, columnName, rangeStatus))
	}
	if prev.IsRange && curr.IsRange && prev.RangeSubtype != nil && curr.RangeSubtype != nil {
		c.compareDataType(*prev.RangeSubtype, *curr.RangeSubtype, tableKey, columnName+" (range subtype)", result)
	}

	// Compare extension type properties
	if prev.IsExtension != curr.IsExtension {
		extensionStatus := "not an extension type"
		if curr.IsExtension {
			extensionStatus = "an extension type"
		}
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Extension type status changed to %s",
			tableKey, columnName, extensionStatus))
	}
	if prev.IsExtension && curr.IsExtension && prev.ExtensionName != curr.ExtensionName {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Column %s: Extension name changed: %s -> %s",
			tableKey, columnName, prev.ExtensionName, curr.ExtensionName))
	}
}

func (c *SchemaComparator) compareTableConstraints(prevTable, currTable models.Table, tableKey string, result *CompareResult) {
	// Ensure constraints slices are not nil
	if prevTable.Constraints == nil {
		prevTable.Constraints = []models.Constraint{}
	}
	if currTable.Constraints == nil {
		currTable.Constraints = []models.Constraint{}
	}

	// Create maps for easier lookup
	prevConstraintMap := make(map[string]models.Constraint)
	currConstraintMap := make(map[string]models.Constraint)

	for _, constraint := range prevTable.Constraints {
		prevConstraintMap[constraint.Name] = constraint
	}

	for _, constraint := range currTable.Constraints {
		currConstraintMap[constraint.Name] = constraint
	}

	// Check for removed constraints
	for constraintName := range prevConstraintMap {
		if _, exists := currConstraintMap[constraintName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Removed constraint: %s", tableKey, constraintName))
		}
	}

	// Check for added constraints and compare existing constraints
	for constraintName, currConstraint := range currConstraintMap {
		if prevConstraint, exists := prevConstraintMap[constraintName]; exists {
			// Compare constraint properties
			if prevConstraint.Type != currConstraint.Type {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Type changed: %s -> %s",
					tableKey, constraintName, prevConstraint.Type, currConstraint.Type))
			}
			if prevConstraint.Deferrable != currConstraint.Deferrable {
				deferrableStatus := "not deferrable"
				if currConstraint.Deferrable {
					deferrableStatus = "deferrable"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Deferrable status changed to %s",
					tableKey, constraintName, deferrableStatus))
			}
			if prevConstraint.InitiallyDeferred != currConstraint.InitiallyDeferred {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Initially deferred changed: %s -> %s",
					tableKey, constraintName, prevConstraint.InitiallyDeferred, currConstraint.InitiallyDeferred))
			}
			if prevConstraint.Enabled != currConstraint.Enabled {
				enabledStatus := "disabled"
				if currConstraint.Enabled {
					enabledStatus = "enabled"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Enabled status changed to %s",
					tableKey, constraintName, enabledStatus))
			}
			if prevConstraint.Validated != currConstraint.Validated {
				validatedStatus := "not validated"
				if currConstraint.Validated {
					validatedStatus = "validated"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Validated status changed to %s",
					tableKey, constraintName, validatedStatus))
			}

			// Compare columns involved in the constraint
			if len(prevConstraint.Columns) != len(currConstraint.Columns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Columns count changed: %d -> %d",
					tableKey, constraintName, len(prevConstraint.Columns), len(currConstraint.Columns)))
			} else {
				for i, prevCol := range prevConstraint.Columns {
					if i < len(currConstraint.Columns) && prevCol != currConstraint.Columns[i] {
						result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Column changed at position %d: %s -> %s",
							tableKey, constraintName, i+1, prevCol, currConstraint.Columns[i]))
					}
				}
			}

			// Compare foreign key specific properties
			if prevConstraint.Type == "FOREIGN KEY" && currConstraint.Type == "FOREIGN KEY" {
				if prevConstraint.ReferencedTable != currConstraint.ReferencedTable {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Referenced table changed: %s -> %s",
						tableKey, constraintName, prevConstraint.ReferencedTable, currConstraint.ReferencedTable))
				}
				if len(prevConstraint.ReferencedColumns) != len(currConstraint.ReferencedColumns) {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Referenced columns count changed: %d -> %d",
						tableKey, constraintName, len(prevConstraint.ReferencedColumns), len(currConstraint.ReferencedColumns)))
				} else {
					for i, prevCol := range prevConstraint.ReferencedColumns {
						if i < len(currConstraint.ReferencedColumns) && prevCol != currConstraint.ReferencedColumns[i] {
							result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Referenced column changed at position %d: %s -> %s",
								tableKey, constraintName, i+1, prevCol, currConstraint.ReferencedColumns[i]))
						}
					}
				}
				if prevConstraint.OnUpdate != currConstraint.OnUpdate {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: ON UPDATE action changed: %s -> %s",
						tableKey, constraintName, prevConstraint.OnUpdate, currConstraint.OnUpdate))
				}
				if prevConstraint.OnDelete != currConstraint.OnDelete {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: ON DELETE action changed: %s -> %s",
						tableKey, constraintName, prevConstraint.OnDelete, currConstraint.OnDelete))
				}
			}

			// Compare check constraint specific properties
			if prevConstraint.Type == "CHECK" && currConstraint.Type == "CHECK" {
				if prevConstraint.CheckExpression != currConstraint.CheckExpression {
					result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Constraint %s: Check expression changed",
						tableKey, constraintName))
				}
			}
		} else {
			// New constraint
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Added constraint: %s", tableKey, constraintName))
		}
	}
}

func (c *SchemaComparator) compareTableIndexes(prevTable, currTable models.Table, tableKey string, result *CompareResult) {
	// Ensure indexes slices are not nil
	if prevTable.Indexes == nil {
		prevTable.Indexes = []models.Index{}
	}
	if currTable.Indexes == nil {
		currTable.Indexes = []models.Index{}
	}

	// Create maps for easier lookup
	prevIndexMap := make(map[string]models.Index)
	currIndexMap := make(map[string]models.Index)

	for _, index := range prevTable.Indexes {
		prevIndexMap[index.Name] = index
	}

	for _, index := range currTable.Indexes {
		currIndexMap[index.Name] = index
	}

	// Check for removed indexes
	for indexName := range prevIndexMap {
		if _, exists := currIndexMap[indexName]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Removed index: %s", tableKey, indexName))
		}
	}

	// Check for added indexes and compare existing indexes
	for indexName, currIndex := range currIndexMap {
		if prevIndex, exists := prevIndexMap[indexName]; exists {
			// Compare index properties
			if prevIndex.Schema != currIndex.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Schema changed: %s -> %s",
					tableKey, indexName, prevIndex.Schema, currIndex.Schema))
			}
			if prevIndex.IsUnique != currIndex.IsUnique {
				uniqueStatus := "not unique"
				if currIndex.IsUnique {
					uniqueStatus = "unique"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Uniqueness changed to %s",
					tableKey, indexName, uniqueStatus))
			}
			if prevIndex.IndexMethod != currIndex.IndexMethod {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Index method changed: %s -> %s",
					tableKey, indexName, prevIndex.IndexMethod, currIndex.IndexMethod))
			}
			if prevIndex.WhereClause != currIndex.WhereClause {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Where clause changed",
					tableKey, indexName))
			}
			if prevIndex.Concurrency != currIndex.Concurrency {
				concurrencyStatus := "not concurrent"
				if currIndex.Concurrency {
					concurrencyStatus = "concurrent"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Concurrency changed to %s",
					tableKey, indexName, concurrencyStatus))
			}
			if prevIndex.FillFactor != currIndex.FillFactor {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Fill factor changed: %d -> %d",
					tableKey, indexName, prevIndex.FillFactor, currIndex.FillFactor))
			}
			if prevIndex.Tablespace != currIndex.Tablespace {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Tablespace changed: %s -> %s",
					tableKey, indexName, prevIndex.Tablespace, currIndex.Tablespace))
			}
			if prevIndex.Collation != currIndex.Collation {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Collation changed: %s -> %s",
					tableKey, indexName, prevIndex.Collation, currIndex.Collation))
			}
			if prevIndex.Comment != currIndex.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Comment changed: %s -> %s",
					tableKey, indexName, prevIndex.Comment, currIndex.Comment))
			}
			if prevIndex.Owner != currIndex.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Owner changed: %s -> %s",
					tableKey, indexName, prevIndex.Owner, currIndex.Owner))
			}

			// Compare index columns
			if len(prevIndex.Columns) != len(currIndex.Columns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Columns count changed: %d -> %d",
					tableKey, indexName, len(prevIndex.Columns), len(currIndex.Columns)))
			} else {
				for i, prevCol := range prevIndex.Columns {
					if i < len(currIndex.Columns) {
						currCol := currIndex.Columns[i]
						if prevCol.ColumnName != currCol.ColumnName {
							result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Column name changed at position %d: %s -> %s",
								tableKey, indexName, i+1, prevCol.ColumnName, currCol.ColumnName))
						}
						if prevCol.Order != currCol.Order {
							result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Column order changed at position %d: %d -> %d",
								tableKey, indexName, i+1, prevCol.Order, currCol.Order))
						}
						if prevCol.Expression != currCol.Expression {
							result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Column expression changed at position %d",
								tableKey, indexName, i+1))
						}
						if prevCol.NullPosition != currCol.NullPosition {
							result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Column null position changed at position %d: %d -> %d",
								tableKey, indexName, i+1, prevCol.NullPosition, currCol.NullPosition))
						}
						if prevCol.Length != currCol.Length {
							result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Column length changed at position %d: %d -> %d",
								tableKey, indexName, i+1, prevCol.Length, currCol.Length))
						}
					}
				}
			}

			// Compare include columns
			if len(prevIndex.IncludeColumns) != len(currIndex.IncludeColumns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Include columns count changed: %d -> %d",
					tableKey, indexName, len(prevIndex.IncludeColumns), len(currIndex.IncludeColumns)))
			} else {
				for i, prevCol := range prevIndex.IncludeColumns {
					if i < len(currIndex.IncludeColumns) && prevCol != currIndex.IncludeColumns[i] {
						result.Changes = append(result.Changes, fmt.Sprintf("Table %s, Index %s: Include column changed at position %d: %s -> %s",
							tableKey, indexName, i+1, prevCol, currIndex.IncludeColumns[i]))
					}
				}
			}
		} else {
			// New index
			result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Added index: %s", tableKey, indexName))
		}
	}
}

func (c *SchemaComparator) comparePartitioning(prevTable, currTable models.Table, tableKey string, result *CompareResult) {
	if prevTable.PartitionStrategy != currTable.PartitionStrategy {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Partition strategy changed: %s -> %s",
			tableKey, prevTable.PartitionStrategy, currTable.PartitionStrategy))
	}

	if len(prevTable.PartitionKeys) != len(currTable.PartitionKeys) {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Partition keys count changed: %d -> %d",
			tableKey, len(prevTable.PartitionKeys), len(currTable.PartitionKeys)))
	} else {
		for i, prevKey := range prevTable.PartitionKeys {
			if i < len(currTable.PartitionKeys) && prevKey != currTable.PartitionKeys[i] {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Partition key changed at position %d: %s -> %s",
					tableKey, i+1, prevKey, currTable.PartitionKeys[i]))
			}
		}
	}

	if len(prevTable.Partitions) != len(currTable.Partitions) {
		result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Partitions count changed: %d -> %d",
			tableKey, len(prevTable.Partitions), len(currTable.Partitions)))
	} else {
		for i, prevPartition := range prevTable.Partitions {
			if i < len(currTable.Partitions) && prevPartition != currTable.Partitions[i] {
				result.Changes = append(result.Changes, fmt.Sprintf("Table %s: Partition changed at position %d: %s -> %s",
					tableKey, i+1, prevPartition, currTable.Partitions[i]))
			}
		}
	}
}

func (c *SchemaComparator) compareEnums(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevEnumMap := make(map[string]models.Enum)
	currEnumMap := make(map[string]models.Enum)

	for _, enum := range prev.Enums {
		key := enum.Schema + "." + enum.Name
		prevEnumMap[key] = enum
	}

	for _, enum := range curr.Enums {
		key := enum.Schema + "." + enum.Name
		currEnumMap[key] = enum
	}

	// Check for removed enums
	for enumKey := range prevEnumMap {
		if _, exists := currEnumMap[enumKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed enum: %s", enumKey))
		}
	}

	// Check for added enums and compare existing enums
	for enumKey, currEnum := range currEnumMap {
		if prevEnum, exists := prevEnumMap[enumKey]; exists {
			// Compare enum properties
			if prevEnum.Schema != currEnum.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Enum %s schema changed: %s -> %s",
					enumKey, prevEnum.Schema, currEnum.Schema))
			}
			if prevEnum.IsExtensible != currEnum.IsExtensible {
				extensibleStatus := "not extensible"
				if currEnum.IsExtensible {
					extensibleStatus = "extensible"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Enum %s extensibility changed to %s",
					enumKey, extensibleStatus))
			}
			if prevEnum.Collation != currEnum.Collation {
				result.Changes = append(result.Changes, fmt.Sprintf("Enum %s collation changed: %s -> %s",
					enumKey, prevEnum.Collation, currEnum.Collation))
			}
			if prevEnum.Comment != currEnum.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Enum %s comment changed: %s -> %s",
					enumKey, prevEnum.Comment, currEnum.Comment))
			}
			if prevEnum.Owner != currEnum.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Enum %s owner changed: %s -> %s",
					enumKey, prevEnum.Owner, currEnum.Owner))
			}

			// Compare enum values
			if len(prevEnum.Values) != len(currEnum.Values) {
				result.Changes = append(result.Changes, fmt.Sprintf("Enum %s values count changed: %d -> %d",
					enumKey, len(prevEnum.Values), len(currEnum.Values)))
			} else {
				for i, prevVal := range prevEnum.Values {
					if i < len(currEnum.Values) && prevVal != currEnum.Values[i] {
						result.Changes = append(result.Changes, fmt.Sprintf("Enum %s value changed at position %d: %s -> %s",
							enumKey, i+1, prevVal, currEnum.Values[i]))
					}
				}
			}
		} else {
			// New enum
			result.Changes = append(result.Changes, fmt.Sprintf("Added enum: %s", enumKey))
		}
	}
}

func (c *SchemaComparator) compareCompositeTypes(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevTypeMap := make(map[string]models.CompositeType)
	currTypeMap := make(map[string]models.CompositeType)

	for _, t := range prev.CompositeTypes {
		key := t.Schema + "." + t.Name
		prevTypeMap[key] = t
	}

	for _, t := range curr.CompositeTypes {
		key := t.Schema + "." + t.Name
		currTypeMap[key] = t
	}

	// Check for removed composite types
	for typeKey := range prevTypeMap {
		if _, exists := currTypeMap[typeKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed composite type: %s", typeKey))
		}
	}

	// Check for added composite types and compare existing ones
	for typeKey, currType := range currTypeMap {
		if prevType, exists := prevTypeMap[typeKey]; exists {
			// Compare composite type properties
			if prevType.Schema != currType.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s schema changed: %s -> %s",
					typeKey, prevType.Schema, currType.Schema))
			}
			if prevType.Comment != currType.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s comment changed: %s -> %s",
					typeKey, prevType.Comment, currType.Comment))
			}
			if prevType.Owner != currType.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s owner changed: %s -> %s",
					typeKey, prevType.Owner, currType.Owner))
			}

			// Compare fields
			if len(prevType.Fields) != len(currType.Fields) {
				result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s fields count changed: %d -> %d",
					typeKey, len(prevType.Fields), len(currType.Fields)))
			} else {
				for i, prevField := range prevType.Fields {
					if i < len(currType.Fields) {
						currField := currType.Fields[i]
						if prevField.Name != currField.Name {
							result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s field name changed at position %d: %s -> %s",
								typeKey, i+1, prevField.Name, currField.Name))
						}
						if prevField.Collation != currField.Collation {
							result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s field %s collation changed: %s -> %s",
								typeKey, currField.Name, prevField.Collation, currField.Collation))
						}
						if prevField.IsNullable != currField.IsNullable {
							nullableStatus := "NOT NULL"
							if currField.IsNullable {
								nullableStatus = "NULL"
							}
							result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s field %s nullability changed to %s",
								typeKey, currField.Name, nullableStatus))
						}
						if prevField.DefaultValue != currField.DefaultValue {
							result.Changes = append(result.Changes, fmt.Sprintf("Composite type %s field %s default value changed: %s -> %s",
								typeKey, currField.Name, prevField.DefaultValue, currField.DefaultValue))
						}
						c.compareDataType(prevField.DataType, currField.DataType, typeKey, currField.Name, result)
					}
				}
			}
		} else {
			// New composite type
			result.Changes = append(result.Changes, fmt.Sprintf("Added composite type: %s", typeKey))
		}
	}
}

func (c *SchemaComparator) compareDomains(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevDomainMap := make(map[string]models.Domain)
	currDomainMap := make(map[string]models.Domain)

	for _, domain := range prev.Domains {
		key := domain.Schema + "." + domain.Name
		prevDomainMap[key] = domain
	}

	for _, domain := range curr.Domains {
		key := domain.Schema + "." + domain.Name
		currDomainMap[key] = domain
	}

	// Check for removed domains
	for domainKey := range prevDomainMap {
		if _, exists := currDomainMap[domainKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed domain: %s", domainKey))
		}
	}

	// Check for added domains and compare existing domains
	for domainKey, currDomain := range currDomainMap {
		if prevDomain, exists := prevDomainMap[domainKey]; exists {
			// Compare domain properties
			if prevDomain.Schema != currDomain.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s schema changed: %s -> %s",
					domainKey, prevDomain.Schema, currDomain.Schema))
			}
			if prevDomain.BaseType != currDomain.BaseType {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s base type changed: %s -> %s",
					domainKey, prevDomain.BaseType, currDomain.BaseType))
			}
			if prevDomain.Collation != currDomain.Collation {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s collation changed: %s -> %s",
					domainKey, prevDomain.Collation, currDomain.Collation))
			}
			if prevDomain.IsNullable != currDomain.IsNullable {
				nullableStatus := "NOT NULL"
				if currDomain.IsNullable {
					nullableStatus = "NULL"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s nullability changed to %s",
					domainKey, nullableStatus))
			}
			if prevDomain.ColumnDefault != currDomain.ColumnDefault {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s default value changed: %s -> %s",
					domainKey, prevDomain.ColumnDefault, currDomain.ColumnDefault))
			}
			if prevDomain.CheckConstraint != currDomain.CheckConstraint {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s check constraint changed",
					domainKey))
			}
			if prevDomain.Comment != currDomain.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s comment changed: %s -> %s",
					domainKey, prevDomain.Comment, currDomain.Comment))
			}
			if prevDomain.Owner != currDomain.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Domain %s owner changed: %s -> %s",
					domainKey, prevDomain.Owner, currDomain.Owner))
			}
		} else {
			// New domain
			result.Changes = append(result.Changes, fmt.Sprintf("Added domain: %s", domainKey))
		}
	}
}

func (c *SchemaComparator) compareRanges(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevRangeMap := make(map[string]models.Range)
	currRangeMap := make(map[string]models.Range)

	for _, r := range prev.Ranges {
		key := r.Schema + "." + r.Name
		prevRangeMap[key] = r
	}

	for _, r := range curr.Ranges {
		key := r.Schema + "." + r.Name
		currRangeMap[key] = r
	}

	// Check for removed ranges
	for rangeKey := range prevRangeMap {
		if _, exists := currRangeMap[rangeKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed range: %s", rangeKey))
		}
	}

	// Check for added ranges and compare existing ranges
	for rangeKey, currRange := range currRangeMap {
		if prevRange, exists := prevRangeMap[rangeKey]; exists {
			// Compare range properties
			if prevRange.Schema != currRange.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s schema changed: %s -> %s",
					rangeKey, prevRange.Schema, currRange.Schema))
			}
			if prevRange.Subtype != currRange.Subtype {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s subtype changed: %s -> %s",
					rangeKey, prevRange.Subtype, currRange.Subtype))
			}
			if prevRange.CanonicalFunction != currRange.CanonicalFunction {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s canonical function changed: %s -> %s",
					rangeKey, prevRange.CanonicalFunction, currRange.CanonicalFunction))
			}
			if prevRange.SubtypeDiffFunction != currRange.SubtypeDiffFunction {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s subtype diff function changed: %s -> %s",
					rangeKey, prevRange.SubtypeDiffFunction, currRange.SubtypeDiffFunction))
			}
			if prevRange.MultirangeType != currRange.MultirangeType {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s multirange type changed: %s -> %s",
					rangeKey, prevRange.MultirangeType, currRange.MultirangeType))
			}
			if prevRange.Comment != currRange.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s comment changed: %s -> %s",
					rangeKey, prevRange.Comment, currRange.Comment))
			}
			if prevRange.Owner != currRange.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Range %s owner changed: %s -> %s",
					rangeKey, prevRange.Owner, currRange.Owner))
			}
		} else {
			// New range
			result.Changes = append(result.Changes, fmt.Sprintf("Added range: %s", rangeKey))
		}
	}
}

func (c *SchemaComparator) compareFunctions(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevFunctionMap := make(map[string]models.Function)
	currFunctionMap := make(map[string]models.Function)

	for _, f := range prev.Functions {
		key := f.Schema + "." + f.Name
		prevFunctionMap[key] = f
	}

	for _, f := range curr.Functions {
		key := f.Schema + "." + f.Name
		currFunctionMap[key] = f
	}

	// Check for removed functions
	for functionKey := range prevFunctionMap {
		if _, exists := currFunctionMap[functionKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed function: %s", functionKey))
		}
	}

	// Check for added functions and compare existing functions
	for functionKey, currFunction := range currFunctionMap {
		if prevFunction, exists := prevFunctionMap[functionKey]; exists {
			// Compare function properties
			if prevFunction.Schema != currFunction.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s schema changed: %s -> %s",
					functionKey, prevFunction.Schema, currFunction.Schema))
			}
			if prevFunction.ReturnType != currFunction.ReturnType {
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s return type changed: %s -> %s",
					functionKey, prevFunction.ReturnType, currFunction.ReturnType))
			}
			if prevFunction.IsDeterministic != currFunction.IsDeterministic {
				deterministicStatus := "not deterministic"
				if currFunction.IsDeterministic {
					deterministicStatus = "deterministic"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s determinism changed to %s",
					functionKey, deterministicStatus))
			}
			if prevFunction.Definition != currFunction.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s definition changed",
					functionKey))
			}

			// Compare arguments
			if len(prevFunction.Arguments) != len(currFunction.Arguments) {
				result.Changes = append(result.Changes, fmt.Sprintf("Function %s arguments count changed: %d -> %d",
					functionKey, len(prevFunction.Arguments), len(currFunction.Arguments)))
			} else {
				for i, prevArg := range prevFunction.Arguments {
					if i < len(currFunction.Arguments) {
						currArg := currFunction.Arguments[i]
						if prevArg.Name != currArg.Name {
							result.Changes = append(result.Changes, fmt.Sprintf("Function %s argument name changed at position %d: %s -> %s",
								functionKey, i+1, prevArg.Name, currArg.Name))
						}
						if prevArg.DataType != currArg.DataType {
							result.Changes = append(result.Changes, fmt.Sprintf("Function %s argument %s data type changed: %s -> %s",
								functionKey, currArg.Name, prevArg.DataType, currArg.DataType))
						}
					}
				}
			}
		} else {
			// New function
			result.Changes = append(result.Changes, fmt.Sprintf("Added function: %s", functionKey))
		}
	}
}

func (c *SchemaComparator) compareTriggers(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevTriggerMap := make(map[string]models.Trigger)
	currTriggerMap := make(map[string]models.Trigger)

	for _, t := range prev.Triggers {
		key := t.Schema + "." + t.Name
		prevTriggerMap[key] = t
	}

	for _, t := range curr.Triggers {
		key := t.Schema + "." + t.Name
		currTriggerMap[key] = t
	}

	// Check for removed triggers
	for triggerKey := range prevTriggerMap {
		if _, exists := currTriggerMap[triggerKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed trigger: %s", triggerKey))
		}
	}

	// Check for added triggers and compare existing triggers
	for triggerKey, currTrigger := range currTriggerMap {
		if prevTrigger, exists := prevTriggerMap[triggerKey]; exists {
			// Compare trigger properties
			if prevTrigger.Schema != currTrigger.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s schema changed: %s -> %s",
					triggerKey, prevTrigger.Schema, currTrigger.Schema))
			}
			if prevTrigger.Table != currTrigger.Table {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s table changed: %s -> %s",
					triggerKey, prevTrigger.Table, currTrigger.Table))
			}
			if prevTrigger.Event != currTrigger.Event {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s event changed: %s -> %s",
					triggerKey, prevTrigger.Event, currTrigger.Event))
			}
			if prevTrigger.Definition != currTrigger.Definition {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s definition changed",
					triggerKey))
			}
			if prevTrigger.Timing != currTrigger.Timing {
				result.Changes = append(result.Changes, fmt.Sprintf("Trigger %s timing changed: %s -> %s",
					triggerKey, prevTrigger.Timing, currTrigger.Timing))
			}
		} else {
			// New trigger
			result.Changes = append(result.Changes, fmt.Sprintf("Added trigger: %s", triggerKey))
		}
	}
}

func (c *SchemaComparator) compareSequences(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevSequenceMap := make(map[string]models.Sequence)
	currSequenceMap := make(map[string]models.Sequence)

	for _, s := range prev.Sequences {
		key := s.Schema + "." + s.Name
		prevSequenceMap[key] = s
	}

	for _, s := range curr.Sequences {
		key := s.Schema + "." + s.Name
		currSequenceMap[key] = s
	}

	// Check for removed sequences
	for sequenceKey := range prevSequenceMap {
		if _, exists := currSequenceMap[sequenceKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed sequence: %s", sequenceKey))
		}
	}

	// Check for added sequences and compare existing sequences
	for sequenceKey, currSequence := range currSequenceMap {
		if prevSequence, exists := prevSequenceMap[sequenceKey]; exists {
			// Compare sequence properties
			if prevSequence.Schema != currSequence.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s schema changed: %s -> %s",
					sequenceKey, prevSequence.Schema, currSequence.Schema))
			}
			if prevSequence.DataType != currSequence.DataType {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s data type changed: %s -> %s",
					sequenceKey, prevSequence.DataType, currSequence.DataType))
			}
			if prevSequence.Start != currSequence.Start {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s start value changed: %d -> %d",
					sequenceKey, prevSequence.Start, currSequence.Start))
			}
			if prevSequence.Increment != currSequence.Increment {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s increment changed: %d -> %d",
					sequenceKey, prevSequence.Increment, currSequence.Increment))
			}
			if prevSequence.MaxValue != currSequence.MaxValue {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s max value changed: %d -> %d",
					sequenceKey, prevSequence.MaxValue, currSequence.MaxValue))
			}
			if prevSequence.MinValue != currSequence.MinValue {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s min value changed: %d -> %d",
					sequenceKey, prevSequence.MinValue, currSequence.MinValue))
			}
			if prevSequence.CacheSize != currSequence.CacheSize {
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s cache size changed: %d -> %d",
					sequenceKey, prevSequence.CacheSize, currSequence.CacheSize))
			}
			if prevSequence.Cycle != currSequence.Cycle {
				cycleStatus := "not cycling"
				if currSequence.Cycle {
					cycleStatus = "cycling"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Sequence %s cycle status changed to %s",
					sequenceKey, cycleStatus))
			}
		} else {
			// New sequence
			result.Changes = append(result.Changes, fmt.Sprintf("Added sequence: %s", sequenceKey))
		}
	}
}

func (c *SchemaComparator) compareExtensions(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevExtensionMap := make(map[string]models.Extension)
	currExtensionMap := make(map[string]models.Extension)

	for _, e := range prev.Extensions {
		key := e.Schema + "." + e.Name
		prevExtensionMap[key] = e
	}

	for _, e := range curr.Extensions {
		key := e.Schema + "." + e.Name
		currExtensionMap[key] = e
	}

	// Check for removed extensions
	for extensionKey := range prevExtensionMap {
		if _, exists := currExtensionMap[extensionKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed extension: %s", extensionKey))
		}
	}

	// Check for added extensions and compare existing extensions
	for extensionKey, currExtension := range currExtensionMap {
		if prevExtension, exists := prevExtensionMap[extensionKey]; exists {
			// Compare extension properties
			if prevExtension.Schema != currExtension.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Extension %s schema changed: %s -> %s",
					extensionKey, prevExtension.Schema, currExtension.Schema))
			}
			if prevExtension.Version != currExtension.Version {
				result.Changes = append(result.Changes, fmt.Sprintf("Extension %s version changed: %s -> %s",
					extensionKey, prevExtension.Version, currExtension.Version))
			}
			if prevExtension.Description != currExtension.Description {
				result.Changes = append(result.Changes, fmt.Sprintf("Extension %s description changed: %s -> %s",
					extensionKey, prevExtension.Description, currExtension.Description))
			}
		} else {
			// New extension
			result.Changes = append(result.Changes, fmt.Sprintf("Added extension: %s", extensionKey))
		}
	}
}

func (c *SchemaComparator) compareGlobalIndexes(prev, curr *models.UnifiedModel, result *CompareResult) {
	// Create maps for easier lookup
	prevIndexMap := make(map[string]models.Index)
	currIndexMap := make(map[string]models.Index)

	for _, i := range prev.Indexes {
		key := i.Schema + "." + i.Name
		prevIndexMap[key] = i
	}

	for _, i := range curr.Indexes {
		key := i.Schema + "." + i.Name
		currIndexMap[key] = i
	}

	// Check for removed indexes
	for indexKey := range prevIndexMap {
		if _, exists := currIndexMap[indexKey]; !exists {
			result.Changes = append(result.Changes, fmt.Sprintf("Removed global index: %s", indexKey))
		}
	}

	// Check for added indexes and compare existing indexes
	for indexKey, currIndex := range currIndexMap {
		if prevIndex, exists := prevIndexMap[indexKey]; exists {
			// Compare index properties
			if prevIndex.Schema != currIndex.Schema {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s schema changed: %s -> %s",
					indexKey, prevIndex.Schema, currIndex.Schema))
			}
			if prevIndex.Table != currIndex.Table {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s table changed: %s -> %s",
					indexKey, prevIndex.Table, currIndex.Table))
			}
			if prevIndex.IsUnique != currIndex.IsUnique {
				uniqueStatus := "not unique"
				if currIndex.IsUnique {
					uniqueStatus = "unique"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s uniqueness changed to %s",
					indexKey, uniqueStatus))
			}
			if prevIndex.IndexMethod != currIndex.IndexMethod {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s method changed: %s -> %s",
					indexKey, prevIndex.IndexMethod, currIndex.IndexMethod))
			}
			if prevIndex.WhereClause != currIndex.WhereClause {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s where clause changed",
					indexKey))
			}
			if prevIndex.Concurrency != currIndex.Concurrency {
				concurrencyStatus := "not concurrent"
				if currIndex.Concurrency {
					concurrencyStatus = "concurrent"
				}
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s concurrency changed to %s",
					indexKey, concurrencyStatus))
			}
			if prevIndex.FillFactor != currIndex.FillFactor {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s fill factor changed: %d -> %d",
					indexKey, prevIndex.FillFactor, currIndex.FillFactor))
			}
			if prevIndex.Tablespace != currIndex.Tablespace {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s tablespace changed: %s -> %s",
					indexKey, prevIndex.Tablespace, currIndex.Tablespace))
			}
			if prevIndex.Collation != currIndex.Collation {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s collation changed: %s -> %s",
					indexKey, prevIndex.Collation, currIndex.Collation))
			}
			if prevIndex.Comment != currIndex.Comment {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s comment changed: %s -> %s",
					indexKey, prevIndex.Comment, currIndex.Comment))
			}
			if prevIndex.Owner != currIndex.Owner {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s owner changed: %s -> %s",
					indexKey, prevIndex.Owner, currIndex.Owner))
			}

			// Compare index columns
			if len(prevIndex.Columns) != len(currIndex.Columns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s columns count changed: %d -> %d",
					indexKey, len(prevIndex.Columns), len(currIndex.Columns)))
			} else {
				for i, prevCol := range prevIndex.Columns {
					if i < len(currIndex.Columns) {
						currCol := currIndex.Columns[i]
						if prevCol.ColumnName != currCol.ColumnName {
							result.Changes = append(result.Changes, fmt.Sprintf("Global index %s column name changed at position %d: %s -> %s",
								indexKey, i+1, prevCol.ColumnName, currCol.ColumnName))
						}
						if prevCol.Order != currCol.Order {
							result.Changes = append(result.Changes, fmt.Sprintf("Global index %s column order changed at position %d: %d -> %d",
								indexKey, i+1, prevCol.Order, currCol.Order))
						}
						if prevCol.Expression != currCol.Expression {
							result.Changes = append(result.Changes, fmt.Sprintf("Global index %s column expression changed at position %d",
								indexKey, i+1))
						}
						if prevCol.NullPosition != currCol.NullPosition {
							result.Changes = append(result.Changes, fmt.Sprintf("Global index %s column null position changed at position %d: %d -> %d",
								indexKey, i+1, prevCol.NullPosition, currCol.NullPosition))
						}
						if prevCol.Length != currCol.Length {
							result.Changes = append(result.Changes, fmt.Sprintf("Global index %s column length changed at position %d: %d -> %d",
								indexKey, i+1, prevCol.Length, currCol.Length))
						}
					}
				}
			}

			// Compare include columns
			if len(prevIndex.IncludeColumns) != len(currIndex.IncludeColumns) {
				result.Changes = append(result.Changes, fmt.Sprintf("Global index %s include columns count changed: %d -> %d",
					indexKey, len(prevIndex.IncludeColumns), len(currIndex.IncludeColumns)))
			} else {
				for i, prevCol := range prevIndex.IncludeColumns {
					if i < len(currIndex.IncludeColumns) && prevCol != currIndex.IncludeColumns[i] {
						result.Changes = append(result.Changes, fmt.Sprintf("Global index %s include column changed at position %d: %s -> %s",
							indexKey, i+1, prevCol, currIndex.IncludeColumns[i]))
					}
				}
			}
		} else {
			// New index
			result.Changes = append(result.Changes, fmt.Sprintf("Added global index: %s", indexKey))
		}
	}
}
