package adapters

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type CockroachIngester struct{}

func (c *CockroachIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var crSchema models.CockroachSchema
	if err := json.Unmarshal(rawSchema, &crSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert schemas
	for _, schema := range crSchema.Schemas {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name:         schema.Name,
			CharacterSet: schema.CharacterSet,
			Collation:    schema.Collation,
		})
	}

	// Convert tables
	for _, table := range crSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertCockroachTableType(table.TableType),
			Owner:     table.Owner,
		}

		// Convert columns
		for _, col := range table.Columns {
			unifiedCol := models.Column{
				Name:                 col.Name,
				IsNullable:           col.IsNullable,
				IsPrimaryKey:         col.IsPrimaryKey,
				IsUnique:             col.IsUnique,
				IsAutoIncrement:      col.IsAutoIncrement,
				IsGenerated:          col.IsGenerated,
				DefaultIsFunction:    col.DefaultIsFunction,
				DefaultValueFunction: col.DefaultValueFunction,
				DefaultValue:         col.DefaultValue,
				Collation:            col.Collation,
			}

			// Parse the data type
			unifiedCol.DataType = parseCockroachType(col.DataType, &warnings)

			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Convert constraints
		for _, constraint := range table.Constraints {
			unifiedTable.Constraints = append(unifiedTable.Constraints, models.Constraint{
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
			})
		}

		// Convert indexes
		for _, index := range table.Indexes {
			unifiedTable.Indexes = append(unifiedTable.Indexes, models.Index{
				Name:           index.Name,
				Schema:         index.Schema,
				Table:          index.Table,
				Columns:        convertIndexColumns(index.Columns),
				IncludeColumns: index.IncludeColumns,
				IsUnique:       index.IsUnique,
				IndexMethod:    index.IndexMethod,
				WhereClause:    index.WhereClause,
				Concurrency:    index.Concurrency,
				FillFactor:     index.FillFactor,
				Tablespace:     index.Tablespace,
				Collation:      index.Collation,
				Comment:        index.Comment,
				Owner:          index.Owner,
			})
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert enum types
	for _, enum := range crSchema.EnumTypes {
		unifiedModel.Enums = append(unifiedModel.Enums, models.Enum{
			Name:         enum.Name,
			Values:       enum.Values,
			Schema:       enum.Schema,
			IsExtensible: enum.IsExtensible,
			Collation:    enum.Collation,
			Comment:      enum.Comment,
			Owner:        enum.Owner,
		})
	}

	// Convert functions
	for _, function := range crSchema.Functions {
		unifiedModel.Functions = append(unifiedModel.Functions, models.Function{
			Name:            function.Name,
			Schema:          function.Schema,
			Arguments:       convertFunctionParameters(function.Arguments),
			ReturnType:      function.ReturnType,
			IsDeterministic: function.IsDeterministic,
			Definition:      function.Definition,
		})
	}

	// Convert triggers
	for _, trigger := range crSchema.Triggers {
		unifiedModel.Triggers = append(unifiedModel.Triggers, models.Trigger{
			Name:       trigger.Name,
			Schema:     trigger.Schema,
			Table:      trigger.Table,
			Event:      trigger.Event,
			Definition: trigger.Definition,
			Timing:     trigger.Timing,
		})
	}

	// Convert sequences
	for _, sequence := range crSchema.Sequences {
		unifiedModel.Sequences = append(unifiedModel.Sequences, models.Sequence{
			Name:      sequence.Name,
			Schema:    sequence.Schema,
			DataType:  sequence.DataType,
			Start:     sequence.Start,
			Increment: sequence.Increment,
			MaxValue:  sequence.MaxValue,
			MinValue:  sequence.MinValue,
			CacheSize: sequence.CacheSize,
			Cycle:     sequence.Cycle,
		})
	}

	// Convert extensions
	for _, extension := range crSchema.Extensions {
		unifiedModel.Extensions = append(unifiedModel.Extensions, models.Extension{
			Name:        extension.Name,
			Schema:      extension.Schema,
			Version:     extension.Version,
			Description: extension.Description,
		})
	}

	return unifiedModel, warnings, nil
}

type CockroachExporter struct{}

func (c *CockroachExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	crSchema := models.CockroachSchema{
		SchemaType: "cockroach",
	}
	warnings := []string{}

	// Convert schemas
	for _, schema := range model.Schemas {
		crSchema.Schemas = append(crSchema.Schemas, models.Schema{
			Name:         schema.Name,
			CharacterSet: schema.CharacterSet,
			Collation:    schema.Collation,
		})
	}

	// Convert tables
	for _, table := range model.Tables {
		crTable := models.CockroachTable{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertToCockroachTableType(table.TableType),
			Owner:     table.Owner,
		}

		// Convert columns
		for _, col := range table.Columns {
			crCol := models.CockroachColumn{
				Name:                 col.Name,
				IsNullable:           col.IsNullable,
				IsPrimaryKey:         col.IsPrimaryKey,
				IsUnique:             col.IsUnique,
				IsAutoIncrement:      col.IsAutoIncrement,
				IsGenerated:          col.IsGenerated,
				DefaultIsFunction:    col.DefaultIsFunction,
				DefaultValueFunction: col.DefaultValueFunction,
				DefaultValue:         col.DefaultValue,
				Collation:            col.Collation,
				DataType:             convertToCockroachType(col.DataType),
			}
			crTable.Columns = append(crTable.Columns, crCol)
		}

		// Convert constraints
		for _, constraint := range table.Constraints {
			crTable.Constraints = append(crTable.Constraints, models.Constraint{
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
			})
		}

		// Convert indexes
		for _, index := range table.Indexes {
			crTable.Indexes = append(crTable.Indexes, models.Index{
				Name:           index.Name,
				Schema:         index.Schema,
				Table:          index.Table,
				Columns:        convertToCockroachIndexColumns(index.Columns),
				IncludeColumns: index.IncludeColumns,
				IsUnique:       index.IsUnique,
				IndexMethod:    index.IndexMethod,
				WhereClause:    index.WhereClause,
				Concurrency:    index.Concurrency,
				FillFactor:     index.FillFactor,
				Tablespace:     index.Tablespace,
				Collation:      index.Collation,
				Comment:        index.Comment,
				Owner:          index.Owner,
			})
		}

		crSchema.Tables = append(crSchema.Tables, crTable)
	}

	// Convert enum types
	for _, enum := range model.Enums {
		crSchema.EnumTypes = append(crSchema.EnumTypes, models.Enum{
			Name:         enum.Name,
			Values:       enum.Values,
			Schema:       enum.Schema,
			IsExtensible: enum.IsExtensible,
			Collation:    enum.Collation,
			Comment:      enum.Comment,
			Owner:        enum.Owner,
		})
	}

	// Convert functions
	for _, function := range model.Functions {
		crSchema.Functions = append(crSchema.Functions, models.Function{
			Name:            function.Name,
			Schema:          function.Schema,
			Arguments:       convertToCockroachFunctionParameters(function.Arguments),
			ReturnType:      function.ReturnType,
			IsDeterministic: function.IsDeterministic,
			Definition:      function.Definition,
		})
	}

	// Convert triggers
	for _, trigger := range model.Triggers {
		crSchema.Triggers = append(crSchema.Triggers, models.Trigger{
			Name:       trigger.Name,
			Schema:     trigger.Schema,
			Table:      trigger.Table,
			Event:      trigger.Event,
			Definition: trigger.Definition,
			Timing:     trigger.Timing,
		})
	}

	// Convert sequences
	for _, sequence := range model.Sequences {
		crSchema.Sequences = append(crSchema.Sequences, models.Sequence{
			Name:      sequence.Name,
			Schema:    sequence.Schema,
			DataType:  sequence.DataType,
			Start:     sequence.Start,
			Increment: sequence.Increment,
			MaxValue:  sequence.MaxValue,
			MinValue:  sequence.MinValue,
			CacheSize: sequence.CacheSize,
			Cycle:     sequence.Cycle,
		})
	}

	// Convert extensions
	for _, extension := range model.Extensions {
		crSchema.Extensions = append(crSchema.Extensions, models.Extension{
			Name:        extension.Name,
			Schema:      extension.Schema,
			Version:     extension.Version,
			Description: extension.Description,
		})
	}

	return crSchema, warnings, nil
}

// Helper functions for type conversion
func convertCockroachTableType(crTableType string) string {
	switch crTableType {
	case "table":
		return "standard"
	case "view":
		return "view"
	case "materialized_view":
		return "materialized"
	default:
		return "standard"
	}
}

func convertToCockroachTableType(unifiedTableType string) string {
	switch unifiedTableType {
	case "standard":
		return "table"
	case "view":
		return "view"
	case "materialized":
		return "materialized_view"
	default:
		return "table"
	}
}

func parseCockroachType(crType string, warnings *[]string) models.DataType {
	dt := models.DataType{
		Name:         crType,
		TypeCategory: "basic",
		BaseType:     crType,
	}

	// Handle array types
	if strings.HasSuffix(crType, "[]") {
		dt.IsArray = true
		dt.BaseType = strings.TrimSuffix(crType, "[]")
		dt.TypeCategory = "array"
	}

	// Handle enum types
	if strings.HasPrefix(crType, "enum.") {
		dt.IsEnum = true
		dt.TypeCategory = "enum"
		dt.Name = crType
		dt.BaseType = strings.TrimPrefix(crType, "enum.")
	}

	// Handle domain types
	if strings.HasPrefix(crType, "domain.") {
		dt.IsDomain = true
		dt.TypeCategory = "domain"
		dt.Name = crType
		dt.BaseType = strings.TrimPrefix(crType, "domain.")
	}

	// Handle composite types
	if strings.HasPrefix(crType, "composite.") {
		dt.IsComposite = true
		dt.TypeCategory = "composite"
		dt.Name = crType
		dt.BaseType = strings.TrimPrefix(crType, "composite.")
	}

	// Handle range types
	if strings.HasPrefix(crType, "range.") {
		dt.IsRange = true
		dt.TypeCategory = "range"
		dt.Name = crType
		dt.BaseType = strings.TrimPrefix(crType, "range.")
	}

	// Extract length, precision, and scale
	if strings.Contains(crType, "(") {
		parts := strings.Split(crType, "(")
		baseType := parts[0]
		dt.BaseType = baseType

		if len(parts) > 1 {
			params := strings.TrimSuffix(parts[1], ")")
			paramParts := strings.Split(params, ",")

			// Handle length for types like varchar(n)
			if len(paramParts) > 0 {
				length, err := strconv.Atoi(paramParts[0])
				if err == nil {
					dt.Length = length
				}
			}

			// Handle precision and scale for types like decimal(p,s)
			if len(paramParts) > 1 {
				precision, err := strconv.Atoi(paramParts[0])
				if err == nil {
					dt.Precision = precision
				}

				scale, err := strconv.Atoi(paramParts[1])
				if err == nil {
					dt.Scale = scale
				}
			}
		}
	}

	return dt
}

func convertToCockroachType(dt models.DataType) string {
	if dt.IsArray {
		return dt.BaseType + "[]"
	}

	if dt.IsEnum {
		return "enum." + dt.BaseType
	}

	if dt.IsDomain {
		return "domain." + dt.BaseType
	}

	if dt.IsComposite {
		return "composite." + dt.BaseType
	}

	if dt.IsRange {
		return "range." + dt.BaseType
	}

	// Handle types with length, precision, or scale
	if dt.Length > 0 {
		return fmt.Sprintf("%s(%d)", dt.BaseType, dt.Length)
	}

	if dt.Precision > 0 && dt.Scale > 0 {
		return fmt.Sprintf("%s(%d,%d)", dt.BaseType, dt.Precision, dt.Scale)
	}

	return dt.BaseType
}

func convertIndexColumns(columns []models.IndexColumn) []models.IndexColumn {
	return columns
}

func convertToCockroachIndexColumns(columns []models.IndexColumn) []models.IndexColumn {
	return columns
}

func convertFunctionParameters(params []models.FunctionParameter) []models.FunctionParameter {
	return params
}

func convertToCockroachFunctionParameters(params []models.FunctionParameter) []models.FunctionParameter {
	return params
}
