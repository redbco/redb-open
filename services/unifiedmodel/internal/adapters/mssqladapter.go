package adapters

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// MSSQLIngester implements the Ingester interface for MSSQL
type MSSQLIngester struct{}

// IngestSchema converts a MSSQL schema to a UnifiedModel
func (m *MSSQLIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var mssqlSchema models.MSSQLSchema
	if err := json.Unmarshal(rawSchema, &mssqlSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{
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
	warnings := []string{}

	// Convert schemas
	for _, schema := range mssqlSchema.Schemas {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name:         schema.Name,
			CharacterSet: "", // MS-SQL doesn't have schema-level character sets
			Collation:    "", // MS-SQL doesn't have schema-level collations
		})
	}

	// Convert tables
	for _, table := range mssqlSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertMSSQLTableType(table.TableType),
			Columns:   []models.Column{},
			Indexes:   []models.Index{},
		}

		// Convert columns
		for _, col := range table.Columns {
			unifiedCol := models.Column{
				Name:            col.Name,
				IsNullable:      col.IsNullable,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsUnique:        col.IsUnique,
				IsAutoIncrement: col.IsAutoIncrement,
				DefaultValue:    col.ColumnDefault,
				DataType:        parseMSSQLType(col.DataType, &warnings),
			}
			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Convert constraints
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
			}
			unifiedTable.Constraints = append(unifiedTable.Constraints, unifiedConstraint)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert functions
	for _, fn := range mssqlSchema.Functions {
		unifiedFunction := models.Function{
			Name:            fn.Name,
			Schema:          fn.Schema,
			Arguments:       []models.FunctionParameter{},
			ReturnType:      fn.ReturnType,
			IsDeterministic: fn.IsDeterministic,
			Definition:      fn.Definition,
		}

		// Convert function parameters
		for _, param := range fn.Arguments {
			unifiedParam := models.FunctionParameter{
				Name:     param.Name,
				DataType: param.DataType,
			}
			unifiedFunction.Arguments = append(unifiedFunction.Arguments, unifiedParam)
		}

		unifiedModel.Functions = append(unifiedModel.Functions, unifiedFunction)
	}

	// Convert triggers
	for _, trigger := range mssqlSchema.Triggers {
		unifiedTrigger := models.Trigger{
			Name:       trigger.Name,
			Schema:     trigger.Schema,
			Table:      trigger.Table,
			Event:      trigger.Event,
			Definition: trigger.Definition,
			Timing:     trigger.Timing,
		}
		unifiedModel.Triggers = append(unifiedModel.Triggers, unifiedTrigger)
	}

	// Convert sequences
	for _, seq := range mssqlSchema.Sequences {
		unifiedSequence := models.Sequence{
			Name:      seq.Name,
			Schema:    seq.Schema,
			DataType:  seq.DataType,
			Start:     seq.Start,
			Increment: seq.Increment,
			MaxValue:  seq.MaxValue,
			MinValue:  seq.MinValue,
			CacheSize: seq.CacheSize,
			Cycle:     seq.Cycle,
		}
		unifiedModel.Sequences = append(unifiedModel.Sequences, unifiedSequence)
	}

	return unifiedModel, warnings, nil
}

// MSSQLExporter implements the Exporter interface for MSSQL
type MSSQLExporter struct{}

// ExportSchema converts a UnifiedModel to a MSSQL schema
func (m *MSSQLExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	mssqlSchema := models.MSSQLSchema{
		SchemaType: "mssql",
	}
	warnings := []string{}

	// Convert tables
	for _, table := range model.Tables {
		mssqlTable := models.MSSQLTable{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertToMSSQLTableType(table.TableType),
		}

		// Convert columns
		for _, col := range table.Columns {
			mssqlCol := models.MSSQLColumn{
				Name:          col.Name,
				IsNullable:    col.IsNullable,
				IsPrimaryKey:  col.IsPrimaryKey,
				IsUnique:      col.IsUnique,
				ColumnDefault: col.DefaultValue,
				DataType:      convertToMSSQLType(col.DataType),
			}
			mssqlTable.Columns = append(mssqlTable.Columns, mssqlCol)
		}

		// Convert constraints
		for _, constraint := range table.Constraints {
			mssqlConstraint := models.MSSQLConstraint{
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
			}
			mssqlTable.Constraints = append(mssqlTable.Constraints, mssqlConstraint)
		}

		mssqlSchema.Tables = append(mssqlSchema.Tables, mssqlTable)
	}

	// Convert functions
	for _, fn := range model.Functions {
		mssqlFunction := models.MSSQLFunction{
			Name:            fn.Name,
			Schema:          fn.Schema,
			ReturnType:      fn.ReturnType,
			IsDeterministic: fn.IsDeterministic,
			Definition:      fn.Definition,
		}

		// Convert function parameters
		for _, param := range fn.Arguments {
			mssqlParam := models.MSSQLFunctionParameter{
				Name:     param.Name,
				DataType: param.DataType,
			}
			mssqlFunction.Arguments = append(mssqlFunction.Arguments, mssqlParam)
		}

		mssqlSchema.Functions = append(mssqlSchema.Functions, mssqlFunction)
	}

	// Convert triggers
	for _, trigger := range model.Triggers {
		mssqlTrigger := models.MSSQLTrigger{
			Name:       trigger.Name,
			Schema:     trigger.Schema,
			Table:      trigger.Table,
			Event:      trigger.Event,
			Definition: trigger.Definition,
			Timing:     trigger.Timing,
		}
		mssqlSchema.Triggers = append(mssqlSchema.Triggers, mssqlTrigger)
	}

	// Convert sequences
	for _, seq := range model.Sequences {
		mssqlSequence := models.MSSQLSequence{
			Name:      seq.Name,
			Schema:    seq.Schema,
			DataType:  seq.DataType,
			Start:     seq.Start,
			Increment: seq.Increment,
			MaxValue:  seq.MaxValue,
			MinValue:  seq.MinValue,
			CacheSize: seq.CacheSize,
			Cycle:     seq.Cycle,
		}
		mssqlSchema.Sequences = append(mssqlSchema.Sequences, mssqlSequence)
	}

	return mssqlSchema, warnings, nil
}

func convertMSSQLTableType(mssqlTableType string) string {
	switch strings.ToLower(mssqlTableType) {
	case "standard":
		return "standard"
	case "view":
		return "view"
	case "temporary":
		return "temporary"
	default:
		return "standard"
	}
}

func convertToMSSQLTableType(unifiedTableType string) string {
	switch strings.ToLower(unifiedTableType) {
	case "standard":
		return "standard"
	case "view":
		return "view"
	case "temporary":
		return "temporary"
	default:
		return "standard"
	}
}

func parseMSSQLType(mssqlType string, warnings *[]string) models.DataType {
	// Split type and parameters
	parts := strings.Split(mssqlType, "(")
	baseType := strings.TrimSpace(parts[0])

	dt := models.DataType{
		Name:         baseType,
		TypeCategory: "basic",
		BaseType:     baseType,
	}

	// Handle type parameters
	if len(parts) > 1 {
		params := strings.TrimRight(parts[1], ")")
		paramParts := strings.Split(params, ",")

		switch strings.ToLower(baseType) {
		case "varchar", "nvarchar", "char", "nchar":
			if len(paramParts) > 0 {
				if length, err := strconv.Atoi(strings.TrimSpace(paramParts[0])); err == nil {
					dt.Length = length
				}
			}
		case "decimal", "numeric":
			if len(paramParts) >= 2 {
				if precision, err := strconv.Atoi(strings.TrimSpace(paramParts[0])); err == nil {
					dt.Precision = precision
				}
				if scale, err := strconv.Atoi(strings.TrimSpace(paramParts[1])); err == nil {
					dt.Scale = scale
				}
			}
		}
	}

	return dt
}

func convertToMSSQLType(dt models.DataType) string {
	var result strings.Builder
	result.WriteString(dt.BaseType)

	// Add type parameters if needed
	switch strings.ToLower(dt.BaseType) {
	case "varchar", "nvarchar", "char", "nchar":
		if dt.Length > 0 {
			result.WriteString(fmt.Sprintf("(%d)", dt.Length))
		}
	case "decimal", "numeric":
		if dt.Precision > 0 || dt.Scale > 0 {
			result.WriteString(fmt.Sprintf("(%d,%d)", dt.Precision, dt.Scale))
		}
	}

	return result.String()
}
