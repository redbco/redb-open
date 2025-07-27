package adapters

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type OracleIngester struct{}

func (o *OracleIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var oracleSchema models.OracleSchema
	if err := json.Unmarshal(rawSchema, &oracleSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
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

	// Convert schemas (users in Oracle)
	for _, schema := range oracleSchema.Schemas {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name: schema.Name,
		})
	}

	// Convert tables
	for _, table := range oracleSchema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: "standard", // Oracle doesn't have different table types like PostgreSQL
		}

		// Convert columns
		for _, col := range table.Columns {
			unifiedCol := models.Column{
				Name:         col.Name,
				IsNullable:   col.IsNullable,
				IsPrimaryKey: col.IsPrimaryKey,
				IsUnique:     col.IsUnique,
				DefaultValue: col.ColumnDefault,
				DataType:     parseOracleType(col.DataType, col.DataLength, col.DataPrecision, col.DataScale, &warnings),
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

	// Convert sequences
	for _, seq := range oracleSchema.Sequences {
		unifiedSeq := models.Sequence{
			Name:      seq.Name,
			Schema:    seq.Schema,
			DataType:  "NUMBER", // Oracle sequences are always NUMBER
			Start:     seq.StartValue,
			Increment: seq.IncrementBy,
			MaxValue:  seq.MaxValue,
			MinValue:  seq.MinValue,
			CacheSize: seq.CacheSize,
			Cycle:     seq.CycleFlag == "Y",
		}
		unifiedModel.Sequences = append(unifiedModel.Sequences, unifiedSeq)
	}

	// Convert functions
	for _, fn := range oracleSchema.Functions {
		unifiedFn := models.Function{
			Name:            fn.Name,
			Schema:          fn.Schema,
			ReturnType:      fn.ReturnType,
			IsDeterministic: fn.IsDeterministic == "Y",
			Definition:      fn.Definition,
		}
		unifiedModel.Functions = append(unifiedModel.Functions, unifiedFn)
	}

	// Convert triggers
	for _, trg := range oracleSchema.Triggers {
		unifiedTrg := models.Trigger{
			Name:       trg.Name,
			Schema:     trg.Schema,
			Table:      trg.TableName,
			Event:      trg.TriggerEvent,
			Definition: trg.Definition,
			Timing:     trg.TriggerTiming,
		}
		unifiedModel.Triggers = append(unifiedModel.Triggers, unifiedTrg)
	}

	return unifiedModel, warnings, nil
}

type OracleExporter struct{}

func (o *OracleExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	oracleSchema := models.OracleSchema{
		SchemaType: "oracle",
	}
	warnings := []string{}

	// Convert tables
	for _, table := range model.Tables {
		oracleTable := models.OracleTable{
			Name:   table.Name,
			Schema: table.Schema,
		}

		// Convert columns
		for _, col := range table.Columns {
			oracleCol := models.OracleColumn{
				Name:          col.Name,
				IsNullable:    col.IsNullable,
				IsPrimaryKey:  col.IsPrimaryKey,
				IsUnique:      col.IsUnique,
				ColumnDefault: col.DefaultValue,
				DataType:      convertToOracleType(col.DataType),
			}
			oracleTable.Columns = append(oracleTable.Columns, oracleCol)
		}

		// Handle constraints
		for _, constraint := range table.Constraints {
			oracleConstraint := models.OracleConstraint{
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
			oracleTable.Constraints = append(oracleTable.Constraints, oracleConstraint)
		}

		oracleSchema.Tables = append(oracleSchema.Tables, oracleTable)
	}

	// Convert sequences
	for _, seq := range model.Sequences {
		oracleSeq := models.OracleSequence{
			Name:        seq.Name,
			Schema:      seq.Schema,
			StartValue:  seq.Start,
			IncrementBy: seq.Increment,
			MaxValue:    seq.MaxValue,
			MinValue:    seq.MinValue,
			CacheSize:   seq.CacheSize,
			CycleFlag:   map[bool]string{true: "Y", false: "N"}[seq.Cycle],
		}
		oracleSchema.Sequences = append(oracleSchema.Sequences, oracleSeq)
	}

	// Convert functions
	for _, fn := range model.Functions {
		oracleFn := models.OracleFunction{
			Name:            fn.Name,
			Schema:          fn.Schema,
			ReturnType:      fn.ReturnType,
			IsDeterministic: map[bool]string{true: "Y", false: "N"}[fn.IsDeterministic],
			Definition:      fn.Definition,
		}
		oracleSchema.Functions = append(oracleSchema.Functions, oracleFn)
	}

	// Convert triggers
	for _, trg := range model.Triggers {
		oracleTrg := models.OracleTrigger{
			Name:          trg.Name,
			Schema:        trg.Schema,
			TableName:     trg.Table,
			TriggerEvent:  trg.Event,
			Definition:    trg.Definition,
			TriggerTiming: trg.Timing,
		}
		oracleSchema.Triggers = append(oracleSchema.Triggers, oracleTrg)
	}

	return oracleSchema, warnings, nil
}

func parseOracleType(dataType string, length *int, precision *string, scale *string, warnings *[]string) models.DataType {
	dt := models.DataType{
		Name:         strings.ToUpper(dataType),
		TypeCategory: "basic",
		BaseType:     strings.ToUpper(dataType),
	}

	switch strings.ToUpper(dataType) {
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR":
		if length != nil {
			dt.Length = *length
		}
	case "NUMBER":
		if precision != nil {
			p, err := strconv.Atoi(*precision)
			if err == nil {
				dt.Precision = p
			}
		}
		if scale != nil {
			s, err := strconv.Atoi(*scale)
			if err == nil {
				dt.Scale = s
			}
		}
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		// These types don't need additional parameters
	case "CLOB", "NCLOB", "BLOB", "BFILE":
		// These types don't need additional parameters
	default:
		*warnings = append(*warnings, fmt.Sprintf("Unhandled Oracle data type: %s", dataType))
	}

	return dt
}

func convertToOracleType(dt models.DataType) string {
	switch strings.ToUpper(dt.BaseType) {
	case "VARCHAR", "VARCHAR2":
		if dt.Length > 0 {
			return fmt.Sprintf("VARCHAR2(%d)", dt.Length)
		}
		return "VARCHAR2(4000)" // Default length
	case "CHAR":
		if dt.Length > 0 {
			return fmt.Sprintf("CHAR(%d)", dt.Length)
		}
		return "CHAR(1)" // Default length
	case "NUMBER":
		if dt.Precision > 0 {
			if dt.Scale > 0 {
				return fmt.Sprintf("NUMBER(%d,%d)", dt.Precision, dt.Scale)
			}
			return fmt.Sprintf("NUMBER(%d)", dt.Precision)
		}
		return "NUMBER"
	case "DATE":
		return "DATE"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "CLOB":
		return "CLOB"
	case "BLOB":
		return "BLOB"
	default:
		return dt.BaseType
	}
}
