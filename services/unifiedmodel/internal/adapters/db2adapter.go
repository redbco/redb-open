package adapters

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type Db2Ingester struct{}

func (d *Db2Ingester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var db2Schema models.Db2Schema
	if err := json.Unmarshal(rawSchema, &db2Schema); err != nil {
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

	// Convert schemas
	for _, schema := range db2Schema.Schemas {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name: schema.Name,
		})
	}

	// Convert tables
	for _, table := range db2Schema.Tables {
		unifiedTable := models.Table{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertDB2TableType(table.TableType),
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
				DataType:        parseDb2Type(col.DataType, col.DataLength, col.DataPrecision, col.DataScale, col.IsArray, &warnings),
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

		// Handle indexes
		for _, idx := range table.Indexes {
			unifiedIdx := models.Index{
				Name:        idx.Name,
				Schema:      idx.Schema,
				Table:       idx.Table,
				IsUnique:    idx.IsUnique,
				IndexMethod: idx.IndexType,
				WhereClause: idx.WhereClause,
			}

			// Convert index columns
			for _, col := range idx.Columns {
				unifiedIdxCol := models.IndexColumn{
					ColumnName: col.ColumnName,
					Order:      col.Order,
				}
				unifiedIdx.Columns = append(unifiedIdx.Columns, unifiedIdxCol)
			}

			unifiedModel.Indexes = append(unifiedModel.Indexes, unifiedIdx)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert sequences
	for _, seq := range db2Schema.Sequences {
		unifiedSeq := models.Sequence{
			Name:      seq.Name,
			Schema:    seq.Schema,
			DataType:  seq.DataType,
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
	for _, fn := range db2Schema.Functions {
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
	for _, trg := range db2Schema.Triggers {
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

type Db2Exporter struct{}

func (d *Db2Exporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	db2Schema := models.Db2Schema{
		SchemaType: "db2",
	}
	warnings := []string{}

	// Convert tables
	for _, table := range model.Tables {
		db2Table := models.Db2Table{
			Name:      table.Name,
			Schema:    table.Schema,
			TableType: convertToDb2TableType(table.TableType),
		}

		// Convert columns
		for _, col := range table.Columns {
			db2Col := models.Db2Column{
				Name:            col.Name,
				IsNullable:      col.IsNullable,
				IsPrimaryKey:    col.IsPrimaryKey,
				IsUnique:        col.IsUnique,
				IsAutoIncrement: col.IsAutoIncrement,
				ColumnDefault:   col.DefaultValue,
				DataType:        convertToDb2Type(col.DataType),
			}
			db2Table.Columns = append(db2Table.Columns, db2Col)
		}

		// Handle constraints
		for _, constraint := range table.Constraints {
			db2Constraint := models.Db2Constraint{
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
			db2Table.Constraints = append(db2Table.Constraints, db2Constraint)
		}

		db2Schema.Tables = append(db2Schema.Tables, db2Table)
	}

	// Convert sequences
	for _, seq := range model.Sequences {
		db2Seq := models.Db2Sequence{
			Name:        seq.Name,
			Schema:      seq.Schema,
			DataType:    seq.DataType,
			StartValue:  seq.Start,
			IncrementBy: seq.Increment,
			MaxValue:    seq.MaxValue,
			MinValue:    seq.MinValue,
			CacheSize:   seq.CacheSize,
			CycleFlag:   map[bool]string{true: "Y", false: "N"}[seq.Cycle],
		}
		db2Schema.Sequences = append(db2Schema.Sequences, db2Seq)
	}

	// Convert functions
	for _, fn := range model.Functions {
		db2Fn := models.Db2Function{
			Name:            fn.Name,
			Schema:          fn.Schema,
			ReturnType:      fn.ReturnType,
			IsDeterministic: map[bool]string{true: "Y", false: "N"}[fn.IsDeterministic],
			Definition:      fn.Definition,
		}
		db2Schema.Functions = append(db2Schema.Functions, db2Fn)
	}

	// Convert triggers
	for _, trg := range model.Triggers {
		db2Trg := models.Db2Trigger{
			Name:          trg.Name,
			Schema:        trg.Schema,
			TableName:     trg.Table,
			TriggerEvent:  trg.Event,
			Definition:    trg.Definition,
			TriggerTiming: trg.Timing,
		}
		db2Schema.Triggers = append(db2Schema.Triggers, db2Trg)
	}

	return db2Schema, warnings, nil
}

func convertDB2TableType(db2TableType string) string {
	switch db2TableType {
	case "db2.standard":
		return "standard"
	case "db2.view":
		return "view"
	case "db2.alias":
		return "alias"
	case "db2.global_temporary":
		return "temporary"
	case "db2.check_pending":
		return "check_pending"
	default:
		return "standard"
	}
}

func convertToDb2TableType(unifiedTableType string) string {
	switch unifiedTableType {
	case "standard":
		return "db2.standard"
	case "view":
		return "db2.view"
	case "alias":
		return "db2.alias"
	case "temporary":
		return "db2.global_temporary"
	case "check_pending":
		return "db2.check_pending"
	default:
		return "db2.standard"
	}
}

func parseDb2Type(dataType string, length *int, precision *string, scale *string, isArray bool, warnings *[]string) models.DataType {
	dt := models.DataType{
		Name:         strings.ToUpper(dataType),
		TypeCategory: "basic",
		BaseType:     strings.ToUpper(dataType),
		IsArray:      isArray,
	}

	switch strings.ToUpper(dataType) {
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER":
		if length != nil {
			dt.Length = *length
		}
	case "DECIMAL", "NUMERIC":
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
	case "DATE", "TIME", "TIMESTAMP":
		// These types don't need additional parameters
	case "CLOB", "BLOB", "DBCLOB":
		// These types don't need additional parameters
	case "XML":
		// XML type doesn't need additional parameters
	default:
		*warnings = append(*warnings, fmt.Sprintf("Unhandled Db2 data type: %s", dataType))
	}

	return dt
}

func convertToDb2Type(dt models.DataType) string {
	switch strings.ToUpper(dt.BaseType) {
	case "VARCHAR", "CHARACTER VARYING":
		if dt.Length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", dt.Length)
		}
		return "VARCHAR(255)" // Default length
	case "CHAR", "CHARACTER":
		if dt.Length > 0 {
			return fmt.Sprintf("CHAR(%d)", dt.Length)
		}
		return "CHAR(1)" // Default length
	case "DECIMAL", "NUMERIC":
		if dt.Precision > 0 {
			if dt.Scale > 0 {
				return fmt.Sprintf("DECIMAL(%d,%d)", dt.Precision, dt.Scale)
			}
			return fmt.Sprintf("DECIMAL(%d)", dt.Precision)
		}
		return "DECIMAL"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "CLOB":
		return "CLOB"
	case "BLOB":
		return "BLOB"
	case "XML":
		return "XML"
	default:
		return dt.BaseType
	}
}
