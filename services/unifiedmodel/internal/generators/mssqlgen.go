package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type MSSQLGenerator struct{}

func (g *MSSQLGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	var sql strings.Builder

	// Start CREATE TABLE statement
	sql.WriteString(fmt.Sprintf("CREATE TABLE [%s].[%s] (\n", table.Schema, table.Name))

	// Add columns
	columnDefs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("    [%s] %s", col.Name, convertToMSSQLType(col.DataType))

		if !col.IsNullable {
			colDef += " NOT NULL"
		}

		if col.IsAutoIncrement {
			colDef += " IDENTITY(1,1)"
		}

		if col.DefaultValue != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.DefaultValue)
		}

		columnDefs = append(columnDefs, colDef)
	}

	// Add primary key constraint if any column is marked as primary key
	pkColumns := make([]string, 0)
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			pkColumns = append(pkColumns, fmt.Sprintf("[%s]", col.Name))
		}
	}
	if len(pkColumns) > 0 {
		pkConstraint := fmt.Sprintf("    CONSTRAINT [PK_%s] PRIMARY KEY CLUSTERED (%s)",
			table.Name, strings.Join(pkColumns, ", "))
		columnDefs = append(columnDefs, pkConstraint)
	}

	// Join all column definitions
	sql.WriteString(strings.Join(columnDefs, ",\n"))
	sql.WriteString("\n);\n")

	// Add foreign key constraints
	for _, constraint := range table.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			fkSQL := fmt.Sprintf("\nALTER TABLE [%s].[%s] ADD CONSTRAINT [%s] FOREIGN KEY (%s) REFERENCES [%s].[%s] (%s)",
				table.Schema,
				table.Name,
				constraint.Name,
				strings.Join(constraint.Columns, ", "),
				table.Schema,
				constraint.ReferencedTable,
				strings.Join(constraint.ReferencedColumns, ", "),
			)

			if constraint.OnDelete != "" {
				fkSQL += fmt.Sprintf(" ON DELETE %s", constraint.OnDelete)
			}
			if constraint.OnUpdate != "" {
				fkSQL += fmt.Sprintf(" ON UPDATE %s", constraint.OnUpdate)
			}
			fkSQL += ";\n"
			sql.WriteString(fkSQL)
		}
	}

	// Add unique constraints
	for _, col := range table.Columns {
		if col.IsUnique && !col.IsPrimaryKey {
			uniqueSQL := fmt.Sprintf("\nALTER TABLE [%s].[%s] ADD CONSTRAINT [UQ_%s_%s] UNIQUE ([%s]);\n",
				table.Schema,
				table.Name,
				table.Name,
				col.Name,
				col.Name)
			sql.WriteString(uniqueSQL)
		}
	}

	return sql.String(), nil
}

func (g *MSSQLGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	var sql strings.Builder

	// Start CREATE FUNCTION statement
	sql.WriteString(fmt.Sprintf("CREATE FUNCTION [%s].[%s] (\n", fn.Schema, fn.Name))

	// Add parameters
	paramDefs := make([]string, 0, len(fn.Arguments))
	for _, param := range fn.Arguments {
		paramDef := fmt.Sprintf("    @%s %s", param.Name, param.DataType)
		paramDefs = append(paramDefs, paramDef)
	}
	sql.WriteString(strings.Join(paramDefs, ",\n"))
	sql.WriteString("\n) RETURNS " + fn.ReturnType + "\n")

	// Add function definition
	if fn.IsDeterministic {
		sql.WriteString("WITH SCHEMABINDING\n")
	}
	sql.WriteString("AS\nBEGIN\n")
	sql.WriteString(fn.Definition)
	sql.WriteString("\nEND;\n")

	return sql.String(), nil
}

func (g *MSSQLGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	var sql strings.Builder

	// Start CREATE TRIGGER statement
	sql.WriteString(fmt.Sprintf("CREATE TRIGGER [%s].[%s] ON [%s].[%s]\n",
		trigger.Schema, trigger.Name, trigger.Schema, trigger.Table))

	// Add trigger timing and events
	sql.WriteString(fmt.Sprintf("AFTER %s\n", trigger.Event))
	sql.WriteString("AS\nBEGIN\n")
	sql.WriteString(trigger.Definition)
	sql.WriteString("\nEND;\n")

	return sql.String(), nil
}

func (g *MSSQLGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	var sql strings.Builder

	// Start CREATE SEQUENCE statement
	sql.WriteString(fmt.Sprintf("CREATE SEQUENCE [%s].[%s]\n", seq.Schema, seq.Name))

	// Add sequence properties
	if seq.DataType != "" {
		sql.WriteString(fmt.Sprintf("AS %s\n", seq.DataType))
	}
	if seq.Start != 0 {
		sql.WriteString(fmt.Sprintf("START WITH %d\n", seq.Start))
	}
	if seq.Increment != 0 {
		sql.WriteString(fmt.Sprintf("INCREMENT BY %d\n", seq.Increment))
	}
	if seq.MinValue != 0 {
		sql.WriteString(fmt.Sprintf("MINVALUE %d\n", seq.MinValue))
	}
	if seq.MaxValue != 0 {
		sql.WriteString(fmt.Sprintf("MAXVALUE %d\n", seq.MaxValue))
	}
	if seq.CacheSize != 0 {
		sql.WriteString(fmt.Sprintf("CACHE %d\n", seq.CacheSize))
	}
	if seq.Cycle {
		sql.WriteString("CYCLE\n")
	}

	sql.WriteString(";\n")
	return sql.String(), nil
}

func (g *MSSQLGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sql strings.Builder
	warnings := []string{}

	// Add header comment
	sql.WriteString("-- MSSQL Schema Generated from UnifiedModel\n\n")

	// Process tables
	for _, table := range model.Tables {
		if table.Schema != "dbo" && table.Schema != "sys" {
			tableSQL, err := g.GenerateCreateTableSQL(table)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("Error generating table %s.%s: %v", table.Schema, table.Name, err))
				continue
			}
			sql.WriteString(tableSQL)
			sql.WriteString("\n")
		}
	}

	// Process functions
	for _, fn := range model.Functions {
		if fn.Schema != "dbo" && fn.Schema != "sys" {
			fnSQL, err := g.GenerateCreateFunctionSQL(fn)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("Error generating function %s.%s: %v", fn.Schema, fn.Name, err))
				continue
			}
			sql.WriteString(fnSQL)
			sql.WriteString("\n")
		}
	}

	// Process triggers
	for _, trigger := range model.Triggers {
		if trigger.Schema != "dbo" && trigger.Schema != "sys" {
			triggerSQL, err := g.GenerateCreateTriggerSQL(trigger)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("Error generating trigger %s.%s: %v", trigger.Schema, trigger.Name, err))
				continue
			}
			sql.WriteString(triggerSQL)
			sql.WriteString("\n")
		}
	}

	// Process sequences
	for _, seq := range model.Sequences {
		if seq.Schema != "dbo" && seq.Schema != "sys" {
			seqSQL, err := g.GenerateCreateSequenceSQL(seq)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("Error generating sequence %s.%s: %v", seq.Schema, seq.Name, err))
				continue
			}
			sql.WriteString(seqSQL)
			sql.WriteString("\n")
		}
	}

	return sql.String(), warnings, nil
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

func (g *MSSQLGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	// This method is kept for backward compatibility
	return nil, nil
}
