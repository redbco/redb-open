package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type Db2Generator struct {
	BaseGenerator
}

func (g *Db2Generator) GenerateDDL(schema *models.UnifiedModel) (string, error) {
	var ddl strings.Builder

	// Generate schema creation statements
	for _, schema := range schema.Schemas {
		ddl.WriteString(fmt.Sprintf("CREATE SCHEMA %s;\n", schema.Name))
	}

	// Generate table creation statements
	for _, table := range schema.Tables {
		ddl.WriteString(g.generateTableDDL(table))
	}

	// Generate sequence creation statements
	for _, sequence := range schema.Sequences {
		ddl.WriteString(g.generateSequenceDDL(sequence))
	}

	// Generate function creation statements
	for _, function := range schema.Functions {
		ddl.WriteString(g.generateFunctionDDL(function))
	}

	// Generate trigger creation statements
	for _, trigger := range schema.Triggers {
		ddl.WriteString(g.generateTriggerDDL(trigger))
	}

	return ddl.String(), nil
}

func (g *Db2Generator) generateTableDDL(table models.Table) string {
	var ddl strings.Builder

	// Start table creation
	ddl.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", table.Schema, table.Name))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.DataType.Name)
		if col.DataType.Length > 0 {
			colDef = fmt.Sprintf("%s(%d)", colDef, col.DataType.Length)
		}
		if col.DataType.Precision > 0 {
			if col.DataType.Scale > 0 {
				colDef = fmt.Sprintf("%s(%d,%d)", colDef, col.DataType.Precision, col.DataType.Scale)
			} else {
				colDef = fmt.Sprintf("%s(%d)", colDef, col.DataType.Precision)
			}
		}
		if !col.IsNullable {
			colDef += " NOT NULL"
		}
		if col.DefaultValue != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.DefaultValue)
		}
		columnDefs = append(columnDefs, colDef)
	}

	// Add primary key constraint if any column is marked as primary key
	var pkColumns []string
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			pkColumns = append(pkColumns, col.Name)
		}
	}
	if len(pkColumns) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pkColumns, ", ")))
	}

	// Add unique constraints
	for _, col := range table.Columns {
		if col.IsUnique && !col.IsPrimaryKey {
			columnDefs = append(columnDefs, fmt.Sprintf("  UNIQUE (%s)", col.Name))
		}
	}

	// Add other constraints
	for _, constraint := range table.Constraints {
		if constraint.Type == "CHECK" {
			columnDefs = append(columnDefs, fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", constraint.Name, constraint.CheckExpression))
		} else if constraint.Type == "FOREIGN KEY" {
			columnDefs = append(columnDefs, fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
				constraint.Name,
				strings.Join(constraint.Columns, ", "),
				constraint.ReferencedTable,
				strings.Join(constraint.ReferencedColumns, ", ")))
		}
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))
	ddl.WriteString("\n);\n\n")

	return ddl.String()
}

func (g *Db2Generator) generateSequenceDDL(sequence models.Sequence) string {
	var ddl strings.Builder

	ddl.WriteString(fmt.Sprintf("CREATE SEQUENCE %s.%s", sequence.Schema, sequence.Name))

	if sequence.DataType != "" {
		ddl.WriteString(fmt.Sprintf(" AS %s", sequence.DataType))
	}

	if sequence.Start != 0 {
		ddl.WriteString(fmt.Sprintf(" START WITH %d", sequence.Start))
	}

	if sequence.Increment != 0 {
		ddl.WriteString(fmt.Sprintf(" INCREMENT BY %d", sequence.Increment))
	}

	if sequence.MaxValue != 0 {
		ddl.WriteString(fmt.Sprintf(" MAXVALUE %d", sequence.MaxValue))
	}

	if sequence.MinValue != 0 {
		ddl.WriteString(fmt.Sprintf(" MINVALUE %d", sequence.MinValue))
	}

	if sequence.CacheSize != 0 {
		ddl.WriteString(fmt.Sprintf(" CACHE %d", sequence.CacheSize))
	}

	if !sequence.Cycle {
		ddl.WriteString(" NO CYCLE")
	} else {
		ddl.WriteString(" CYCLE")
	}

	ddl.WriteString(";\n\n")

	return ddl.String()
}

func (g *Db2Generator) generateFunctionDDL(function models.Function) string {
	var ddl strings.Builder

	ddl.WriteString(fmt.Sprintf("CREATE FUNCTION %s.%s\n", function.Schema, function.Name))

	if function.IsDeterministic {
		ddl.WriteString("DETERMINISTIC\n")
	} else {
		ddl.WriteString("NOT DETERMINISTIC\n")
	}

	ddl.WriteString(fmt.Sprintf("RETURNS %s\n", function.ReturnType))
	ddl.WriteString("BEGIN\n")
	ddl.WriteString(function.Definition)
	ddl.WriteString("\nEND;\n\n")

	return ddl.String()
}

func (g *Db2Generator) generateTriggerDDL(trigger models.Trigger) string {
	var ddl strings.Builder

	ddl.WriteString(fmt.Sprintf("CREATE TRIGGER %s.%s\n", trigger.Schema, trigger.Name))
	ddl.WriteString(fmt.Sprintf("AFTER %s ON %s.%s\n", trigger.Event, trigger.Schema, trigger.Table))
	ddl.WriteString("FOR EACH ROW\n")
	ddl.WriteString("BEGIN\n")
	ddl.WriteString(trigger.Definition)
	ddl.WriteString("\nEND;\n\n")

	return ddl.String()
}
