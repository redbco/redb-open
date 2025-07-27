package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type ClickhouseGenerator struct {
	BaseGenerator
}

func (g *ClickhouseGenerator) GenerateCreateSchema(schema models.Schema) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", schema.Name))
	sb.WriteString(";")
	return sb.String()
}

func (g *ClickhouseGenerator) GenerateCreateTable(table models.Table) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n", table.Schema, table.Name))

	// Add columns
	columnDefs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		colDef := g.generateColumnDefinition(col)
		columnDefs = append(columnDefs, colDef)
	}

	sb.WriteString(strings.Join(columnDefs, ",\n"))
	sb.WriteString("\n)")

	// Add engine
	sb.WriteString(" ENGINE = MergeTree")

	// Add order by
	pkColumns := make([]string, 0)
	for _, constraint := range table.Constraints {
		if constraint.Type == "PRIMARY KEY" {
			pkColumns = constraint.Columns
			break
		}
	}
	if len(pkColumns) > 0 {
		sb.WriteString(fmt.Sprintf("\nORDER BY (%s)", strings.Join(pkColumns, ", ")))
	}

	sb.WriteString(";")
	return sb.String()
}

func (g *ClickhouseGenerator) generateColumnDefinition(col models.Column) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.DataType.Name))

	if !col.IsNullable {
		sb.WriteString(" NOT NULL")
	}

	if col.DefaultValue != nil {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.DefaultValue))
	}

	return sb.String()
}

func (g *ClickhouseGenerator) GenerateCreateView(view models.Table) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s.%s AS\n", view.Schema, view.Name))
	// Note: View definition would need to be added here, but it's not available in the current model
	sb.WriteString("SELECT 1;") // Placeholder
	return sb.String()
}

func (g *ClickhouseGenerator) GenerateCreateFunction(function models.Function) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE FUNCTION %s.%s AS ", function.Schema, function.Name))

	// Add parameters
	params := make([]string, 0, len(function.Arguments))
	for _, arg := range function.Arguments {
		params = append(params, fmt.Sprintf("%s %s", arg.Name, arg.DataType))
	}
	sb.WriteString(strings.Join(params, ", "))

	// Add return type
	sb.WriteString(fmt.Sprintf(" -> %s ", function.ReturnType))

	// Add function definition
	sb.WriteString(function.Definition)
	sb.WriteString(";")

	return sb.String()
}

func (g *ClickhouseGenerator) GenerateDropSchema(schema models.Schema) string {
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s;", schema.Name)
}

func (g *ClickhouseGenerator) GenerateDropTable(table models.Table) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", table.Schema, table.Name)
}

func (g *ClickhouseGenerator) GenerateDropView(view models.Table) string {
	return fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", view.Schema, view.Name)
}

func (g *ClickhouseGenerator) GenerateDropFunction(function models.Function) string {
	return fmt.Sprintf("DROP FUNCTION IF EXISTS %s.%s;", function.Schema, function.Name)
}
