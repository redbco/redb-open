package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type CockroachGenerator struct{}

func (g *CockroachGenerator) GenerateCreateSchema(schema models.Schema) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema.Name))
	if schema.CharacterSet != "" {
		sb.WriteString(fmt.Sprintf(" CHARACTER SET %s", schema.CharacterSet))
	}
	if schema.Collation != "" {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", schema.Collation))
	}
	sb.WriteString(";")
	return sb.String()
}

func (g *CockroachGenerator) GenerateCreateTable(table models.Table) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n", table.Schema, table.Name))

	// Add columns
	columnDefs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		colDef := g.generateColumnDefinition(col)
		columnDefs = append(columnDefs, colDef)
	}

	// Add primary key constraint if any column is marked as primary key
	pkColumns := make([]string, 0)
	for _, col := range table.Columns {
		if col.IsPrimaryKey {
			pkColumns = append(pkColumns, col.Name)
		}
	}
	if len(pkColumns) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkColumns, ", ")))
	}

	// Add other constraints
	for _, constraint := range table.Constraints {
		if constraint.Type != "PRIMARY KEY" { // Skip primary key as it's handled above
			columnDefs = append(columnDefs, g.generateConstraintDefinition(constraint))
		}
	}

	sb.WriteString(strings.Join(columnDefs, ",\n"))
	sb.WriteString("\n);")

	// Add indexes
	for _, index := range table.Indexes {
		sb.WriteString("\n" + g.generateCreateIndex(index))
	}

	return sb.String()
}

func (g *CockroachGenerator) generateColumnDefinition(col models.Column) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.DataType.Name))

	if !col.IsNullable {
		sb.WriteString(" NOT NULL")
	}

	if col.DefaultValue != nil {
		if col.DefaultIsFunction {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.DefaultValue))
		} else {
			sb.WriteString(fmt.Sprintf(" DEFAULT '%s'", *col.DefaultValue))
		}
	}

	if col.Collation != "" {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", col.Collation))
	}

	return sb.String()
}

func (g *CockroachGenerator) generateConstraintDefinition(constraint models.Constraint) string {
	var sb strings.Builder

	switch constraint.Type {
	case "UNIQUE":
		sb.WriteString(fmt.Sprintf("UNIQUE (%s)", strings.Join(constraint.Columns, ", ")))
	case "CHECK":
		sb.WriteString(fmt.Sprintf("CHECK (%s)", constraint.CheckExpression))
	case "FOREIGN KEY":
		sb.WriteString(fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)",
			strings.Join(constraint.Columns, ", "),
			constraint.ReferencedTable,
			strings.Join(constraint.ReferencedColumns, ", ")))
		if constraint.OnDelete != "" {
			sb.WriteString(fmt.Sprintf(" ON DELETE %s", constraint.OnDelete))
		}
		if constraint.OnUpdate != "" {
			sb.WriteString(fmt.Sprintf(" ON UPDATE %s", constraint.OnUpdate))
		}
	}

	if constraint.Name != "" {
		sb.WriteString(fmt.Sprintf(" CONSTRAINT %s", constraint.Name))
	}

	return sb.String()
}

func (g *CockroachGenerator) generateCreateIndex(index models.Index) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if index.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	if index.Name != "" {
		sb.WriteString(index.Name)
	}
	sb.WriteString(fmt.Sprintf(" ON %s.%s (", index.Schema, index.Table))

	// Add index columns
	colDefs := make([]string, 0, len(index.Columns))
	for _, col := range index.Columns {
		colDef := col.ColumnName
		if col.Order > 0 {
			colDef += " ASC"
		} else if col.Order < 0 {
			colDef += " DESC"
		}
		if col.NullPosition > 0 {
			colDef += " NULLS FIRST"
		} else if col.NullPosition < 0 {
			colDef += " NULLS LAST"
		}
		colDefs = append(colDefs, colDef)
	}
	sb.WriteString(strings.Join(colDefs, ", "))
	sb.WriteString(")")

	// Add include columns if any
	if len(index.IncludeColumns) > 0 {
		sb.WriteString(fmt.Sprintf(" INCLUDE (%s)", strings.Join(index.IncludeColumns, ", ")))
	}

	// Add where clause if any
	if index.WhereClause != "" {
		sb.WriteString(fmt.Sprintf(" WHERE %s", index.WhereClause))
	}

	sb.WriteString(";")
	return sb.String()
}

func (g *CockroachGenerator) GenerateCreateEnum(enum models.Enum) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TYPE %s.%s AS ENUM (", enum.Schema, enum.Name))

	values := make([]string, 0, len(enum.Values))
	for _, value := range enum.Values {
		values = append(values, fmt.Sprintf("'%s'", value))
	}
	sb.WriteString(strings.Join(values, ", "))
	sb.WriteString(");")

	return sb.String()
}

func (g *CockroachGenerator) GenerateCreateFunction(function models.Function) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s.%s(", function.Schema, function.Name))

	// Add parameters
	params := make([]string, 0, len(function.Arguments))
	for _, arg := range function.Arguments {
		params = append(params, fmt.Sprintf("%s %s", arg.Name, arg.DataType))
	}
	sb.WriteString(strings.Join(params, ", "))
	sb.WriteString(")")

	// Add return type
	sb.WriteString(fmt.Sprintf(" RETURNS %s", function.ReturnType))

	// Add function definition
	sb.WriteString(" AS ")
	sb.WriteString(function.Definition)
	sb.WriteString(";")

	return sb.String()
}

func (g *CockroachGenerator) GenerateCreateTrigger(trigger models.Trigger) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TRIGGER %s\n", trigger.Name))
	sb.WriteString(fmt.Sprintf("  %s %s\n", trigger.Timing, trigger.Event))
	sb.WriteString(fmt.Sprintf("  ON %s.%s\n", trigger.Schema, trigger.Table))
	sb.WriteString("  FOR EACH ROW\n")
	sb.WriteString(trigger.Definition)
	sb.WriteString(";")

	return sb.String()
}

func (g *CockroachGenerator) GenerateCreateSequence(sequence models.Sequence) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s.%s", sequence.Schema, sequence.Name))

	if sequence.DataType != "" {
		sb.WriteString(fmt.Sprintf(" AS %s", sequence.DataType))
	}

	if sequence.Start != 0 {
		sb.WriteString(fmt.Sprintf(" START WITH %d", sequence.Start))
	}

	if sequence.Increment != 0 {
		sb.WriteString(fmt.Sprintf(" INCREMENT BY %d", sequence.Increment))
	}

	if sequence.MinValue != 0 {
		sb.WriteString(fmt.Sprintf(" MINVALUE %d", sequence.MinValue))
	}

	if sequence.MaxValue != 0 {
		sb.WriteString(fmt.Sprintf(" MAXVALUE %d", sequence.MaxValue))
	}

	if sequence.CacheSize != 0 {
		sb.WriteString(fmt.Sprintf(" CACHE %d", sequence.CacheSize))
	}

	if sequence.Cycle {
		sb.WriteString(" CYCLE")
	} else {
		sb.WriteString(" NO CYCLE")
	}

	sb.WriteString(";")
	return sb.String()
}

func (g *CockroachGenerator) GenerateCreateExtension(extension models.Extension) string {
	return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s SCHEMA %s;", extension.Name, extension.Schema)
}

func (g *CockroachGenerator) GenerateDropSchema(schema models.Schema) string {
	return fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", schema.Name)
}

func (g *CockroachGenerator) GenerateDropTable(table models.Table) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", table.Schema, table.Name)
}

func (g *CockroachGenerator) GenerateDropEnum(enum models.Enum) string {
	return fmt.Sprintf("DROP TYPE IF EXISTS %s.%s CASCADE;", enum.Schema, enum.Name)
}

func (g *CockroachGenerator) GenerateDropFunction(function models.Function) string {
	return fmt.Sprintf("DROP FUNCTION IF EXISTS %s.%s CASCADE;", function.Schema, function.Name)
}

func (g *CockroachGenerator) GenerateDropTrigger(trigger models.Trigger) string {
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s.%s CASCADE;", trigger.Name, trigger.Schema, trigger.Table)
}

func (g *CockroachGenerator) GenerateDropSequence(sequence models.Sequence) string {
	return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s.%s CASCADE;", sequence.Schema, sequence.Name)
}

func (g *CockroachGenerator) GenerateDropExtension(extension models.Extension) string {
	return fmt.Sprintf("DROP EXTENSION IF EXISTS %s CASCADE;", extension.Name)
}

func (g *CockroachGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return g.GenerateCreateTable(table), nil
}

func (g *CockroachGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return g.GenerateCreateFunction(fn), nil
}

func (g *CockroachGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return g.GenerateCreateTrigger(trigger), nil
}

func (g *CockroachGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return g.GenerateCreateSequence(seq), nil
}

func (g *CockroachGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sql strings.Builder
	warnings := []string{}

	// Add header comment
	sql.WriteString("-- CockroachDB Schema Generated from UnifiedModel\n\n")

	// Process schemas
	for _, schema := range model.Schemas {
		sql.WriteString(g.GenerateCreateSchema(schema))
		sql.WriteString("\n\n")
	}

	// Process tables
	for _, table := range model.Tables {
		tableSQL := g.GenerateCreateTable(table)
		sql.WriteString(tableSQL)
		sql.WriteString("\n\n")
	}

	// Process enums
	for _, enum := range model.Enums {
		sql.WriteString(g.GenerateCreateEnum(enum))
		sql.WriteString("\n\n")
	}

	// Process functions
	for _, fn := range model.Functions {
		fnSQL := g.GenerateCreateFunction(fn)
		sql.WriteString(fnSQL)
		sql.WriteString("\n\n")
	}

	// Process triggers
	for _, trigger := range model.Triggers {
		triggerSQL := g.GenerateCreateTrigger(trigger)
		sql.WriteString(triggerSQL)
		sql.WriteString("\n\n")
	}

	// Process sequences
	for _, seq := range model.Sequences {
		seqSQL := g.GenerateCreateSequence(seq)
		sql.WriteString(seqSQL)
		sql.WriteString("\n\n")
	}

	// Process extensions
	for _, ext := range model.Extensions {
		sql.WriteString(g.GenerateCreateExtension(ext))
		sql.WriteString("\n\n")
	}

	return sql.String(), warnings, nil
}

func (g *CockroachGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	// This method is kept for backward compatibility
	return nil, nil
}
