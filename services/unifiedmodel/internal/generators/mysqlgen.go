package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// MySQLGenerator implements SQL generation for MySQL
type MySQLGenerator struct {
	BaseGenerator
}

// NewMySQLGenerator creates a new MySQL generator
func NewMySQLGenerator() *MySQLGenerator {
	return &MySQLGenerator{}
}

// GenerateCreateTable generates MySQL CREATE TABLE statement
func (g *MySQLGenerator) GenerateCreateTable(table models.MySQLTable) string {
	var sb strings.Builder

	// Start CREATE TABLE statement
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", table.Schema, table.Name))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		columnDefs = append(columnDefs, g.generateColumnDefinition(col))
	}

	// Add primary key constraint
	for _, constraint := range table.Constraints {
		if constraint.Type == "PRIMARY KEY" {
			columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)",
				strings.Join(constraint.Columns, ", ")))
		}
	}

	// Add foreign key constraints
	for _, constraint := range table.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			columnDefs = append(columnDefs, fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)",
				constraint.Name,
				strings.Join(constraint.Columns, ", "),
				constraint.ReferencedTable,
				strings.Join(constraint.ReferencedColumns, ", ")))
		}
	}

	sb.WriteString(strings.Join(columnDefs, ",\n"))
	sb.WriteString("\n)")

	// Add table options
	if table.Engine != "" {
		sb.WriteString(fmt.Sprintf(" ENGINE=%s", table.Engine))
	}
	if table.RowFormat != "" {
		sb.WriteString(fmt.Sprintf(" ROW_FORMAT=%s", table.RowFormat))
	}
	if table.CharacterSet != "" {
		sb.WriteString(fmt.Sprintf(" CHARACTER SET %s", table.CharacterSet))
	}
	if table.Collation != "" {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", table.Collation))
	}
	if table.Comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT='%s'", table.Comment))
	}

	// Add partitioning
	if table.PartitionInfo != nil {
		sb.WriteString(g.generatePartitionDefinition(table.PartitionInfo))
	}

	return sb.String()
}

// generateColumnDefinition generates MySQL column definition
func (g *MySQLGenerator) generateColumnDefinition(col models.MySQLColumn) string {
	var sb strings.Builder

	// Column name and data type
	sb.WriteString(fmt.Sprintf("  `%s` %s", col.Name, g.generateDataType(col.DataType)))

	// Character set and collation
	if col.CharacterSet != "" {
		sb.WriteString(fmt.Sprintf(" CHARACTER SET %s", col.CharacterSet))
	}
	if col.Collation != "" {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", col.Collation))
	}

	// Nullability
	if !col.IsNullable {
		sb.WriteString(" NOT NULL")
	}

	// Default value
	if col.DefaultValue != nil {
		if col.DefaultIsFunction {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.DefaultValue))
		} else {
			sb.WriteString(fmt.Sprintf(" DEFAULT '%s'", *col.DefaultValue))
		}
	}

	// Auto increment
	if col.IsAutoIncrement {
		sb.WriteString(" AUTO_INCREMENT")
	}

	// Generated column
	if col.IsGenerated {
		if col.IsStored {
			sb.WriteString(fmt.Sprintf(" STORED GENERATED ALWAYS AS (%s)", col.GenerationExpr))
		} else {
			sb.WriteString(fmt.Sprintf(" VIRTUAL GENERATED ALWAYS AS (%s)", col.GenerationExpr))
		}
	}

	// Column comment
	if col.Comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
	}

	return sb.String()
}

// generateDataType generates MySQL data type definition
func (g *MySQLGenerator) generateDataType(dt models.DataType) string {
	var sb strings.Builder

	switch dt.TypeCategory {
	case "basic":
		sb.WriteString(dt.BaseType)
		if dt.Length > 0 {
			sb.WriteString(fmt.Sprintf("(%d)", dt.Length))
		}
		if dt.Precision > 0 {
			sb.WriteString(fmt.Sprintf("(%d,%d)", dt.Precision, dt.Scale))
		}

	case "enum":
		sb.WriteString("ENUM(")
		var values []string
		for _, v := range dt.EnumValues {
			values = append(values, fmt.Sprintf("'%s'", v))
		}
		sb.WriteString(strings.Join(values, ", "))
		sb.WriteString(")")

	case "array":
		// MySQL doesn't support array types directly
		sb.WriteString("JSON")

	default:
		sb.WriteString(dt.BaseType)
	}

	return sb.String()
}

// generatePartitionDefinition generates MySQL partition definition
func (g *MySQLGenerator) generatePartitionDefinition(pi *models.MySQLPartitionInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(" PARTITION BY %s", pi.Type))

	if pi.Expression != "" {
		sb.WriteString(fmt.Sprintf(" (%s)", pi.Expression))
	}

	if pi.SubPartitionBy != "" {
		sb.WriteString(fmt.Sprintf(" SUBPARTITION BY %s", pi.SubPartitionBy))
		if pi.SubPartitions > 0 {
			sb.WriteString(fmt.Sprintf(" (%d)", pi.SubPartitions))
		}
	}

	if len(pi.Partitions) > 0 {
		sb.WriteString(" (\n")
		var partitionDefs []string
		for _, p := range pi.Partitions {
			partitionDefs = append(partitionDefs, fmt.Sprintf("  PARTITION %s", p))
		}
		sb.WriteString(strings.Join(partitionDefs, ",\n"))
		sb.WriteString("\n)")
	}

	return sb.String()
}

// GenerateCreateIndex generates MySQL CREATE INDEX statement
func (g *MySQLGenerator) GenerateCreateIndex(idx models.Index) string {
	var sb strings.Builder

	// Start CREATE INDEX statement
	if idx.IsUnique {
		sb.WriteString("CREATE UNIQUE INDEX")
	} else {
		sb.WriteString("CREATE INDEX")
	}

	sb.WriteString(fmt.Sprintf(" `%s` ON `%s`.`%s` (", idx.Name, idx.Schema, idx.Table))

	// Add columns
	var columnDefs []string
	for _, col := range idx.Columns {
		columnDef := fmt.Sprintf("`%s`", col.ColumnName)
		if col.Order > 0 {
			columnDef += " ASC"
		} else if col.Order < 0 {
			columnDef += " DESC"
		}
		columnDefs = append(columnDefs, columnDef)
	}

	sb.WriteString(strings.Join(columnDefs, ", "))
	sb.WriteString(")")

	// Add index options
	if idx.IndexMethod != "" {
		sb.WriteString(fmt.Sprintf(" USING %s", idx.IndexMethod))
	}

	if idx.WhereClause != "" {
		sb.WriteString(fmt.Sprintf(" WHERE %s", idx.WhereClause))
	}

	return sb.String()
}

// GenerateCreateEnum generates MySQL ENUM type definition
func (g *MySQLGenerator) GenerateCreateEnum(enum models.Enum) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TYPE `%s`.`%s` AS ENUM (", enum.Schema, enum.Name))

	var values []string
	for _, v := range enum.Values {
		values = append(values, fmt.Sprintf("'%s'", v))
	}

	sb.WriteString(strings.Join(values, ", "))
	sb.WriteString(")")

	return sb.String()
}

// GenerateCreateFunction generates MySQL function definition
func (g *MySQLGenerator) GenerateCreateFunction(fn models.Function) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE FUNCTION `%s`.`%s` %s\n", fn.Schema, fn.Name, fn.Arguments))
	sb.WriteString("RETURNS ")
	sb.WriteString(fn.ReturnType)
	sb.WriteString("\n")
	sb.WriteString(fn.Definition)

	return sb.String()
}

// GenerateCreateTrigger generates MySQL trigger definition
func (g *MySQLGenerator) GenerateCreateTrigger(trigger models.Trigger) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TRIGGER `%s`.`%s`\n", trigger.Schema, trigger.Name))
	sb.WriteString(fmt.Sprintf("%s %s ON `%s`\n", trigger.Timing, trigger.Event, trigger.Table))
	sb.WriteString("FOR EACH ROW\n")
	sb.WriteString(trigger.Definition)

	return sb.String()
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (g *MySQLGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return g.BaseGenerator.GenerateCreateTableSQL(table)
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (g *MySQLGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return g.BaseGenerator.GenerateCreateFunctionSQL(fn)
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (g *MySQLGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return g.BaseGenerator.GenerateCreateTriggerSQL(trigger)
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (g *MySQLGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return g.BaseGenerator.GenerateCreateSequenceSQL(seq)
}

// GenerateSchema implements StatementGenerator interface
func (g *MySQLGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	return g.BaseGenerator.GenerateSchema(model)
}
