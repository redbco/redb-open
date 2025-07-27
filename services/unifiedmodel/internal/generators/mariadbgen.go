package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

// MariaDBGenerator generates SQL for MariaDB databases
type MariaDBGenerator struct {
	BaseGenerator
}

// NewMariaDBGenerator creates a new MariaDB generator
func NewMariaDBGenerator() *MariaDBGenerator {
	return &MariaDBGenerator{}
}

// GenerateCreateTable generates the SQL for creating a table
func (g *MariaDBGenerator) GenerateCreateTable(table models.MariaDBTable) string {
	var sb strings.Builder

	// Start CREATE TABLE statement
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", table.Schema, table.Name))

	// Add columns
	columnDefs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		columnDefs = append(columnDefs, g.generateColumnDefinition(col))
	}

	// Add primary key if exists
	if pk := table.GetPrimaryKey(); pk != nil {
		columnDefs = append(columnDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pk.Columns, ", ")))
	}

	// Add foreign keys
	for _, constraint := range table.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			columnDefs = append(columnDefs, g.generateForeignKeyDefinition(constraint))
		}
	}

	// Add indexes
	for _, idx := range table.Indexes {
		if idx.Name != "PRIMARY" {
			columnDefs = append(columnDefs, g.generateIndexDefinition(idx))
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
	if table.Collation != "" {
		sb.WriteString(fmt.Sprintf(" COLLATE=%s", table.Collation))
	}
	if table.Comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT='%s'", table.Comment))
	}

	// Add partitioning if exists
	if table.PartitionInfo != nil {
		sb.WriteString(g.generatePartitionDefinition(table.PartitionInfo))
	}

	return sb.String()
}

// generateColumnDefinition generates the SQL for a column definition
func (g *MariaDBGenerator) generateColumnDefinition(col models.MariaDBColumn) string {
	var sb strings.Builder

	// Column name and data type
	sb.WriteString(fmt.Sprintf("  `%s` %s", col.Name, col.DataType.Name))

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
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.DefaultValue))
	}

	// Auto increment
	if col.IsAutoIncrement {
		sb.WriteString(" AUTO_INCREMENT")
	}

	// Generated column
	if col.GenerationExpr != "" {
		if col.IsVirtual {
			sb.WriteString(" GENERATED ALWAYS AS (")
		} else {
			sb.WriteString(" GENERATED ALWAYS AS (")
		}
		sb.WriteString(col.GenerationExpr)
		sb.WriteString(") ")
		if col.IsStored {
			sb.WriteString("STORED")
		} else {
			sb.WriteString("VIRTUAL")
		}
	}

	// Comment
	if col.Comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
	}

	return sb.String()
}

// generateForeignKeyDefinition generates the SQL for a foreign key constraint
func (g *MariaDBGenerator) generateForeignKeyDefinition(constraint models.Constraint) string {
	return fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)",
		constraint.Name,
		strings.Join(constraint.Columns, ", "),
		constraint.ReferencedTable,
		strings.Join(constraint.ReferencedColumns, ", "),
	)
}

// generateIndexDefinition generates the SQL for an index
func (g *MariaDBGenerator) generateIndexDefinition(idx models.Index) string {
	var sb strings.Builder

	if idx.IsUnique {
		sb.WriteString("  UNIQUE")
	}

	// Convert IndexColumn slice to string slice for column names
	columnNames := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		columnNames[i] = col.ColumnName
	}

	sb.WriteString(fmt.Sprintf(" KEY `%s` (%s)", idx.Name, strings.Join(columnNames, ", ")))

	if idx.IndexMethod != "" {
		sb.WriteString(fmt.Sprintf(" USING %s", idx.IndexMethod))
	}

	return sb.String()
}

// generatePartitionDefinition generates the SQL for table partitioning
func (g *MariaDBGenerator) generatePartitionDefinition(info *models.MariaDBPartitionInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("\nPARTITION BY %s (%s)", info.Type, info.Expression))

	if info.SubPartitionBy != "" {
		sb.WriteString(fmt.Sprintf("\nSUBPARTITION BY %s (%s)", info.SubPartitionBy, info.SubPartitionExpr))
	}

	if len(info.Partitions) > 0 {
		sb.WriteString("\n(")
		partitionDefs := make([]string, 0, len(info.Partitions))
		for _, partition := range info.Partitions {
			partitionDefs = append(partitionDefs, fmt.Sprintf("  PARTITION %s", partition))
		}
		sb.WriteString(strings.Join(partitionDefs, ",\n"))
		sb.WriteString("\n)")
	}

	return sb.String()
}

// GenerateCreateEnum generates the SQL for creating an enum type
func (g *MariaDBGenerator) GenerateCreateEnum(enum models.Enum) string {
	values := make([]string, len(enum.Values))
	for i, v := range enum.Values {
		values[i] = fmt.Sprintf("'%s'", v)
	}
	return fmt.Sprintf("CREATE TYPE `%s`.`%s` AS ENUM (%s)",
		enum.Schema,
		enum.Name,
		strings.Join(values, ", "),
	)
}

// GenerateCreateFunction generates the SQL for creating a function
func (g *MariaDBGenerator) GenerateCreateFunction(fn models.Function) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE FUNCTION `%s`.`%s` ", fn.Schema, fn.Name))

	// Add parameters if they exist
	if len(fn.Arguments) > 0 {
		params := make([]string, len(fn.Arguments))
		for i, param := range fn.Arguments {
			params[i] = fmt.Sprintf("%s %s", param.Name, param.DataType)
		}
		sb.WriteString("(")
		sb.WriteString(strings.Join(params, ", "))
		sb.WriteString(")")
	}

	// Add return type
	sb.WriteString(fmt.Sprintf(" RETURNS %s\n", fn.ReturnType))

	// Add deterministic flag
	if fn.IsDeterministic {
		sb.WriteString("DETERMINISTIC\n")
	}

	// Add function body
	sb.WriteString("BEGIN\n")
	sb.WriteString(fn.Definition)
	sb.WriteString("\nEND")

	return sb.String()
}

// GenerateCreateTrigger generates the SQL for creating a trigger
func (g *MariaDBGenerator) GenerateCreateTrigger(trigger models.Trigger) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TRIGGER `%s`.`%s`\n", trigger.Schema, trigger.Name))
	sb.WriteString(fmt.Sprintf("%s %s ON `%s`\n", trigger.Timing, trigger.Event, trigger.Table))
	sb.WriteString("FOR EACH ROW\n")
	sb.WriteString("BEGIN\n")
	sb.WriteString(trigger.Definition)
	sb.WriteString("\nEND")

	return sb.String()
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (g *MariaDBGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return g.BaseGenerator.GenerateCreateTableSQL(table)
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (g *MariaDBGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return g.BaseGenerator.GenerateCreateFunctionSQL(fn)
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (g *MariaDBGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return g.BaseGenerator.GenerateCreateTriggerSQL(trigger)
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (g *MariaDBGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return g.BaseGenerator.GenerateCreateSequenceSQL(seq)
}

// GenerateSchema implements StatementGenerator interface
func (g *MariaDBGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	return g.BaseGenerator.GenerateSchema(model)
}
