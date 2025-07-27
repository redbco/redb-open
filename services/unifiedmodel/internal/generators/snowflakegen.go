package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type SnowflakeGenerator struct {
	BaseGenerator
}

func (s *SnowflakeGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sb strings.Builder
	warnings := []string{}

	// Generate warehouse creation
	sb.WriteString("-- Create warehouse\n")
	sb.WriteString("CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH\n")
	sb.WriteString("  WITH WAREHOUSE_SIZE = 'X-Small'\n")
	sb.WriteString("  AUTO_SUSPEND = 60\n")
	sb.WriteString("  AUTO_RESUME = TRUE;\n\n")

	// Generate database creation
	sb.WriteString("-- Create database\n")
	sb.WriteString("CREATE DATABASE IF NOT EXISTS MY_DATABASE;\n\n")

	// Generate schema creation
	for _, schema := range model.Schemas {
		sb.WriteString(fmt.Sprintf("-- Create schema %s\n", schema.Name))
		sb.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n\n", schema.Name))
	}

	// Generate table creation
	for _, table := range model.Tables {
		if table.TableType == "view" {
			// Handle views separately
			sb.WriteString(fmt.Sprintf("-- Create view %s\n", table.Name))
			sb.WriteString(fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s AS\n", table.Schema, table.Name))

			// Try to extract view definition from columns
			var definition string
			for _, col := range table.Columns {
				if col.Name == "definition" {
					definition = col.Comment
					break
				}
			}

			if definition != "" {
				sb.WriteString(definition)
			} else {
				sb.WriteString("-- View definition not available\n")
				sb.WriteString("SELECT 1;\n")
			}
			sb.WriteString(";\n\n")
			continue
		}

		sb.WriteString(fmt.Sprintf("-- Create table %s\n", table.Name))
		sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n", table.Schema, table.Name))

		// Add columns
		for i, col := range table.Columns {
			colDef := fmt.Sprintf("  %s %s", col.Name, col.DataType.Name)

			// Add type modifiers if needed
			if col.DataType.Length > 0 {
				colDef += fmt.Sprintf("(%d)", col.DataType.Length)
			} else if col.DataType.Precision > 0 {
				if col.DataType.Scale > 0 {
					colDef += fmt.Sprintf("(%d,%d)", col.DataType.Precision, col.DataType.Scale)
				} else {
					colDef += fmt.Sprintf("(%d)", col.DataType.Precision)
				}
			}

			// Add constraints
			if !col.IsNullable {
				colDef += " NOT NULL"
			}
			if col.IsPrimaryKey {
				colDef += " PRIMARY KEY"
			}
			if col.IsUnique {
				colDef += " UNIQUE"
			}
			if col.IsAutoIncrement {
				colDef += " AUTOINCREMENT"
			}

			// Add default value if specified
			if col.DefaultValue != nil {
				colDef += fmt.Sprintf(" DEFAULT %s", *col.DefaultValue)
			} else if col.DefaultValueFunction != "" {
				colDef += fmt.Sprintf(" DEFAULT %s", col.DefaultValueFunction)
			}

			// Add comment if specified
			if col.Comment != "" {
				colDef += fmt.Sprintf(" COMMENT '%s'", col.Comment)
			}

			// Add comma if not the last column
			if i < len(table.Columns)-1 {
				colDef += ","
			}

			sb.WriteString(colDef + "\n")
		}

		sb.WriteString(");\n\n")

		// Add table comment if specified
		if table.Comment != "" {
			sb.WriteString(fmt.Sprintf("COMMENT ON TABLE %s.%s IS '%s';\n\n",
				table.Schema, table.Name, table.Comment))
		}

		// Add constraints
		for _, constraint := range table.Constraints {
			if constraint.Type == "PRIMARY KEY" || constraint.Type == "UNIQUE" {
				// These are already handled in column definitions
				continue
			}

			sb.WriteString(fmt.Sprintf("-- Add %s constraint to %s\n", constraint.Type, table.Name))

			switch constraint.Type {
			case "FOREIGN KEY":
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
					table.Schema, table.Name, constraint.Name,
					strings.Join(constraint.Columns, ", "),
					constraint.ReferencedTable,
					strings.Join(constraint.ReferencedColumns, ", ")))

				if constraint.OnDelete != "" {
					sb.WriteString(fmt.Sprintf(" ON DELETE %s", constraint.OnDelete))
				}
				if constraint.OnUpdate != "" {
					sb.WriteString(fmt.Sprintf(" ON UPDATE %s", constraint.OnUpdate))
				}
				sb.WriteString(";\n\n")
			case "CHECK":
				sb.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s CHECK (%s);\n\n",
					table.Schema, table.Name, constraint.Name, constraint.CheckExpression))
			}
		}
	}

	// Generate sequence creation
	for _, seq := range model.Sequences {
		sb.WriteString(fmt.Sprintf("-- Create sequence %s\n", seq.Name))
		sb.WriteString(fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s.%s\n", seq.Schema, seq.Name))

		if seq.Start != 0 {
			sb.WriteString(fmt.Sprintf("  START WITH %d\n", seq.Start))
		}
		if seq.Increment != 0 {
			sb.WriteString(fmt.Sprintf("  INCREMENT BY %d\n", seq.Increment))
		}
		if seq.MinValue != 0 {
			sb.WriteString(fmt.Sprintf("  MINVALUE %d\n", seq.MinValue))
		}
		if seq.MaxValue != 0 {
			sb.WriteString(fmt.Sprintf("  MAXVALUE %d\n", seq.MaxValue))
		}
		if seq.CacheSize != 0 {
			sb.WriteString(fmt.Sprintf("  CACHE %d\n", seq.CacheSize))
		}
		if seq.Cycle {
			sb.WriteString("  CYCLE\n")
		}

		sb.WriteString(";\n\n")
	}

	// Generate function creation
	for _, fn := range model.Functions {
		sb.WriteString(fmt.Sprintf("-- Create function %s\n", fn.Name))
		sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s.%s(", fn.Schema, fn.Name))

		// Add arguments
		for i, arg := range fn.Arguments {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s %s", arg.Name, arg.DataType))
		}

		sb.WriteString(fmt.Sprintf(") RETURNS %s\n", fn.ReturnType))

		if fn.IsDeterministic {
			sb.WriteString("  DETERMINISTIC\n")
		}

		sb.WriteString("  AS\n")
		sb.WriteString(fn.Definition)
		sb.WriteString(";\n\n")
	}

	return sb.String(), warnings, nil
}
