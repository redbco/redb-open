package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type OracleGenerator struct {
	BaseGenerator
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (g *OracleGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return g.BaseGenerator.GenerateCreateTableSQL(table)
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (g *OracleGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return g.BaseGenerator.GenerateCreateFunctionSQL(fn)
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (g *OracleGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return g.BaseGenerator.GenerateCreateTriggerSQL(trigger)
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (g *OracleGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return g.BaseGenerator.GenerateCreateSequenceSQL(seq)
}

// GenerateSchema implements StatementGenerator interface
func (g *OracleGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	return g.BaseGenerator.GenerateSchema(model)
}

func (g *OracleGenerator) GenerateDDL(model *models.UnifiedModel) (string, []string, error) {
	var ddl strings.Builder
	warnings := []string{}

	// Generate schema creation statements
	for _, schema := range model.Schemas {
		ddl.WriteString(fmt.Sprintf("CREATE USER %s IDENTIFIED BY password;\n", schema.Name))
		ddl.WriteString(fmt.Sprintf("GRANT CREATE SESSION TO %s;\n", schema.Name))
		ddl.WriteString(fmt.Sprintf("ALTER USER %s QUOTA UNLIMITED ON USERS;\n\n", schema.Name))
	}

	// Generate sequence creation statements
	for _, seq := range model.Sequences {
		ddl.WriteString(fmt.Sprintf("CREATE SEQUENCE %s.%s\n", seq.Schema, seq.Name))
		ddl.WriteString("    START WITH " + fmt.Sprintf("%d", seq.Start) + "\n")
		ddl.WriteString("    INCREMENT BY " + fmt.Sprintf("%d", seq.Increment) + "\n")
		if seq.MaxValue > 0 {
			ddl.WriteString("    MAXVALUE " + fmt.Sprintf("%d", seq.MaxValue) + "\n")
		}
		if seq.MinValue > 0 {
			ddl.WriteString("    MINVALUE " + fmt.Sprintf("%d", seq.MinValue) + "\n")
		}
		if seq.CacheSize > 0 {
			ddl.WriteString("    CACHE " + fmt.Sprintf("%d", seq.CacheSize) + "\n")
		}
		if seq.Cycle {
			ddl.WriteString("    CYCLE\n")
		}
		ddl.WriteString(";\n\n")
	}

	// Generate table creation statements
	for _, table := range model.Tables {
		ddl.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", table.Schema, table.Name))

		// Add columns
		columnDefs := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			colDef := fmt.Sprintf("    %s %s", col.Name, col.DataType.Name)
			if col.DataType.Length > 0 {
				colDef += fmt.Sprintf("(%d)", col.DataType.Length)
			}
			if col.DataType.Precision > 0 {
				if col.DataType.Scale > 0 {
					colDef += fmt.Sprintf("(%d,%d)", col.DataType.Precision, col.DataType.Scale)
				} else {
					colDef += fmt.Sprintf("(%d)", col.DataType.Precision)
				}
			}
			if !col.IsNullable {
				colDef += " NOT NULL"
			}
			if col.DefaultValue != nil {
				colDef += " DEFAULT " + *col.DefaultValue
			}
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
			columnDefs = append(columnDefs, fmt.Sprintf("    CONSTRAINT pk_%s PRIMARY KEY (%s)",
				table.Name, strings.Join(pkColumns, ", ")))
		}

		ddl.WriteString(strings.Join(columnDefs, ",\n"))
		ddl.WriteString("\n);\n\n")

		// Add table-level constraints
		for _, constraint := range table.Constraints {
			if constraint.Type == "PRIMARY KEY" {
				continue // Already handled above
			}

			ddl.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT %s %s",
				table.Schema, table.Name, constraint.Name, constraint.Type))

			if len(constraint.Columns) > 0 {
				ddl.WriteString(fmt.Sprintf(" (%s)", strings.Join(constraint.Columns, ", ")))
			}

			if constraint.Type == "FOREIGN KEY" {
				ddl.WriteString(fmt.Sprintf(" REFERENCES %s.%s (%s)",
					table.Schema, constraint.ReferencedTable, strings.Join(constraint.ReferencedColumns, ", ")))
				if constraint.OnDelete != "" {
					ddl.WriteString(" ON DELETE " + constraint.OnDelete)
				}
				if constraint.OnUpdate != "" {
					ddl.WriteString(" ON UPDATE " + constraint.OnUpdate)
				}
			}

			if constraint.Type == "CHECK" && constraint.CheckExpression != "" {
				ddl.WriteString(" (" + constraint.CheckExpression + ")")
			}

			if constraint.Deferrable {
				ddl.WriteString(" DEFERRABLE")
				if constraint.InitiallyDeferred != "" {
					ddl.WriteString(" INITIALLY " + constraint.InitiallyDeferred)
				}
			}

			ddl.WriteString(";\n")
		}
		ddl.WriteString("\n")
	}

	// Generate function creation statements
	for _, fn := range model.Functions {
		ddl.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s.%s\n", fn.Schema, fn.Name))
		if fn.IsDeterministic {
			ddl.WriteString("    DETERMINISTIC\n")
		}
		ddl.WriteString("RETURNS " + fn.ReturnType + "\n")
		ddl.WriteString("AS\n")
		ddl.WriteString(fn.Definition + "\n")
		ddl.WriteString(";\n\n")
	}

	// Generate trigger creation statements
	for _, trg := range model.Triggers {
		ddl.WriteString(fmt.Sprintf("CREATE OR REPLACE TRIGGER %s.%s\n", trg.Schema, trg.Name))
		ddl.WriteString(fmt.Sprintf("    %s ON %s.%s\n", trg.Timing, trg.Schema, trg.Table))
		ddl.WriteString("    FOR EACH ROW\n")
		ddl.WriteString("BEGIN\n")
		ddl.WriteString(trg.Definition + "\n")
		ddl.WriteString("END;\n\n")
	}

	return ddl.String(), warnings, nil
}
