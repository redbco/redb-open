package generators

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type PostgresGenerator struct {
	BaseGenerator
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return pg.BaseGenerator.GenerateCreateTableSQL(table)
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return pg.BaseGenerator.GenerateCreateFunctionSQL(fn)
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return pg.BaseGenerator.GenerateCreateTriggerSQL(trigger)
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return pg.BaseGenerator.GenerateCreateSequenceSQL(seq)
}

// GenerateSchema implements StatementGenerator interface
func (pg *PostgresGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	return pg.BaseGenerator.GenerateSchema(model)
}

func (pg *PostgresGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	pgSchema, ok := schema.(models.PostgresSchema)
	if !ok {
		// Try to convert from map[string]interface{} if direct type assertion fails
		schemaMap, ok := schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid schema type for Postgres generator")
		}

		// Convert the map to PostgresSchema
		schemaBytes, err := json.Marshal(schemaMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal schema: %v", err)
		}

		if err := json.Unmarshal(schemaBytes, &pgSchema); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to PostgresSchema: %v", err)
		}
	}

	var statements []string
	for _, table := range pgSchema.Tables {
		stmt := pg.generateTableStatement(table)
		statements = append(statements, stmt)
	}

	return statements, nil
}

func (pg *PostgresGenerator) generateTableStatement(table models.PGTable) string {
	var sb strings.Builder

	// Start CREATE TABLE statement
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (",
		table.Schema, table.Name))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("%s %s", col.Name, col.Type)

		if !col.IsNullable {
			colDef += " NOT NULL"
		}

		if col.IsPrimaryKey {
			colDef += " PRIMARY KEY"
		}

		if col.IsUnique {
			colDef += " UNIQUE"
		}

		if col.ColumnDefault != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.ColumnDefault)
		}

		columnDefs = append(columnDefs, colDef)
	}

	sb.WriteString(strings.Join(columnDefs, ", "))
	sb.WriteString(");")

	return sb.String()
}
