package generators

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type CassandraGenerator struct{}

func (cg *CassandraGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	cassSchema, ok := schema.(models.CassandraSchema)
	if !ok {
		// Try to convert from map[string]interface{} if direct type assertion fails
		schemaMap, ok := schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid schema type for Cassandra generator")
		}

		// Convert the map to CassandraSchema
		schemaBytes, err := json.Marshal(schemaMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal schema: %v", err)
		}

		if err := json.Unmarshal(schemaBytes, &cassSchema); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to CassandraSchema: %v", err)
		}
	}

	var statements []string

	// Generate keyspace creation statements
	for _, keyspace := range cassSchema.Keyspaces {
		stmt := cg.generateKeyspaceStatement(keyspace)
		statements = append(statements, stmt)
	}

	// Generate user-defined type creation statements
	for _, udt := range cassSchema.Types {
		stmt := cg.generateTypeStatement(udt)
		statements = append(statements, stmt)
	}

	// Generate table creation statements
	for _, table := range cassSchema.Tables {
		stmt := cg.generateTableStatement(table)
		statements = append(statements, stmt)
	}

	// Generate materialized view creation statements
	for _, view := range cassSchema.MaterializedViews {
		stmt := cg.generateViewStatement(view)
		statements = append(statements, stmt)
	}

	return statements, nil
}

func (cg *CassandraGenerator) generateKeyspaceStatement(keyspace models.KeyspaceInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s\n", keyspace.Name))
	sb.WriteString("WITH replication = {\n")
	sb.WriteString(fmt.Sprintf("  'class': '%s',\n", keyspace.ReplicationStrategy))

	// Add replication options
	for key, value := range keyspace.ReplicationOptions {
		sb.WriteString(fmt.Sprintf("  '%s': '%s',\n", key, value))
	}

	// Remove trailing comma from last option
	stmt := sb.String()
	stmt = strings.TrimSuffix(stmt, ",\n")
	stmt += "\n}"

	if !keyspace.DurableWrites {
		stmt += " AND durable_writes = false"
	}

	stmt += ";"
	return stmt
}

func (cg *CassandraGenerator) generateTypeStatement(udt models.CassandraType) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TYPE IF NOT EXISTS %s.%s (\n", udt.Keyspace, udt.Name))

	// Add fields
	var fieldDefs []string
	for _, field := range udt.Fields {
		fieldDef := fmt.Sprintf("  %s %s", field.Name, field.DataType)
		fieldDefs = append(fieldDefs, fieldDef)
	}

	sb.WriteString(strings.Join(fieldDefs, ",\n"))
	sb.WriteString("\n);")

	return sb.String()
}

func (cg *CassandraGenerator) generateTableStatement(table models.CassandraTable) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n", table.Keyspace, table.Name))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.DataType)
		if !col.IsNullable {
			colDef += " NOT NULL"
		}
		if col.IsStatic {
			colDef += " STATIC"
		}
		columnDefs = append(columnDefs, colDef)
	}

	sb.WriteString(strings.Join(columnDefs, ",\n"))

	// Add primary key
	if len(table.PrimaryKey) > 0 {
		sb.WriteString(",\n  PRIMARY KEY (")
		sb.WriteString(strings.Join(table.PrimaryKey, ", "))
		sb.WriteString(")")
	}

	// Add table properties
	if len(table.Properties) > 0 {
		sb.WriteString(",\n  WITH ")
		var props []string
		for key, value := range table.Properties {
			props = append(props, fmt.Sprintf("%s = %s", key, value))
		}
		sb.WriteString(strings.Join(props, " AND "))
	}

	sb.WriteString("\n);")
	return sb.String()
}

func (cg *CassandraGenerator) generateViewStatement(view models.CassandraView) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS\n", view.Keyspace, view.Name))
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(view.Columns, ", "))
	sb.WriteString(fmt.Sprintf("\nFROM %s.%s", view.Keyspace, view.BaseTable))

	if view.WhereClause != "" {
		sb.WriteString("\nWHERE ")
		sb.WriteString(view.WhereClause)
	}

	// Add view properties
	if len(view.Properties) > 0 {
		sb.WriteString("\nWITH ")
		var props []string
		for key, value := range view.Properties {
			props = append(props, fmt.Sprintf("%s = %s", key, value))
		}
		sb.WriteString(strings.Join(props, " AND "))
	}

	sb.WriteString(";")
	return sb.String()
}

func (g *CassandraGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	var sql strings.Builder

	// Start CREATE TABLE statement
	sql.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n", table.Schema, table.Name))

	// Add columns
	columnDefs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		colDef := fmt.Sprintf("    %s %s", col.Name, convertToCassandraType(col.DataType))

		if !col.IsNullable {
			colDef += " NOT NULL"
		}

		if col.IsPrimaryKey {
			colDef += " PRIMARY KEY"
		}

		columnDefs = append(columnDefs, colDef)
	}

	// Join all column definitions
	sql.WriteString(strings.Join(columnDefs, ",\n"))
	sql.WriteString("\n);\n")

	return sql.String(), nil
}

func (g *CassandraGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	var sql strings.Builder

	// Add comment for the function
	sql.WriteString(fmt.Sprintf("-- Function: %s\n", fn.Name))
	sql.WriteString(fmt.Sprintf("-- Arguments: %v\n", fn.Arguments))
	sql.WriteString(fmt.Sprintf("-- Return type: %s\n", fn.ReturnType))

	// Create a function using CREATE FUNCTION
	sql.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s.%s (%s) RETURNS %s\n",
		fn.Schema, fn.Name, formatFunctionArguments(fn.Arguments), fn.ReturnType))
	sql.WriteString("LANGUAGE java\n")
	sql.WriteString("AS $$\n")
	sql.WriteString(fn.Definition)
	sql.WriteString("\n$$;\n")

	return sql.String(), nil
}

func (g *CassandraGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	var sql strings.Builder

	// Add comment for the trigger
	sql.WriteString(fmt.Sprintf("-- Trigger: %s\n", trigger.Name))
	sql.WriteString(fmt.Sprintf("-- Table: %s.%s\n", trigger.Schema, trigger.Table))
	sql.WriteString(fmt.Sprintf("-- Event: %s\n", trigger.Event))

	// Note: Cassandra doesn't support triggers directly
	sql.WriteString("-- Note: Cassandra doesn't support triggers directly\n")
	sql.WriteString("-- Consider using application-level triggers or materialized views\n")

	return sql.String(), nil
}

func (g *CassandraGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	var sql strings.Builder

	// Add comment for the sequence
	sql.WriteString(fmt.Sprintf("-- Sequence: %s\n", seq.Name))

	// Note: Cassandra doesn't support sequences directly
	sql.WriteString("-- Note: Cassandra doesn't support sequences directly\n")
	sql.WriteString("-- Consider using UUID or TimeUUID for unique identifiers\n")

	return sql.String(), nil
}

func (g *CassandraGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	var sql strings.Builder
	warnings := []string{}

	// Add header comment
	sql.WriteString("-- Cassandra Schema Generated from UnifiedModel\n\n")

	// Process tables
	for _, table := range model.Tables {
		if table.Schema != "system" && table.Schema != "system_schema" {
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
		if fn.Schema != "system" && fn.Schema != "system_schema" {
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
		if trigger.Schema != "system" && trigger.Schema != "system_schema" {
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
		if seq.Schema != "system" && seq.Schema != "system_schema" {
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

func convertToCassandraType(dt models.DataType) string {
	var result strings.Builder
	result.WriteString(dt.BaseType)

	// Add type parameters if needed
	switch strings.ToLower(dt.BaseType) {
	case "varchar", "text":
		if dt.Length > 0 {
			result.WriteString(fmt.Sprintf("(%d)", dt.Length))
		}
	case "decimal":
		if dt.Precision > 0 || dt.Scale > 0 {
			result.WriteString(fmt.Sprintf("(%d,%d)", dt.Precision, dt.Scale))
		}
	}

	return result.String()
}

func formatFunctionArguments(args []models.FunctionParameter) string {
	argStrings := make([]string, len(args))
	for i, arg := range args {
		argStrings[i] = fmt.Sprintf("%s %s", arg.Name, arg.DataType)
	}
	return strings.Join(argStrings, ", ")
}
