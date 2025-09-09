package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// MySQLGenerator implements SQL generation for MySQL
type MySQLGenerator struct {
	BaseGenerator
}

// NewMySQLGenerator creates a new MySQL generator
func NewMySQLGenerator() *MySQLGenerator {
	return &MySQLGenerator{}
}

// Override BaseGenerator methods to provide MySQL-specific implementations

// Structural organization
func (g *MySQLGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	if database.Name == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE DATABASE `%s`", database.Name))

	// Add character set (from options if available)
	if charset, ok := database.Options["character_set"].(string); ok && charset != "" {
		sb.WriteString(fmt.Sprintf(" CHARACTER SET %s", charset))
	} else {
		sb.WriteString(" CHARACTER SET utf8mb4") // Default to utf8mb4
	}

	// Add collation (from options if available)
	if collation, ok := database.Options["collation"].(string); ok && collation != "" {
		sb.WriteString(fmt.Sprintf(" COLLATE %s", collation))
	} else {
		sb.WriteString(" COLLATE utf8mb4_unicode_ci") // Default collation
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Primary Data Containers
func (g *MySQLGenerator) GenerateCreateTableSQL(table unifiedmodel.Table) (string, error) {
	if table.Name == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (", table.Name))

	// Add columns
	var columnDefs []string
	for _, col := range table.Columns {
		colDef, err := g.generateColumnDefinition(col)
		if err != nil {
			return "", fmt.Errorf("failed to generate column definition for %s: %w", col.Name, err)
		}
		columnDefs = append(columnDefs, colDef)
	}

	if len(columnDefs) == 0 {
		return "", fmt.Errorf("table must have at least one column")
	}

	sb.WriteString(strings.Join(columnDefs, ", "))

	// Add table-level constraints
	for _, constraint := range table.Constraints {
		constraintDef, err := g.generateConstraintDefinition(constraint)
		if err != nil {
			return "", fmt.Errorf("failed to generate constraint definition for %s: %w", constraint.Name, err)
		}
		if constraintDef != "" {
			sb.WriteString(", ")
			sb.WriteString(constraintDef)
		}
	}

	sb.WriteString(")")

	// Add table options
	sb.WriteString(" ENGINE=InnoDB") // Default engine

	// Add character set and collation
	sb.WriteString(" CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

	// Add table comment if present
	if table.Comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT='%s'", strings.ReplaceAll(table.Comment, "'", "''")))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Virtual Data Containers
func (g *MySQLGenerator) GenerateCreateViewSQL(view unifiedmodel.View) (string, error) {
	if view.Name == "" {
		return "", fmt.Errorf("view name cannot be empty")
	}
	if view.Definition == "" {
		return "", fmt.Errorf("view definition cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE VIEW `%s` AS\n%s;", view.Name, view.Definition))

	return sb.String(), nil
}

// Integrity, performance and identity objects
func (g *MySQLGenerator) GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error) {
	if index.Name == "" {
		return "", fmt.Errorf("index name cannot be empty")
	}

	var sb strings.Builder

	// Add UNIQUE if specified
	if index.Unique {
		sb.WriteString("CREATE UNIQUE INDEX ")
	} else {
		sb.WriteString("CREATE INDEX ")
	}

	sb.WriteString(fmt.Sprintf("`%s` ON ", index.Name))

	// Determine table name - this is a limitation of the current design
	// In practice, the table name should be provided in the index context
	tableName := "unknown_table" // This should be resolved by the caller
	if len(index.Fields) > 0 {
		// Try to extract table name from field references if available
		// This is a simplified approach
	}

	sb.WriteString(fmt.Sprintf("`%s` ", tableName))

	// Add index method if specified
	if index.Type != "" {
		sb.WriteString(fmt.Sprintf("USING %s ", strings.ToUpper(string(index.Type))))
	}

	// Add columns or expression
	if index.Expression != "" {
		sb.WriteString(fmt.Sprintf("(%s)", index.Expression))
	} else if len(index.Columns) > 0 {
		var cols []string
		for _, col := range index.Columns {
			cols = append(cols, fmt.Sprintf("`%s`", col))
		}
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(cols, ", ")))
	} else if len(index.Fields) > 0 {
		var fields []string
		for _, field := range index.Fields {
			fields = append(fields, fmt.Sprintf("`%s`", field))
		}
		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(fields, ", ")))
	} else {
		return "", fmt.Errorf("index must have columns, fields, or expression")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *MySQLGenerator) GenerateCreateSequenceSQL(seq unifiedmodel.Sequence) (string, error) {
	// MySQL doesn't have native sequences, but we can simulate with AUTO_INCREMENT tables
	if seq.Name == "" {
		return "", fmt.Errorf("sequence name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s_seq` (", seq.Name))
	sb.WriteString("id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY")
	sb.WriteString(")")

	// Set AUTO_INCREMENT starting value
	if seq.Start != 0 {
		sb.WriteString(fmt.Sprintf(" AUTO_INCREMENT=%d", seq.Start))
	}

	sb.WriteString(";")

	// Add a comment explaining this is a sequence simulation
	sb.WriteString(fmt.Sprintf("\n-- Sequence '%s' simulated using AUTO_INCREMENT table", seq.Name))

	return sb.String(), nil
}

// Executable code objects
func (g *MySQLGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	if fn.Name == "" {
		return "", fmt.Errorf("function name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE FUNCTION `%s`(", fn.Name))

	// Add arguments
	var argDefs []string
	for _, arg := range fn.Arguments {
		argDef := fmt.Sprintf("%s %s", arg.Name, g.mapDataType(arg.Type))
		argDefs = append(argDefs, argDef)
	}
	sb.WriteString(strings.Join(argDefs, ", "))
	sb.WriteString(")")

	// Add return type
	if fn.Returns != "" {
		sb.WriteString(fmt.Sprintf(" RETURNS %s", g.mapDataType(fn.Returns)))
	}

	// Add characteristics
	sb.WriteString(" DETERMINISTIC")

	// Add function body
	sb.WriteString("\nBEGIN\n")
	sb.WriteString(fn.Definition)
	sb.WriteString("\nEND;")

	return sb.String(), nil
}

func (g *MySQLGenerator) GenerateCreateProcedureSQL(procedure unifiedmodel.Procedure) (string, error) {
	if procedure.Name == "" {
		return "", fmt.Errorf("procedure name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE PROCEDURE `%s`(", procedure.Name))

	// Add parameters (using Arguments field like Function)
	var paramDefs []string
	for _, arg := range procedure.Arguments {
		paramMode := "IN" // Default mode for MySQL procedures
		paramDef := fmt.Sprintf("%s %s %s", paramMode, arg.Name, g.mapDataType(arg.Type))
		paramDefs = append(paramDefs, paramDef)
	}
	sb.WriteString(strings.Join(paramDefs, ", "))
	sb.WriteString(")")

	// Add procedure body
	sb.WriteString("\nBEGIN\n")
	sb.WriteString(procedure.Definition)
	sb.WriteString("\nEND;")

	return sb.String(), nil
}

func (g *MySQLGenerator) GenerateCreateTriggerSQL(trigger unifiedmodel.Trigger) (string, error) {
	if trigger.Name == "" {
		return "", fmt.Errorf("trigger name cannot be empty")
	}
	if trigger.Table == "" {
		return "", fmt.Errorf("trigger table cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TRIGGER `%s`", trigger.Name))

	// Add timing
	if trigger.Timing != "" {
		sb.WriteString(fmt.Sprintf(" %s", strings.ToUpper(trigger.Timing)))
	} else {
		sb.WriteString(" BEFORE") // Default timing
	}

	// Add events (MySQL supports only one event per trigger)
	if len(trigger.Events) > 0 {
		sb.WriteString(fmt.Sprintf(" %s", strings.ToUpper(trigger.Events[0])))
	} else {
		sb.WriteString(" INSERT") // Default event
	}

	// Add table
	sb.WriteString(fmt.Sprintf(" ON `%s`", trigger.Table))

	// Add FOR EACH ROW
	sb.WriteString(" FOR EACH ROW")

	// Add trigger body (MySQL triggers reference procedures, so we need to call the procedure)
	sb.WriteString("\nBEGIN\n")
	if trigger.Procedure != "" {
		sb.WriteString(fmt.Sprintf("CALL %s();", trigger.Procedure))
	} else {
		sb.WriteString("-- No procedure specified for trigger")
	}
	sb.WriteString("\nEND;")

	return sb.String(), nil
}

// Security and access control
func (g *MySQLGenerator) GenerateCreateUserSQL(user unifiedmodel.DBUser) (string, error) {
	if user.Name == "" {
		return "", fmt.Errorf("user name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE USER '%s'@'%%'", user.Name))

	// Add password if specified in options
	if password, ok := user.Options["password"].(string); ok && password != "" {
		sb.WriteString(fmt.Sprintf(" IDENTIFIED BY '%s'", password))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *MySQLGenerator) GenerateCreateGrantSQL(grant unifiedmodel.Grant) (string, error) {
	if grant.Principal == "" {
		return "", fmt.Errorf("grant principal cannot be empty")
	}
	if grant.Privilege == "" {
		return "", fmt.Errorf("grant privilege cannot be empty")
	}
	if grant.Object == "" {
		return "", fmt.Errorf("grant object cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("GRANT %s", grant.Privilege))

	// Add columns if specified
	if len(grant.Columns) > 0 {
		var cols []string
		for _, col := range grant.Columns {
			cols = append(cols, fmt.Sprintf("`%s`", col))
		}
		sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(cols, ", ")))
	}

	sb.WriteString(fmt.Sprintf(" ON %s TO '%s'@'%%'", grant.Object, grant.Principal))
	sb.WriteString(";")

	return sb.String(), nil
}

// Extensions and customization
func (g *MySQLGenerator) GenerateCreateExtensionSQL(extension unifiedmodel.Extension) (string, error) {
	// MySQL doesn't have extensions like PostgreSQL, but we can install plugins
	if extension.Name == "" {
		return "", fmt.Errorf("extension name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSTALL PLUGIN %s SONAME '%s.so';", extension.Name, extension.Name))

	return sb.String(), nil
}

// High-level generation methods
func (g *MySQLGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	if model == nil {
		return "", nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string
	var warnings []string

	// Generate in dependency order for MySQL

	// 1. Databases first
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate database %s: %v", database.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 2. Tables (core data structures)
	for _, table := range model.Tables {
		stmt, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate table %s: %v", table.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 3. Views (depend on tables)
	for _, view := range model.Views {
		stmt, err := g.GenerateCreateViewSQL(view)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate view %s: %v", view.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 4. Indexes (depend on tables/views)
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate index %s: %v", index.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 5. Functions and Procedures
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate function %s: %v", fn.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	for _, proc := range model.Procedures {
		stmt, err := g.GenerateCreateProcedureSQL(proc)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate procedure %s: %v", proc.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 6. Triggers (depend on functions and tables)
	for _, trigger := range model.Triggers {
		stmt, err := g.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate trigger %s: %v", trigger.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 7. Sequences (simulated with tables)
	for _, seq := range model.Sequences {
		stmt, err := g.GenerateCreateSequenceSQL(seq)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate sequence %s: %v", seq.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 8. Users (MySQL-specific)
	for _, user := range model.Users {
		stmt, err := g.GenerateCreateUserSQL(user)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate user %s: %v", user.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 9. Grants (should be last)
	for _, grant := range model.Grants {
		stmt, err := g.GenerateCreateGrantSQL(grant)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate grant for %s: %v", grant.Principal, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// Combine all statements
	fullScript := strings.Join(statements, "\n\n")

	return fullScript, warnings, nil
}

func (g *MySQLGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
	if model == nil {
		return nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string

	// Generate in dependency order (same as GenerateSchema but with error propagation)

	// 1. Databases first
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			return nil, fmt.Errorf("failed to generate database %s: %w", database.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 2. Tables
	for _, table := range model.Tables {
		stmt, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			return nil, fmt.Errorf("failed to generate table %s: %w", table.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 3. Views
	for _, view := range model.Views {
		stmt, err := g.GenerateCreateViewSQL(view)
		if err != nil {
			return nil, fmt.Errorf("failed to generate view %s: %w", view.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 4. Indexes
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index %s: %w", index.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 5. Functions
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to generate function %s: %w", fn.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 6. Procedures
	for _, proc := range model.Procedures {
		stmt, err := g.GenerateCreateProcedureSQL(proc)
		if err != nil {
			return nil, fmt.Errorf("failed to generate procedure %s: %w", proc.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 7. Triggers
	for _, trigger := range model.Triggers {
		stmt, err := g.GenerateCreateTriggerSQL(trigger)
		if err != nil {
			return nil, fmt.Errorf("failed to generate trigger %s: %w", trigger.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 8. Sequences
	for _, seq := range model.Sequences {
		stmt, err := g.GenerateCreateSequenceSQL(seq)
		if err != nil {
			return nil, fmt.Errorf("failed to generate sequence %s: %w", seq.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 9. Users
	for _, user := range model.Users {
		stmt, err := g.GenerateCreateUserSQL(user)
		if err != nil {
			return nil, fmt.Errorf("failed to generate user %s: %w", user.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 10. Grants
	for _, grant := range model.Grants {
		stmt, err := g.GenerateCreateGrantSQL(grant)
		if err != nil {
			return nil, fmt.Errorf("failed to generate grant for %s: %w", grant.Principal, err)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// Helper methods

func (g *MySQLGenerator) generateColumnDefinition(col unifiedmodel.Column) (string, error) {
	if col.Name == "" {
		return "", fmt.Errorf("column name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("`%s` %s", col.Name, g.mapDataType(col.DataType)))

	// Add NOT NULL constraint
	if !col.Nullable {
		sb.WriteString(" NOT NULL")
	}

	// Add AUTO_INCREMENT
	if col.AutoIncrement {
		sb.WriteString(" AUTO_INCREMENT")
	}

	// Add DEFAULT value
	if col.Default != "" {
		sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.Default))
	}

	// Add UNIQUE constraint (check options)
	if unique, ok := col.Options["unique"].(bool); ok && unique {
		sb.WriteString(" UNIQUE")
	}

	// Add PRIMARY KEY constraint
	if col.IsPrimaryKey {
		sb.WriteString(" PRIMARY KEY")
	}

	// Add comment (from options)
	if comment, ok := col.Options["comment"].(string); ok && comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(comment, "'", "''")))
	}

	return sb.String(), nil
}

func (g *MySQLGenerator) generateConstraintDefinition(constraint unifiedmodel.Constraint) (string, error) {
	switch constraint.Type {
	case unifiedmodel.ConstraintTypePrimaryKey:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("primary key constraint must have columns")
		}
		var cols []string
		for _, col := range constraint.Columns {
			cols = append(cols, fmt.Sprintf("`%s`", col))
		}
		return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(cols, ", ")), nil

	case unifiedmodel.ConstraintTypeForeignKey:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("foreign key constraint must have columns")
		}
		if constraint.Reference.Table == "" || len(constraint.Reference.Columns) == 0 {
			return "", fmt.Errorf("foreign key constraint must have reference table and columns")
		}

		var sb strings.Builder
		var cols []string
		for _, col := range constraint.Columns {
			cols = append(cols, fmt.Sprintf("`%s`", col))
		}
		var refCols []string
		for _, col := range constraint.Reference.Columns {
			refCols = append(refCols, fmt.Sprintf("`%s`", col))
		}

		sb.WriteString(fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s` (%s)",
			constraint.Name,
			strings.Join(cols, ", "),
			constraint.Reference.Table,
			strings.Join(refCols, ", ")))

		if constraint.Reference.OnUpdate != "" {
			sb.WriteString(fmt.Sprintf(" ON UPDATE %s", constraint.Reference.OnUpdate))
		}
		if constraint.Reference.OnDelete != "" {
			sb.WriteString(fmt.Sprintf(" ON DELETE %s", constraint.Reference.OnDelete))
		}

		return sb.String(), nil

	case unifiedmodel.ConstraintTypeUnique:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("unique constraint must have columns")
		}
		var cols []string
		for _, col := range constraint.Columns {
			cols = append(cols, fmt.Sprintf("`%s`", col))
		}
		return fmt.Sprintf("CONSTRAINT `%s` UNIQUE (%s)", constraint.Name, strings.Join(cols, ", ")), nil

	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Expression == "" {
			return "", fmt.Errorf("check constraint must have expression")
		}
		return fmt.Sprintf("CONSTRAINT `%s` CHECK (%s)", constraint.Name, constraint.Expression), nil

	default:
		return "", nil // Skip unsupported constraint types
	}
}

func (g *MySQLGenerator) mapDataType(dataType string) string {
	// Map common data types to MySQL equivalents
	switch strings.ToLower(dataType) {
	case "int", "integer":
		return "INT"
	case "bigint", "long":
		return "BIGINT"
	case "smallint", "short":
		return "SMALLINT"
	case "tinyint":
		return "TINYINT"
	case "varchar", "string":
		return "VARCHAR(255)" // Default length
	case "text":
		return "TEXT"
	case "longtext":
		return "LONGTEXT"
	case "mediumtext":
		return "MEDIUMTEXT"
	case "char":
		return "CHAR(1)" // Default length
	case "boolean", "bool":
		return "BOOLEAN"
	case "decimal", "numeric":
		return "DECIMAL(10,2)" // Default precision
	case "float", "real":
		return "FLOAT"
	case "double":
		return "DOUBLE"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "datetime", "timestamp":
		return "DATETIME"
	case "json":
		return "JSON"
	case "binary":
		return "BINARY"
	case "varbinary":
		return "VARBINARY(255)"
	case "blob":
		return "BLOB"
	case "longblob":
		return "LONGBLOB"
	case "mediumblob":
		return "MEDIUMBLOB"
	case "tinyblob":
		return "TINYBLOB"
	case "enum":
		return "ENUM" // Will need values
	case "set":
		return "SET" // Will need values
	default:
		// Return as-is for MySQL-specific types
		return dataType
	}
}
