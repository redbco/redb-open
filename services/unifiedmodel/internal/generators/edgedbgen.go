package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// EdgeDBGenerator implements EdgeQL generation for EdgeDB
type EdgeDBGenerator struct {
	BaseGenerator
}

// NewEdgeDBGenerator creates a new EdgeDB generator
func NewEdgeDBGenerator() *EdgeDBGenerator {
	return &EdgeDBGenerator{}
}

// Override BaseGenerator methods to provide EdgeDB-specific implementations

// Structural organization (EdgeDB uses modules and schemas)
func (g *EdgeDBGenerator) GenerateCreateDatabaseSQL(database unifiedmodel.Database) (string, error) {
	if database.Name == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	// EdgeDB database creation (administrative command)
	return fmt.Sprintf("CREATE DATABASE %s;", database.Name), nil
}

func (g *EdgeDBGenerator) GenerateCreateSchemaSQL(schema unifiedmodel.Schema) (string, error) {
	if schema.Name == "" {
		return "", fmt.Errorf("schema name cannot be empty")
	}

	// EdgeDB uses modules instead of schemas
	return fmt.Sprintf("CREATE MODULE %s;", schema.Name), nil
}

// Primary Data Containers (EdgeDB object types)
func (g *EdgeDBGenerator) GenerateCreateTableSQL(table unifiedmodel.Table) (string, error) {
	if table.Name == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	// EdgeDB uses object types instead of tables
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TYPE %s", table.Name))

	// Add extending clause if specified in options
	if extending, ok := table.Options["extending"].([]string); ok && len(extending) > 0 {
		sb.WriteString(fmt.Sprintf(" EXTENDING %s", strings.Join(extending, ", ")))
	}

	sb.WriteString(" {\n")

	// Add properties (columns become properties)
	for _, col := range table.Columns {
		propDef, err := g.generatePropertyDefinition(col)
		if err != nil {
			return "", fmt.Errorf("failed to generate property definition for %s: %w", col.Name, err)
		}
		sb.WriteString(fmt.Sprintf("  %s\n", propDef))
	}

	// Add constraints as computed properties or constraints
	for _, constraint := range table.Constraints {
		constraintDef, err := g.generateConstraintDefinition(constraint)
		if err != nil {
			return "", fmt.Errorf("failed to generate constraint definition for %s: %w", constraint.Name, err)
		}
		if constraintDef != "" {
			sb.WriteString(fmt.Sprintf("  %s\n", constraintDef))
		}
	}

	sb.WriteString("};")

	return sb.String(), nil
}

// Virtual Data Containers
func (g *EdgeDBGenerator) GenerateCreateViewSQL(view unifiedmodel.View) (string, error) {
	if view.Name == "" {
		return "", fmt.Errorf("view name cannot be empty")
	}
	if view.Definition == "" {
		return "", fmt.Errorf("view definition cannot be empty")
	}

	// EdgeDB uses aliases for view-like functionality
	return fmt.Sprintf("CREATE ALIAS %s := (%s);", view.Name, view.Definition), nil
}

// Integrity, performance and identity objects
func (g *EdgeDBGenerator) GenerateCreateIndexSQL(index unifiedmodel.Index) (string, error) {
	if index.Name == "" {
		return "", fmt.Errorf("index name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE INDEX %s", index.Name))

	// Add ON clause - determine the object type from options
	objectType := "ObjectType"
	if objType, ok := index.Options["object_type"].(string); ok && objType != "" {
		objectType = objType
	}

	sb.WriteString(fmt.Sprintf(" ON %s", objectType))

	// Add index expression
	if index.Expression != "" {
		sb.WriteString(fmt.Sprintf(" (%s)", index.Expression))
	} else if len(index.Columns) > 0 {
		var cols []string
		for _, col := range index.Columns {
			cols = append(cols, fmt.Sprintf(".%s", col))
		}
		sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(cols, ", ")))
	} else if len(index.Fields) > 0 {
		var fields []string
		for _, field := range index.Fields {
			fields = append(fields, fmt.Sprintf(".%s", field))
		}
		sb.WriteString(fmt.Sprintf(" (%s)", strings.Join(fields, ", ")))
	} else {
		return "", fmt.Errorf("index must have columns, fields, or expression")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *EdgeDBGenerator) GenerateCreateConstraintSQL(constraint unifiedmodel.Constraint) (string, error) {
	if constraint.Name == "" {
		return "", fmt.Errorf("constraint name cannot be empty")
	}

	var sb strings.Builder

	// EdgeDB constraints are defined within object types
	// This generates a constraint that can be added to an object type
	sb.WriteString(fmt.Sprintf("constraint %s", constraint.Name))

	switch constraint.Type {
	case unifiedmodel.ConstraintTypeUnique:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("unique constraint must have columns")
		}
		sb.WriteString(" := exclusive")

	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Expression == "" {
			return "", fmt.Errorf("check constraint must have expression")
		}
		sb.WriteString(fmt.Sprintf(" := (%s)", constraint.Expression))

	default:
		return "", fmt.Errorf("unsupported constraint type for EdgeDB: %s", constraint.Type)
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Data types and custom objects
func (g *EdgeDBGenerator) GenerateCreateTypeSQL(dataType unifiedmodel.Type) (string, error) {
	if dataType.Name == "" {
		return "", fmt.Errorf("type name cannot be empty")
	}

	var sb strings.Builder

	switch dataType.Category {
	case "scalar":
		// Create scalar type
		sb.WriteString(fmt.Sprintf("CREATE SCALAR TYPE %s", dataType.Name))
		// Check if base type is specified in definition
		if baseType, ok := dataType.Definition["base_type"].(string); ok && baseType != "" {
			sb.WriteString(fmt.Sprintf(" EXTENDING %s", baseType))
		}

	case "enum":
		// Create enum type
		sb.WriteString(fmt.Sprintf("CREATE SCALAR TYPE %s EXTENDING enum", dataType.Name))
		if values, ok := dataType.Definition["values"].([]interface{}); ok && len(values) > 0 {
			var enumValues []string
			for _, value := range values {
				enumValues = append(enumValues, fmt.Sprintf("'%s'", value))
			}
			sb.WriteString(fmt.Sprintf("<%s>", strings.Join(enumValues, ", ")))
		}

	case "composite":
		// Create object type (EdgeDB's composite type equivalent)
		sb.WriteString(fmt.Sprintf("CREATE TYPE %s", dataType.Name))
		if attributes, ok := dataType.Definition["attributes"].([]interface{}); ok && len(attributes) > 0 {
			sb.WriteString(" {\n")
			for _, attr := range attributes {
				if attrMap, ok := attr.(map[string]interface{}); ok {
					name, _ := attrMap["name"].(string)
					attrType, _ := attrMap["type"].(string)
					if name != "" && attrType != "" {
						sb.WriteString(fmt.Sprintf("  property %s -> %s;\n", name, g.mapDataType(attrType)))
					}
				}
			}
			sb.WriteString("}")
		}

	default:
		return "", fmt.Errorf("unsupported type category for EdgeDB: %s", dataType.Category)
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// Executable code objects
func (g *EdgeDBGenerator) GenerateCreateFunctionSQL(fn unifiedmodel.Function) (string, error) {
	if fn.Name == "" {
		return "", fmt.Errorf("function name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE FUNCTION %s(", fn.Name))

	// Add parameters
	var paramDefs []string
	for _, arg := range fn.Arguments {
		paramDef := fmt.Sprintf("%s: %s", arg.Name, g.mapDataType(arg.Type))
		paramDefs = append(paramDefs, paramDef)
	}
	sb.WriteString(strings.Join(paramDefs, ", "))
	sb.WriteString(")")

	// Add return type
	if fn.Returns != "" {
		sb.WriteString(fmt.Sprintf(" -> %s", g.mapDataType(fn.Returns)))
	} else {
		sb.WriteString(" -> std::str") // Default return type
	}

	// Add function body
	sb.WriteString(" {\n")
	if fn.Definition != "" {
		sb.WriteString(fmt.Sprintf("  %s\n", fn.Definition))
	} else {
		sb.WriteString("  # Function implementation goes here\n")
	}
	sb.WriteString("};")

	return sb.String(), nil
}

// Extensions and customization
func (g *EdgeDBGenerator) GenerateCreateExtensionSQL(extension unifiedmodel.Extension) (string, error) {
	if extension.Name == "" {
		return "", fmt.Errorf("extension name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE EXTENSION %s", extension.Name))

	// Add version if specified
	if extension.Version != "" {
		sb.WriteString(fmt.Sprintf(" VERSION '%s'", extension.Version))
	}

	sb.WriteString(";")

	return sb.String(), nil
}

// EdgeDB-specific objects (using existing UnifiedModel types)
func (g *EdgeDBGenerator) GenerateCreateAliasSQL(alias unifiedmodel.Alias) (string, error) {
	if alias.Alias == "" {
		return "", fmt.Errorf("alias name cannot be empty")
	}
	if alias.On == "" {
		return "", fmt.Errorf("alias target cannot be empty")
	}

	// EdgeDB alias creation - alias.On is the expression, alias.Alias is the name
	return fmt.Sprintf("CREATE ALIAS %s := %s;", alias.Alias, alias.On), nil
}

// High-level generation methods
func (g *EdgeDBGenerator) GenerateSchema(model *unifiedmodel.UnifiedModel) (string, []string, error) {
	if model == nil {
		return "", nil, fmt.Errorf("unified model cannot be nil")
	}

	var statements []string
	var warnings []string

	// Generate in dependency order for EdgeDB

	// 1. Databases first
	for _, database := range model.Databases {
		stmt, err := g.GenerateCreateDatabaseSQL(database)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate database %s: %v", database.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 2. Extensions (should be early)
	for _, extension := range model.Extensions {
		stmt, err := g.GenerateCreateExtensionSQL(extension)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate extension %s: %v", extension.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 3. Modules (EdgeDB's schema equivalent)
	for _, schema := range model.Schemas {
		stmt, err := g.GenerateCreateSchemaSQL(schema)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate module %s: %v", schema.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 4. Scalar and enum types
	for _, dataType := range model.Types {
		stmt, err := g.GenerateCreateTypeSQL(dataType)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate type %s: %v", dataType.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 5. Aliases (computed expressions)
	for _, alias := range model.Aliases {
		stmt, err := g.GenerateCreateAliasSQL(alias)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate alias %s: %v", alias.Alias, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 6. Object types (EdgeDB's tables)
	for _, table := range model.Tables {
		stmt, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate object type %s: %v", table.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 7. Views (as aliases)
	for _, view := range model.Views {
		stmt, err := g.GenerateCreateViewSQL(view)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate view alias %s: %v", view.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 8. Indexes
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate index %s: %v", index.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// 9. Functions
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Failed to generate function %s: %v", fn.Name, err))
			continue
		}
		statements = append(statements, stmt)
	}

	// Note: Links are not part of the base UnifiedModel structure
	// They would be handled as part of object type definitions

	// Combine all statements
	fullScript := strings.Join(statements, "\n\n")

	return fullScript, warnings, nil
}

func (g *EdgeDBGenerator) GenerateCreateStatements(model *unifiedmodel.UnifiedModel) ([]string, error) {
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

	// 2. Extensions
	for _, extension := range model.Extensions {
		stmt, err := g.GenerateCreateExtensionSQL(extension)
		if err != nil {
			return nil, fmt.Errorf("failed to generate extension %s: %w", extension.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 3. Modules
	for _, schema := range model.Schemas {
		stmt, err := g.GenerateCreateSchemaSQL(schema)
		if err != nil {
			return nil, fmt.Errorf("failed to generate module %s: %w", schema.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 4. Types
	for _, dataType := range model.Types {
		stmt, err := g.GenerateCreateTypeSQL(dataType)
		if err != nil {
			return nil, fmt.Errorf("failed to generate type %s: %w", dataType.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 5. Aliases
	for _, alias := range model.Aliases {
		stmt, err := g.GenerateCreateAliasSQL(alias)
		if err != nil {
			return nil, fmt.Errorf("failed to generate alias %s: %w", alias.Alias, err)
		}
		statements = append(statements, stmt)
	}

	// 6. Object types
	for _, table := range model.Tables {
		stmt, err := g.GenerateCreateTableSQL(table)
		if err != nil {
			return nil, fmt.Errorf("failed to generate object type %s: %w", table.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 7. Views
	for _, view := range model.Views {
		stmt, err := g.GenerateCreateViewSQL(view)
		if err != nil {
			return nil, fmt.Errorf("failed to generate view alias %s: %w", view.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 8. Indexes
	for _, index := range model.Indexes {
		stmt, err := g.GenerateCreateIndexSQL(index)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index %s: %w", index.Name, err)
		}
		statements = append(statements, stmt)
	}

	// 9. Functions
	for _, fn := range model.Functions {
		stmt, err := g.GenerateCreateFunctionSQL(fn)
		if err != nil {
			return nil, fmt.Errorf("failed to generate function %s: %w", fn.Name, err)
		}
		statements = append(statements, stmt)
	}

	// Note: Links are not part of the base UnifiedModel structure
	// They would be handled as part of object type definitions

	return statements, nil
}

// Helper methods

func (g *EdgeDBGenerator) generatePropertyDefinition(col unifiedmodel.Column) (string, error) {
	if col.Name == "" {
		return "", fmt.Errorf("column name cannot be empty")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("property %s -> %s", col.Name, g.mapDataType(col.DataType)))

	// Add property modifiers if needed
	var modifiers []string

	if !col.Nullable {
		modifiers = append(modifiers, "required := true")
	}

	if col.Default != "" {
		modifiers = append(modifiers, fmt.Sprintf("default := %s", col.Default))
	}

	// Add readonly if specified in options
	if readonly, ok := col.Options["readonly"].(bool); ok && readonly {
		modifiers = append(modifiers, "readonly := true")
	}

	if len(modifiers) > 0 {
		sb.WriteString(" {\n")
		for _, modifier := range modifiers {
			sb.WriteString(fmt.Sprintf("    %s;\n", modifier))
		}
		sb.WriteString("  }")
	}

	sb.WriteString(";")

	return sb.String(), nil
}

func (g *EdgeDBGenerator) generateConstraintDefinition(constraint unifiedmodel.Constraint) (string, error) {
	switch constraint.Type {
	case unifiedmodel.ConstraintTypeUnique:
		if len(constraint.Columns) == 0 {
			return "", fmt.Errorf("unique constraint must have columns")
		}
		// EdgeDB exclusive constraint
		return fmt.Sprintf("constraint exclusive on (.%s);", strings.Join(constraint.Columns, ", .")), nil

	case unifiedmodel.ConstraintTypeCheck:
		if constraint.Expression == "" {
			return "", fmt.Errorf("check constraint must have expression")
		}
		return fmt.Sprintf("constraint expression on (%s);", constraint.Expression), nil

	default:
		// Skip unsupported constraint types
		return "", nil
	}
}

func (g *EdgeDBGenerator) mapDataType(dataType string) string {
	// Map common data types to EdgeDB equivalents
	switch strings.ToLower(dataType) {
	case "int", "integer":
		return "std::int32"
	case "bigint", "long":
		return "std::int64"
	case "smallint", "short":
		return "std::int16"
	case "varchar", "string", "text":
		return "std::str"
	case "boolean", "bool":
		return "std::bool"
	case "decimal", "numeric":
		return "std::decimal"
	case "float", "real":
		return "std::float32"
	case "double":
		return "std::float64"
	case "date":
		return "cal::local_date"
	case "time":
		return "cal::local_time"
	case "datetime", "timestamp":
		return "std::datetime"
	case "json":
		return "std::json"
	case "uuid":
		return "std::uuid"
	case "bytes", "binary":
		return "std::bytes"
	case "array":
		return "array<std::str>" // Default array type
	case "tuple":
		return "tuple<std::str>" // Default tuple type
	default:
		// Return as-is for EdgeDB-specific types or custom types
		if strings.Contains(dataType, "::") {
			return dataType // Already qualified
		}
		return fmt.Sprintf("std::%s", dataType)
	}
}
