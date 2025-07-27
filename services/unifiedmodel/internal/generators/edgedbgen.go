package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type EdgeDBGenerator struct {
	BaseGenerator
}

func (g *EdgeDBGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	edgeSchema, ok := schema.(models.EdgeDBSchema)
	if !ok {
		return nil, fmt.Errorf("invalid schema type for EdgeDB generator")
	}

	statements := []string{}

	// Create modules
	for _, module := range edgeSchema.Modules {
		statements = append(statements, fmt.Sprintf("CREATE MODULE %s;", module.Name))
	}

	// Create scalar types
	for _, scalar := range edgeSchema.Scalars {
		stmt := fmt.Sprintf("CREATE SCALAR TYPE %s::%s", scalar.Module, scalar.Name)
		if scalar.BaseType != "" {
			stmt += fmt.Sprintf(" EXTENDING %s", scalar.BaseType)
		}
		stmt += ";"
		statements = append(statements, stmt)
	}

	// Create aliases
	for _, alias := range edgeSchema.Aliases {
		stmt := fmt.Sprintf("CREATE ALIAS %s::%s := %s;", alias.Module, alias.Name, alias.Type)
		statements = append(statements, stmt)
	}

	// Create object types
	for _, objType := range edgeSchema.Types {
		if objType.IsAbstract {
			continue // Skip abstract types as they can't be instantiated
		}

		var stmt strings.Builder
		stmt.WriteString(fmt.Sprintf("CREATE TYPE %s::%s", objType.Module, objType.Name))

		// Add base types if any
		if len(objType.Bases) > 0 {
			stmt.WriteString(" EXTENDING " + strings.Join(objType.Bases, ", "))
		}

		stmt.WriteString(" {\n")

		// Add properties
		for _, prop := range objType.Properties {
			stmt.WriteString(fmt.Sprintf("  property %s -> %s", prop.Name, prop.Type))
			if prop.Required || prop.ReadOnly || prop.Default != nil {
				stmt.WriteString(" {\n")
				if prop.Required {
					stmt.WriteString("    required := true;\n")
				}
				if prop.ReadOnly {
					stmt.WriteString("    readonly := true;\n")
				}
				if prop.Default != nil {
					stmt.WriteString(fmt.Sprintf("    default := %v;\n", prop.Default))
				}
				stmt.WriteString("  }")
			}
			stmt.WriteString(";\n")
		}

		// Add links
		for _, link := range objType.Links {
			stmt.WriteString(fmt.Sprintf("  link %s -> %s", link.Name, link.Target))
			if link.Required || link.ReadOnly || link.Cardinality != "" || link.OnTargetDelete != "" {
				stmt.WriteString(" {\n")
				if link.Required {
					stmt.WriteString("    required := true;\n")
				}
				if link.ReadOnly {
					stmt.WriteString("    readonly := true;\n")
				}
				if link.Cardinality != "" {
					stmt.WriteString(fmt.Sprintf("    cardinality := %s;\n", link.Cardinality))
				}
				if link.OnTargetDelete != "" {
					stmt.WriteString(fmt.Sprintf("    on target delete %s;\n", link.OnTargetDelete))
				}
				stmt.WriteString("  }")
			}
			stmt.WriteString(";\n")
		}

		stmt.WriteString("};")
		statements = append(statements, stmt.String())
	}

	// Create functions
	for _, edgeFunc := range edgeSchema.Functions {
		var stmt strings.Builder
		stmt.WriteString(fmt.Sprintf("CREATE FUNCTION %s::%s(", edgeFunc.Module, edgeFunc.Name))

		// Add parameters
		params := make([]string, len(edgeFunc.Parameters))
		for i, param := range edgeFunc.Parameters {
			params[i] = fmt.Sprintf("%s: %s", param.Name, param.Type)
		}
		stmt.WriteString(strings.Join(params, ", "))
		stmt.WriteString(") -> ")
		stmt.WriteString(edgeFunc.ReturnType)
		stmt.WriteString(" {\n")
		stmt.WriteString(edgeFunc.Body)
		stmt.WriteString("\n};")
		statements = append(statements, stmt.String())
	}

	// Create extensions
	for _, ext := range edgeSchema.Extensions {
		stmt := fmt.Sprintf("CREATE EXTENSION %s VERSION %s;", ext.Name, ext.Version)
		if ext.Description != "" {
			stmt = fmt.Sprintf("%s -- %s", stmt, ext.Description)
		}
		statements = append(statements, stmt)
	}

	return statements, nil
}

// GenerateCreateTableSQL implements StatementGenerator interface
func (g *EdgeDBGenerator) GenerateCreateTableSQL(table models.Table) (string, error) {
	return g.BaseGenerator.GenerateCreateTableSQL(table)
}

// GenerateCreateFunctionSQL implements StatementGenerator interface
func (g *EdgeDBGenerator) GenerateCreateFunctionSQL(fn models.Function) (string, error) {
	return g.BaseGenerator.GenerateCreateFunctionSQL(fn)
}

// GenerateCreateTriggerSQL implements StatementGenerator interface
func (g *EdgeDBGenerator) GenerateCreateTriggerSQL(trigger models.Trigger) (string, error) {
	return g.BaseGenerator.GenerateCreateTriggerSQL(trigger)
}

// GenerateCreateSequenceSQL implements StatementGenerator interface
func (g *EdgeDBGenerator) GenerateCreateSequenceSQL(seq models.Sequence) (string, error) {
	return g.BaseGenerator.GenerateCreateSequenceSQL(seq)
}

// GenerateSchema implements StatementGenerator interface
func (g *EdgeDBGenerator) GenerateSchema(model *models.UnifiedModel) (string, []string, error) {
	return g.BaseGenerator.GenerateSchema(model)
}
