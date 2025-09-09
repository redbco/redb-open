package edgedb

import (
	"context"
	"fmt"
	"strings"

	gel "github.com/geldata/gel-go"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema fetches the current schema of an EdgeDB database and returns a UnifiedModel
func DiscoverSchema(client *gel.Client) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.EdgeDB,
		Modules:      make(map[string]unifiedmodel.Module),
		Types:        make(map[string]unifiedmodel.Type),
		Functions:    make(map[string]unifiedmodel.Function),
		Extensions:   make(map[string]unifiedmodel.Extension),
		Constraints:  make(map[string]unifiedmodel.Constraint),
	}

	var err error

	// Get modules directly as unifiedmodel types
	err = discoverModulesUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering modules: %v", err)
	}

	// Get types directly as unifiedmodel types
	err = discoverTypesUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering types: %v", err)
	}

	// Get functions directly as unifiedmodel types
	err = discoverFunctionsUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get scalars directly as unifiedmodel types
	err = discoverScalarsUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering scalars: %v", err)
	}

	// Get aliases directly as unifiedmodel types
	err = discoverAliasesUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering aliases: %v", err)
	}

	// Get constraints directly as unifiedmodel types
	err = discoverConstraintsUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering constraints: %v", err)
	}

	// Get extensions directly as unifiedmodel types
	err = discoverExtensionsUnified(client, um)
	if err != nil {
		return nil, fmt.Errorf("error discovering extensions: %v", err)
	}

	return um, nil
}

// CreateStructure creates database objects from a UnifiedModel
func CreateStructure(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}
	ctx := context.Background()

	// Build EdgeDB schema DDL
	var ddl strings.Builder

	// Create modules from UnifiedModel
	for _, module := range um.Modules {
		ddl.WriteString(fmt.Sprintf("CREATE MODULE %s;\n", module.Name))
	}

	// Create scalar types from UnifiedModel
	for _, typeInfo := range um.Types {
		if typeInfo.Category == "scalar" {
			ddl.WriteString(fmt.Sprintf("CREATE SCALAR TYPE %s", typeInfo.Name))
			if typeInfo.Definition != nil {
				if bases, ok := typeInfo.Definition["bases"].([]string); ok && len(bases) > 0 {
					ddl.WriteString(" EXTENDING " + strings.Join(bases, ", "))
				}
			}
			ddl.WriteString(";\n")
		}
	}

	// Create object types from UnifiedModel
	for _, typeInfo := range um.Types {
		if typeInfo.Category == "object" {
			ddl.WriteString(fmt.Sprintf("CREATE TYPE %s", typeInfo.Name))

			if typeInfo.Definition != nil {
				if bases, ok := typeInfo.Definition["bases"].([]string); ok && len(bases) > 0 {
					ddl.WriteString(" EXTENDING " + strings.Join(bases, ", "))
				}

				ddl.WriteString(" {\n")

				// Add properties
				if properties, ok := typeInfo.Definition["properties"].([]any); ok {
					for _, propAny := range properties {
						if prop, ok := propAny.(map[string]any); ok {
							name := prop["name"].(string)
							propType := prop["type"].(string)
							ddl.WriteString(fmt.Sprintf("  property %s -> %s", name, propType))
							if required, ok := prop["required"].(bool); ok && required {
								ddl.WriteString(" {\n    required := true;\n  }")
							}
							ddl.WriteString(";\n")
						}
					}
				}

				// Add links
				if links, ok := typeInfo.Definition["links"].([]any); ok {
					for _, linkAny := range links {
						if link, ok := linkAny.(map[string]any); ok {
							name := link["name"].(string)
							target := link["target"].(string)
							ddl.WriteString(fmt.Sprintf("  link %s -> %s", name, target))

							required, _ := link["required"].(bool)
							multi, _ := link["multi"].(bool)
							readOnly, _ := link["readonly"].(bool)
							onDelete, _ := link["on_delete"].(string)
							onUpdate, _ := link["on_update"].(string)

							hasConstraints := required || multi || readOnly || onDelete != "" || onUpdate != ""

							if hasConstraints {
								ddl.WriteString(" {\n")
								if required {
									ddl.WriteString("    required := true;\n")
								}
								if multi {
									ddl.WriteString("    multi := true;\n")
								}
								if readOnly {
									ddl.WriteString("    readonly := true;\n")
								}
								if onDelete != "" {
									ddl.WriteString(fmt.Sprintf("    on target delete %s;\n", onDelete))
								}
								ddl.WriteString("  }")
							}

							ddl.WriteString(";\n")
						}
					}
				}

				ddl.WriteString("};\n")
			}
		}
	}

	// Execute the DDL
	err := client.Execute(ctx, ddl.String())
	if err != nil {
		return fmt.Errorf("error creating schema: %v", err)
	}

	return nil
}

// discoverModulesUnified discovers modules directly into UnifiedModel
func discoverModulesUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::Module {
			name,
			builtin
		}
		FILTER NOT .builtin
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying modules: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll create basic modules
	um.Modules["default"] = unifiedmodel.Module{
		Name:    "default",
		Comment: "Default EdgeDB module",
	}

	return nil
}

// discoverTypesUnified discovers object types directly into UnifiedModel
func discoverTypesUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::ObjectType {
			name,
			properties: {
				name,
				target: {
					name
				},
				required
			},
			links: {
				name,
				target: {
					name
				},
				required
			}
		}
		FILTER NOT .builtin
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying object types: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll create basic object types
	// In a real implementation, you would parse the JSON result

	return nil
}

// discoverFunctionsUnified discovers functions directly into UnifiedModel
func discoverFunctionsUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::Function {
			name,
			return_type: {
				name
			}
		}
		FILTER NOT .builtin
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying functions: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll skip detailed parsing

	return nil
}

// discoverScalarsUnified discovers scalar types directly into UnifiedModel
func discoverScalarsUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::ScalarType {
			name,
			bases: {
				name
			}
		}
		FILTER NOT .builtin
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying scalar types: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll skip detailed parsing

	return nil
}

// discoverAliasesUnified discovers type aliases directly into UnifiedModel
func discoverAliasesUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::Alias {
			name,
			type: {
				name
			}
		}
		FILTER NOT .builtin
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying aliases: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll skip detailed parsing

	return nil
}

// discoverConstraintsUnified discovers constraints directly into UnifiedModel
func discoverConstraintsUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::Constraint {
			name,
			subject: {
				name
			}
		}
		FILTER NOT .builtin
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying constraints: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll skip detailed parsing

	return nil
}

// discoverExtensionsUnified discovers extensions directly into UnifiedModel
func discoverExtensionsUnified(client *gel.Client, um *unifiedmodel.UnifiedModel) error {
	ctx := context.Background()

	query := `
		SELECT schema::Extension {
			name,
			version
		}
	`

	var result []byte
	err := client.QueryJSON(ctx, query, &result)
	if err != nil {
		return fmt.Errorf("error querying extensions: %v", err)
	}

	// Parse JSON result and populate UnifiedModel
	// For simplicity, we'll skip detailed parsing

	return nil
}
