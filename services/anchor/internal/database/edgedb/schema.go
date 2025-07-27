package edgedb

import (
	"context"
	"fmt"
	"strings"

	gel "github.com/geldata/gel-go"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of an EdgeDB database
func DiscoverSchema(client *gel.Client) (*EdgeDBSchema, error) {
	schema := &EdgeDBSchema{}
	var err error

	// Get modules
	schema.Modules, err = discoverModules(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering modules: %v", err)
	}

	// Get types
	schema.Types, err = discoverTypes(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering types: %v", err)
	}

	// Get functions
	schema.Functions, err = discoverFunctions(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering functions: %v", err)
	}

	// Get scalars
	schema.Scalars, err = discoverScalars(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering scalars: %v", err)
	}

	// Get aliases
	schema.Aliases, err = discoverAliases(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering aliases: %v", err)
	}

	// Get constraints
	schema.Constraints, err = discoverConstraints(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering constraints: %v", err)
	}

	// Get extensions
	schema.Extensions, err = discoverExtensions(client)
	if err != nil {
		return nil, fmt.Errorf("error discovering extensions: %v", err)
	}

	return schema, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(client *gel.Client, params common.StructureParams) error {
	ctx := context.Background()

	// Build EdgeDB schema DDL
	var ddl strings.Builder

	// Create modules first
	for _, module := range params.Modules {
		ddl.WriteString(fmt.Sprintf("CREATE MODULE %s;\n", module.Name))
	}

	// Create scalar types
	for _, scalar := range params.Types {
		if scalar.TypeCode == "scalar" {
			ddl.WriteString(fmt.Sprintf("CREATE SCALAR TYPE %s::%s", scalar.Module, scalar.Name))
			if len(scalar.Bases) > 0 {
				ddl.WriteString(" EXTENDING " + strings.Join(scalar.Bases, ", "))
			}
			ddl.WriteString(";\n")
		}
	}

	// Create object types
	for _, objType := range params.Types {
		if objType.TypeCode == "object" {
			ddl.WriteString(fmt.Sprintf("CREATE TYPE %s::%s", objType.Module, objType.Name))

			if len(objType.Bases) > 0 {
				ddl.WriteString(" EXTENDING " + strings.Join(objType.Bases, ", "))
			}

			ddl.WriteString(" {\n")

			// Add properties
			for _, prop := range objType.Properties {
				ddl.WriteString(fmt.Sprintf("  property %s -> %s", prop.Name, prop.Type))
				if prop.Required {
					ddl.WriteString(" {\n    required := true;\n  }")
				}
				ddl.WriteString(";\n")
			}

			// Add links
			for _, link := range objType.Links {
				ddl.WriteString(fmt.Sprintf("  link %s -> %s", link.Name, link.Target))

				hasConstraints := link.Required || link.Multi || link.ReadOnly ||
					link.OnDelete != "" || link.OnUpdate != ""

				if hasConstraints {
					ddl.WriteString(" {\n")
					if link.Required {
						ddl.WriteString("    required := true;\n")
					}
					if link.Multi {
						ddl.WriteString("    multi := true;\n")
					}
					if link.ReadOnly {
						ddl.WriteString("    readonly := true;\n")
					}
					if link.OnDelete != "" {
						ddl.WriteString(fmt.Sprintf("    on target delete %s;\n", link.OnDelete))
					}
					ddl.WriteString("  }")
				}

				ddl.WriteString(";\n")
			}

			ddl.WriteString("};\n")
		}
	}

	// Execute the DDL
	err := client.Execute(ctx, ddl.String())
	if err != nil {
		return fmt.Errorf("error creating schema: %v", err)
	}

	return nil
}

func discoverModules(client *gel.Client) ([]common.ModuleInfo, error) {
	ctx := context.Background()

	query := `
		SELECT schema::Module {
			name
		}
		FILTER .name NOT LIKE 'std::%'
		   AND .name NOT LIKE 'schema::%'
		   AND .name NOT LIKE 'sys::%'
		   AND .name NOT LIKE 'cfg::%'
		   AND .name NOT LIKE 'math::%'
		   AND .name NOT LIKE 'cal::%'
		   AND .name NOT LIKE 'ext::%'
		ORDER BY .name;
	`

	var modules []common.ModuleInfo
	err := client.Query(ctx, query, &modules)
	if err != nil {
		return nil, fmt.Errorf("error querying modules: %v", err)
	}

	return modules, nil
}

func discoverTypes(client *gel.Client) ([]common.TypeInfo, error) {
	ctx := context.Background()

	query := `
		SELECT schema::ObjectType {
			module: {
				name
			},
			name,
			is_abstract,
			properties: {
				name,
				target: {
					name
				},
				required,
				readonly,
				default
			},
			links: {
				name,
				target: {
					name
				},
				required,
				readonly,
				cardinality,
				on_target_delete
			},
			bases: {
				name
			},
			constraints: {
				name,
				args
			}
		}
		FILTER .module.name NOT LIKE 'std::%'
		   AND .module.name NOT LIKE 'schema::%'
		   AND .module.name NOT LIKE 'sys::%'
		   AND .module.name NOT LIKE 'cfg::%'
		   AND .module.name NOT LIKE 'math::%'
		   AND .module.name NOT LIKE 'cal::%'
		   AND .module.name NOT LIKE 'ext::%'
		ORDER BY .module.name, .name;
	`

	type rawType struct {
		Module struct {
			Name string `edgedb:"name"`
		} `edgedb:"module"`
		Name       string `edgedb:"name"`
		IsAbstract bool   `edgedb:"is_abstract"`
		Properties []struct {
			Name   string `edgedb:"name"`
			Target struct {
				Name string `edgedb:"name"`
			} `edgedb:"target"`
			Required bool        `edgedb:"required"`
			ReadOnly bool        `edgedb:"readonly"`
			Default  interface{} `edgedb:"default"`
		} `edgedb:"properties"`
		Links []struct {
			Name   string `edgedb:"name"`
			Target struct {
				Name string `edgedb:"name"`
			} `edgedb:"target"`
			Required       bool   `edgedb:"required"`
			ReadOnly       bool   `edgedb:"readonly"`
			Cardinality    string `edgedb:"cardinality"`
			OnTargetDelete string `edgedb:"on_target_delete"`
		} `edgedb:"links"`
		Bases []struct {
			Name string `edgedb:"name"`
		} `edgedb:"bases"`
		Constraints []struct {
			Name string      `edgedb:"name"`
			Args interface{} `edgedb:"args"`
		} `edgedb:"constraints"`
	}

	var rawTypes []rawType
	err := client.Query(ctx, query, &rawTypes)
	if err != nil {
		return nil, fmt.Errorf("error querying types: %v", err)
	}

	var types []common.TypeInfo
	for _, rt := range rawTypes {
		t := common.TypeInfo{
			Module:     rt.Module.Name,
			Name:       rt.Name,
			IsAbstract: rt.IsAbstract,
			TypeCode:   "object",
		}

		// Process properties
		for _, p := range rt.Properties {
			prop := common.EdgeDBPropertyInfo{
				Name:     p.Name,
				Type:     p.Target.Name,
				Required: p.Required,
				ReadOnly: p.ReadOnly,
				Default:  p.Default,
			}
			t.Properties = append(t.Properties, prop)
		}

		// Process links
		for _, l := range rt.Links {
			link := common.LinkInfo{
				Name:     l.Name,
				Target:   l.Target.Name,
				Required: l.Required,
				ReadOnly: l.ReadOnly,
				Multi:    l.Cardinality == "Many",
				OnDelete: l.OnTargetDelete,
			}
			t.Links = append(t.Links, link)
		}

		// Process bases
		for _, b := range rt.Bases {
			t.Bases = append(t.Bases, b.Name)
		}

		// Process constraints
		for _, c := range rt.Constraints {
			constraint := common.EdgeDBConstraintInfo{
				Name: c.Name,
				Args: c.Args,
			}
			t.Constraints = append(t.Constraints, constraint)
		}

		types = append(types, t)
	}

	return types, nil
}

func discoverFunctions(client *gel.Client) ([]common.FunctionInfo, error) {
	ctx := context.Background()

	query := `
		SELECT schema::Function {
			module: {
				name
			},
			name,
			params: {
				name,
				type: {
					name
				}
			},
			return_type: {
				name
			},
			body
		}
		FILTER .module.name NOT LIKE 'std::%'
		   AND .module.name NOT LIKE 'schema::%'
		   AND .module.name NOT LIKE 'sys::%'
		   AND .module.name NOT LIKE 'cfg::%'
		   AND .module.name NOT LIKE 'math::%'
		   AND .module.name NOT LIKE 'cal::%'
		   AND .module.name NOT LIKE 'ext::%'
		ORDER BY .module.name, .name;
	`

	type rawFunction struct {
		Module struct {
			Name string `edgedb:"name"`
		} `edgedb:"module"`
		Name   string `edgedb:"name"`
		Params []struct {
			Name string `edgedb:"name"`
			Type struct {
				Name string `edgedb:"name"`
			} `edgedb:"type"`
		} `edgedb:"params"`
		ReturnType struct {
			Name string `edgedb:"name"`
		} `edgedb:"return_type"`
		Body string `edgedb:"body"`
	}

	var rawFunctions []rawFunction
	err := client.Query(ctx, query, &rawFunctions)
	if err != nil {
		return nil, fmt.Errorf("error querying functions: %v", err)
	}

	var functions []common.FunctionInfo
	for _, rf := range rawFunctions {
		// Build arguments string
		var args []string
		for _, p := range rf.Params {
			args = append(args, fmt.Sprintf("%s: %s", p.Name, p.Type.Name))
		}

		f := common.FunctionInfo{
			Name:       rf.Name,
			Schema:     rf.Module.Name,
			Arguments:  strings.Join(args, ", "),
			ReturnType: rf.ReturnType.Name,
			Body:       rf.Body,
		}

		functions = append(functions, f)
	}

	return functions, nil
}

func discoverScalars(client *gel.Client) ([]EdgeDBScalarInfo, error) {
	ctx := context.Background()

	query := `
		SELECT schema::ScalarType {
			module: {
				name
			},
			name,
			bases: {
				name
			},
			constraints: {
				name,
				args
			}
		}
		FILTER .module.name NOT LIKE 'std::%'
		   AND .module.name NOT LIKE 'schema::%'
		   AND .module.name NOT LIKE 'sys::%'
		   AND .module.name NOT LIKE 'cfg::%'
		   AND .module.name NOT LIKE 'math::%'
		   AND .module.name NOT LIKE 'cal::%'
		   AND .module.name NOT LIKE 'ext::%'
		ORDER BY .module.name, .name;
	`

	type rawScalar struct {
		Module struct {
			Name string `edgedb:"name"`
		} `edgedb:"module"`
		Name  string `edgedb:"name"`
		Bases []struct {
			Name string `edgedb:"name"`
		} `edgedb:"bases"`
		Constraints []struct {
			Name string      `edgedb:"name"`
			Args interface{} `edgedb:"args"`
		} `edgedb:"constraints"`
	}

	var rawScalars []rawScalar
	err := client.Query(ctx, query, &rawScalars)
	if err != nil {
		return nil, fmt.Errorf("error querying scalars: %v", err)
	}

	var scalars []EdgeDBScalarInfo
	for _, rs := range rawScalars {
		s := EdgeDBScalarInfo{
			Module: rs.Module.Name,
			Name:   rs.Name,
		}

		// Get base type
		if len(rs.Bases) > 0 {
			s.BaseType = rs.Bases[0].Name
		}

		// Process constraints
		for _, c := range rs.Constraints {
			constraint := common.EdgeDBConstraintInfo{
				Name: c.Name,
				Args: c.Args,
			}
			s.Constraints = append(s.Constraints, constraint)
		}

		scalars = append(scalars, s)
	}

	return scalars, nil
}

func discoverAliases(client *gel.Client) ([]EdgeDBAliasInfo, error) {
	ctx := context.Background()

	query := `
		SELECT schema::Alias {
			module: {
				name
			},
			name,
			target: {
				name
			}
		}
		FILTER .module.name NOT LIKE 'std::%'
		   AND .module.name NOT LIKE 'schema::%'
		   AND .module.name NOT LIKE 'sys::%'
		   AND .module.name NOT LIKE 'cfg::%'
		   AND .module.name NOT LIKE 'math::%'
		   AND .module.name NOT LIKE 'cal::%'
		   AND .module.name NOT LIKE 'ext::%'
		ORDER BY .module.name, .name;
	`

	type rawAlias struct {
		Module struct {
			Name string `edgedb:"name"`
		} `edgedb:"module"`
		Name   string `edgedb:"name"`
		Target struct {
			Name string `edgedb:"name"`
		} `edgedb:"target"`
	}

	var rawAliases []rawAlias
	err := client.Query(ctx, query, &rawAliases)
	if err != nil {
		return nil, fmt.Errorf("error querying aliases: %v", err)
	}

	var aliases []EdgeDBAliasInfo
	for _, ra := range rawAliases {
		a := EdgeDBAliasInfo{
			Module: ra.Module.Name,
			Name:   ra.Name,
			Type:   ra.Target.Name,
		}

		aliases = append(aliases, a)
	}

	return aliases, nil
}

func discoverConstraints(client *gel.Client) ([]EdgeDBConstraintInfo, error) {
	ctx := context.Background()

	query := `
		SELECT schema::Constraint {
			module: {
				name
			},
			name,
			params: {
				name,
				type: {
					name
				}
			},
			return_type: {
				name
			},
			description
		}
		FILTER .module.name NOT LIKE 'std::%'
		   AND .module.name NOT LIKE 'schema::%'
		   AND .module.name NOT LIKE 'sys::%'
		   AND .module.name NOT LIKE 'cfg::%'
		   AND .module.name NOT LIKE 'math::%'
		   AND .module.name NOT LIKE 'cal::%'
		   AND .module.name NOT LIKE 'ext::%'
		ORDER BY .module.name, .name;
	`

	type rawConstraint struct {
		Module struct {
			Name string `edgedb:"name"`
		} `edgedb:"module"`
		Name   string `edgedb:"name"`
		Params []struct {
			Name string `edgedb:"name"`
			Type struct {
				Name string `edgedb:"name"`
			} `edgedb:"type"`
		} `edgedb:"params"`
		ReturnType struct {
			Name string `edgedb:"name"`
		} `edgedb:"return_type"`
		Description string `edgedb:"description"`
	}

	var rawConstraints []rawConstraint
	err := client.Query(ctx, query, &rawConstraints)
	if err != nil {
		return nil, fmt.Errorf("error querying constraints: %v", err)
	}

	var constraints []EdgeDBConstraintInfo
	for _, rc := range rawConstraints {
		c := EdgeDBConstraintInfo{
			Module:      rc.Module.Name,
			Name:        rc.Name,
			ReturnType:  rc.ReturnType.Name,
			Description: rc.Description,
		}

		// Process param types
		for _, p := range rc.Params {
			c.ParamTypes = append(c.ParamTypes, p.Type.Name)
		}

		constraints = append(constraints, c)
	}

	return constraints, nil
}

func discoverExtensions(client *gel.Client) ([]common.ExtensionInfo, error) {
	ctx := context.Background()

	query := `
		SELECT sys::Extension {
			name,
			version
		}
		ORDER BY .name;
	`

	type rawExtension struct {
		Name    string `edgedb:"name"`
		Version string `edgedb:"version"`
	}

	var rawExtensions []rawExtension
	err := client.Query(ctx, query, &rawExtensions)
	if err != nil {
		return nil, fmt.Errorf("error querying extensions: %v", err)
	}

	var extensions []common.ExtensionInfo
	for _, re := range rawExtensions {
		e := common.ExtensionInfo{
			Name:    re.Name,
			Schema:  "ext",
			Version: re.Version,
		}

		extensions = append(extensions, e)
	}

	return extensions, nil
}
