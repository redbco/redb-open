package adapters

import (
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type EdgeDBIngester struct{}

func (e *EdgeDBIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var edgeSchema models.EdgeDBSchema
	if err := json.Unmarshal(rawSchema, &edgeSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Convert modules to schemas
	for _, module := range edgeSchema.Modules {
		unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
			Name: module.Name,
		})
	}

	// Convert types to tables
	for _, edgeType := range edgeSchema.Types {
		if edgeType.IsAbstract {
			continue // Skip abstract types as they don't have a table representation
		}

		unifiedTable := models.Table{
			Name:      edgeType.Name,
			Schema:    edgeType.Module,
			TableType: "standard",
		}

		// Convert properties to columns
		for _, prop := range edgeType.Properties {
			unifiedCol := models.Column{
				Name:       prop.Name,
				IsNullable: !prop.Required,
				DataType: models.DataType{
					Name:         prop.Type,
					TypeCategory: "basic",
					BaseType:     prop.Type,
				},
			}

			if prop.Default != nil {
				defaultStr := fmt.Sprintf("%v", prop.Default)
				unifiedCol.DefaultValue = &defaultStr
			}

			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		// Convert links to foreign key constraints
		for _, link := range edgeType.Links {
			if link.Target == "" {
				warnings = append(warnings, fmt.Sprintf("Link '%s' in type '%s' has no target type", link.Name, edgeType.Name))
				continue
			}

			constraint := models.Constraint{
				Type:              "FOREIGN KEY",
				Name:              fmt.Sprintf("fk_%s_%s", edgeType.Name, link.Name),
				Table:             edgeType.Name,
				Columns:           []string{link.Name},
				ReferencedTable:   link.Target,
				ReferencedColumns: []string{"id"}, // EdgeDB typically uses 'id' as the primary key
				OnDelete:          link.OnTargetDelete,
			}

			unifiedTable.Constraints = append(unifiedTable.Constraints, constraint)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert scalars to domains
	for _, scalar := range edgeSchema.Scalars {
		unifiedDomain := models.Domain{
			Name:       scalar.Name,
			BaseType:   scalar.BaseType,
			Schema:     scalar.Module,
			IsNullable: true, // EdgeDB scalars are nullable by default
		}

		unifiedModel.Domains = append(unifiedModel.Domains, unifiedDomain)
	}

	// Convert aliases to domains
	for _, alias := range edgeSchema.Aliases {
		unifiedDomain := models.Domain{
			Name:       alias.Name,
			BaseType:   alias.Type,
			Schema:     alias.Module,
			IsNullable: true, // EdgeDB aliases are nullable by default
		}

		unifiedModel.Domains = append(unifiedModel.Domains, unifiedDomain)
	}

	// Convert functions
	for _, edgeFunc := range edgeSchema.Functions {
		unifiedFunc := models.Function{
			Name:       edgeFunc.Name,
			Schema:     edgeFunc.Module,
			Definition: edgeFunc.Body,
		}

		// Convert parameters
		for _, param := range edgeFunc.Parameters {
			unifiedFunc.Arguments = append(unifiedFunc.Arguments, models.FunctionParameter{
				Name:     param.Name,
				DataType: param.Type,
			})
		}

		unifiedFunc.ReturnType = edgeFunc.ReturnType
		unifiedModel.Functions = append(unifiedModel.Functions, unifiedFunc)
	}

	// Convert extensions
	for _, ext := range edgeSchema.Extensions {
		unifiedExt := models.Extension{
			Name:        ext.Name,
			Version:     ext.Version,
			Description: ext.Description,
		}

		unifiedModel.Extensions = append(unifiedModel.Extensions, unifiedExt)
	}

	return unifiedModel, warnings, nil
}

type EdgeDBExporter struct{}

func (e *EdgeDBExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	edgeSchema := models.EdgeDBSchema{
		SchemaType: "edgedb",
	}
	warnings := []string{}

	// Convert schemas to modules
	for _, schema := range model.Schemas {
		edgeSchema.Modules = append(edgeSchema.Modules, models.EdgeDBModule{
			Name: schema.Name,
		})
	}

	// Convert tables to types
	for _, table := range model.Tables {
		edgeType := models.EdgeDBType{
			Module:     table.Schema,
			Name:       table.Name,
			IsAbstract: false,
		}

		// Convert columns to properties
		for _, col := range table.Columns {
			prop := models.EdgeDBProperty{
				Name:     col.Name,
				Type:     col.DataType.BaseType,
				Required: !col.IsNullable,
			}

			if col.DefaultValue != nil {
				prop.Default = *col.DefaultValue
			}

			edgeType.Properties = append(edgeType.Properties, prop)
		}

		// Convert foreign key constraints to links
		for _, constraint := range table.Constraints {
			if constraint.Type == "FOREIGN KEY" && len(constraint.Columns) == 1 {
				link := models.EdgeDBLink{
					Name:           constraint.Columns[0],
					Target:         constraint.ReferencedTable,
					Required:       true, // Default to required for foreign key links
					OnTargetDelete: constraint.OnDelete,
				}

				edgeType.Links = append(edgeType.Links, link)
			}
		}

		edgeSchema.Types = append(edgeSchema.Types, edgeType)
	}

	// Convert domains to scalars
	for _, domain := range model.Domains {
		scalar := models.EdgeDBScalar{
			Module:   domain.Schema,
			Name:     domain.Name,
			BaseType: domain.BaseType,
		}

		edgeSchema.Scalars = append(edgeSchema.Scalars, scalar)
	}

	// Convert functions
	for _, function := range model.Functions {
		edgeFunc := models.EdgeDBFunction{
			Module:     function.Schema,
			Name:       function.Name,
			ReturnType: function.ReturnType,
			Body:       function.Definition,
		}

		// Convert parameters
		for _, arg := range function.Arguments {
			edgeFunc.Parameters = append(edgeFunc.Parameters, models.EdgeDBParameter{
				Name: arg.Name,
				Type: arg.DataType,
			})
		}

		edgeSchema.Functions = append(edgeSchema.Functions, edgeFunc)
	}

	// Convert extensions
	for _, ext := range model.Extensions {
		edgeExt := models.EdgeDBExtension{
			Name:        ext.Name,
			Version:     ext.Version,
			Description: ext.Description,
		}

		edgeSchema.Extensions = append(edgeSchema.Extensions, edgeExt)
	}

	return edgeSchema, warnings, nil
}
