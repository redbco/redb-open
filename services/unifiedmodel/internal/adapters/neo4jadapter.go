package adapters

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type Neo4jIngester struct{}

func (n *Neo4jIngester) IngestSchema(rawSchema json.RawMessage) (*models.UnifiedModel, []string, error) {
	var neo4jSchema models.Neo4jSchema
	if err := json.Unmarshal(rawSchema, &neo4jSchema); err != nil {
		return nil, nil, err
	}

	unifiedModel := &models.UnifiedModel{
		SchemaType: "unified",
	}
	warnings := []string{}

	// Neo4j doesn't have schemas in the traditional sense, so we'll create a default schema
	unifiedModel.Schemas = append(unifiedModel.Schemas, models.Schema{
		Name: "default",
	})

	// Convert labels to tables
	for _, label := range neo4jSchema.Labels {
		unifiedTable := models.Table{
			Name:      label.Name,
			Schema:    "default",
			TableType: "standard",
		}

		// Convert properties to columns
		for _, prop := range label.Properties {
			unifiedCol := models.Column{
				Name:       prop.Name,
				IsNullable: prop.Nullable,
				DataType: models.DataType{
					Name:         prop.DataType,
					TypeCategory: "basic",
					BaseType:     prop.DataType,
				},
			}

			unifiedTable.Columns = append(unifiedTable.Columns, unifiedCol)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, unifiedTable)
	}

	// Convert relationship types to tables
	for _, relType := range neo4jSchema.RelationshipTypes {
		// Create a table for the relationship type
		relTable := models.Table{
			Name:      relType.Name,
			Schema:    "default",
			TableType: "standard",
		}

		// Add columns for the relationship properties
		for _, prop := range relType.Properties {
			relCol := models.Column{
				Name:       prop.Name,
				IsNullable: prop.Nullable,
				DataType: models.DataType{
					Name:         prop.DataType,
					TypeCategory: "basic",
					BaseType:     prop.DataType,
				},
			}

			relTable.Columns = append(relTable.Columns, relCol)
		}

		// Add source and target columns for the relationship
		sourceCol := models.Column{
			Name:       "source",
			IsNullable: false,
			DataType: models.DataType{
				Name:         "string",
				TypeCategory: "basic",
				BaseType:     "string",
			},
		}
		relTable.Columns = append(relTable.Columns, sourceCol)

		targetCol := models.Column{
			Name:       "target",
			IsNullable: false,
			DataType: models.DataType{
				Name:         "string",
				TypeCategory: "basic",
				BaseType:     "string",
			},
		}
		relTable.Columns = append(relTable.Columns, targetCol)

		// Add foreign key constraints if start and end labels are specified
		if relType.StartLabel != "" {
			sourceConstraint := models.Constraint{
				Type:              "FOREIGN KEY",
				Name:              fmt.Sprintf("fk_%s_source", relType.Name),
				Table:             relType.Name,
				Columns:           []string{"source"},
				ReferencedTable:   relType.StartLabel,
				ReferencedColumns: []string{"id"},
			}
			relTable.Constraints = append(relTable.Constraints, sourceConstraint)
		}

		if relType.EndLabel != "" {
			targetConstraint := models.Constraint{
				Type:              "FOREIGN KEY",
				Name:              fmt.Sprintf("fk_%s_target", relType.Name),
				Table:             relType.Name,
				Columns:           []string{"target"},
				ReferencedTable:   relType.EndLabel,
				ReferencedColumns: []string{"id"},
			}
			relTable.Constraints = append(relTable.Constraints, targetConstraint)
		}

		unifiedModel.Tables = append(unifiedModel.Tables, relTable)
	}

	// Convert constraints
	for _, constraint := range neo4jSchema.Constraints {
		// Only handle node property constraints for now
		if !constraint.IsRelationship {
			// Find the table for this label
			var tableName string
			for _, table := range unifiedModel.Tables {
				if table.Name == constraint.LabelOrType {
					tableName = table.Name
					break
				}
			}

			if tableName == "" {
				warnings = append(warnings, fmt.Sprintf("Constraint '%s' references unknown label '%s'", constraint.Name, constraint.LabelOrType))
				continue
			}

			// Create a constraint in the unified model
			unifiedConstraint := models.Constraint{
				Name:    constraint.Name,
				Table:   tableName,
				Columns: constraint.PropertyKeys,
			}

			// Map Neo4j constraint types to unified model constraint types
			switch constraint.Type {
			case "UNIQUENESS":
				unifiedConstraint.Type = "UNIQUE"
			case "NODE_PROPERTY_EXISTENCE":
				unifiedConstraint.Type = "NOT NULL"
			default:
				warnings = append(warnings, fmt.Sprintf("Unsupported constraint type '%s' for constraint '%s'", constraint.Type, constraint.Name))
				continue
			}

			// Find the table and add the constraint
			for i, table := range unifiedModel.Tables {
				if table.Name == tableName {
					unifiedModel.Tables[i].Constraints = append(unifiedModel.Tables[i].Constraints, unifiedConstraint)
					break
				}
			}
		}
	}

	// Convert indexes
	for _, index := range neo4jSchema.Indexes {
		// Skip indexes with empty names as they are likely system or internal indexes
		if index.Name == "" {
			warnings = append(warnings, "Skipping index with empty name (likely system index)")
			continue
		}

		// Find the table for this index
		var tableName string
		if len(index.LabelsOrTypes) > 0 {
			tableName = index.LabelsOrTypes[0]
		}

		// For Neo4j, some indexes might not have associated labels (like lookup indexes)
		// In such cases, we'll create a generic table or skip the index
		if tableName == "" {
			// Check if this is a lookup or system index that doesn't need a specific label
			if index.Type == "LOOKUP" || strings.Contains(strings.ToLower(index.Name), "lookup") {
				// Create a generic lookup table for these indexes
				tableName = "_lookup_indexes"
			} else {
				// For other indexes without labels, add a warning but still include them
				// with a generic table name to allow comparison
				warnings = append(warnings, fmt.Sprintf("Index '%s' has no associated label, using generic table", index.Name))
				tableName = "_generic_indexes"
			}
		}

		// Create an index in the unified model
		unifiedIndex := models.Index{
			Name:        index.Name,
			Schema:      "default",
			Table:       tableName,
			IsUnique:    index.Uniqueness == "UNIQUE",
			IndexMethod: index.Type,
		}

		// Add columns to the index
		for _, prop := range index.Properties {
			unifiedIndex.Columns = append(unifiedIndex.Columns, models.IndexColumn{
				ColumnName: prop,
			})
		}

		unifiedModel.Indexes = append(unifiedModel.Indexes, unifiedIndex)
	}

	// Convert procedures to functions
	for _, proc := range neo4jSchema.Procedures {
		unifiedFunc := models.Function{
			Name:       proc.Name,
			Schema:     "default",
			Definition: proc.Signature,
		}

		// Parse the signature to extract parameters and return type
		// This is a simplified approach and might need refinement
		parts := strings.Split(proc.Signature, "->")
		if len(parts) == 2 {
			unifiedFunc.ReturnType = strings.TrimSpace(parts[1])

			// Extract parameters from the first part
			paramPart := strings.TrimSpace(parts[0])
			if strings.HasPrefix(paramPart, "(") && strings.HasSuffix(paramPart, ")") {
				paramPart = paramPart[1 : len(paramPart)-1]
				if paramPart != "" {
					params := strings.Split(paramPart, ",")
					for _, param := range params {
						param = strings.TrimSpace(param)
						paramParts := strings.Split(param, ":")
						if len(paramParts) == 2 {
							unifiedFunc.Arguments = append(unifiedFunc.Arguments, models.FunctionParameter{
								Name:     strings.TrimSpace(paramParts[0]),
								DataType: strings.TrimSpace(paramParts[1]),
							})
						}
					}
				}
			}
		}

		unifiedModel.Functions = append(unifiedModel.Functions, unifiedFunc)
	}

	// Convert functions
	for _, function := range neo4jSchema.Functions {
		unifiedFunc := models.Function{
			Name:       function.Name,
			Schema:     "default",
			Definition: function.Signature,
		}

		// Parse the signature to extract parameters and return type
		// This is a simplified approach and might need refinement
		parts := strings.Split(function.Signature, "->")
		if len(parts) == 2 {
			unifiedFunc.ReturnType = strings.TrimSpace(parts[1])

			// Extract parameters from the first part
			paramPart := strings.TrimSpace(parts[0])
			if strings.HasPrefix(paramPart, "(") && strings.HasSuffix(paramPart, ")") {
				paramPart = paramPart[1 : len(paramPart)-1]
				if paramPart != "" {
					params := strings.Split(paramPart, ",")
					for _, param := range params {
						param = strings.TrimSpace(param)
						paramParts := strings.Split(param, ":")
						if len(paramParts) == 2 {
							unifiedFunc.Arguments = append(unifiedFunc.Arguments, models.FunctionParameter{
								Name:     strings.TrimSpace(paramParts[0]),
								DataType: strings.TrimSpace(paramParts[1]),
							})
						}
					}
				}
			}
		}

		unifiedModel.Functions = append(unifiedModel.Functions, unifiedFunc)
	}

	return unifiedModel, warnings, nil
}

type Neo4jExporter struct{}

func (n *Neo4jExporter) ExportSchema(model *models.UnifiedModel) (interface{}, []string, error) {
	neo4jSchema := models.Neo4jSchema{
		SchemaType: "neo4j",
	}
	warnings := []string{}

	// Convert tables to labels and relationship types
	for _, table := range model.Tables {
		// Check if this is a relationship table (has source and target columns)
		isRelationship := false
		var sourceCol, targetCol *models.Column

		for i, col := range table.Columns {
			if col.Name == "source" {
				sourceCol = &table.Columns[i]
			}
			if col.Name == "target" {
				targetCol = &table.Columns[i]
			}
		}

		if sourceCol != nil && targetCol != nil {
			isRelationship = true
		}

		if isRelationship {
			// This is a relationship type
			relType := models.Neo4jRelationshipType{
				Name: table.Name,
			}

			// Add properties (excluding source and target)
			for _, col := range table.Columns {
				if col.Name != "source" && col.Name != "target" {
					prop := models.Neo4jProperty{
						Name:     col.Name,
						DataType: col.DataType.BaseType,
						Nullable: col.IsNullable,
					}
					relType.Properties = append(relType.Properties, prop)
				}
			}

			// Extract start and end labels from foreign key constraints
			for _, constraint := range table.Constraints {
				if constraint.Type == "FOREIGN KEY" && constraint.Columns[0] == "source" {
					relType.StartLabel = constraint.ReferencedTable
				}
				if constraint.Type == "FOREIGN KEY" && constraint.Columns[0] == "target" {
					relType.EndLabel = constraint.ReferencedTable
				}
			}

			neo4jSchema.RelationshipTypes = append(neo4jSchema.RelationshipTypes, relType)
		} else {
			// This is a node label
			label := models.Neo4jLabel{
				Name: table.Name,
			}

			// Add properties
			for _, col := range table.Columns {
				prop := models.Neo4jProperty{
					Name:     col.Name,
					DataType: col.DataType.BaseType,
					Nullable: col.IsNullable,
				}
				label.Properties = append(label.Properties, prop)
			}

			neo4jSchema.Labels = append(neo4jSchema.Labels, label)
		}
	}

	// Convert constraints
	for _, table := range model.Tables {
		for _, constraint := range table.Constraints {
			// Map unified model constraint types to Neo4j constraint types
			var constraintType string
			switch constraint.Type {
			case "UNIQUE":
				constraintType = "UNIQUENESS"
			case "NOT NULL":
				constraintType = "NODE_PROPERTY_EXISTENCE"
			default:
				warnings = append(warnings, fmt.Sprintf("Unsupported constraint type '%s' for constraint '%s'", constraint.Type, constraint.Name))
				continue
			}

			neo4jConstraint := models.Neo4jConstraint{
				Name:           constraint.Name,
				Type:           constraintType,
				LabelOrType:    table.Name,
				PropertyKeys:   constraint.Columns,
				IsRelationship: false,
			}

			neo4jSchema.Constraints = append(neo4jSchema.Constraints, neo4jConstraint)
		}
	}

	// Convert indexes
	for _, index := range model.Indexes {
		neo4jIndex := models.Neo4jIndex{
			Name:          index.Name,
			Type:          index.IndexMethod,
			LabelsOrTypes: []string{index.Table},
			Uniqueness:    "NONUNIQUE",
			State:         "ONLINE",
		}

		if index.IsUnique {
			neo4jIndex.Uniqueness = "UNIQUE"
		}

		// Add properties
		for _, col := range index.Columns {
			neo4jIndex.Properties = append(neo4jIndex.Properties, col.ColumnName)
		}

		neo4jSchema.Indexes = append(neo4jSchema.Indexes, neo4jIndex)
	}

	// Convert functions
	for _, function := range model.Functions {
		// Check if this is a procedure or a function based on the name
		// This is a simplified approach and might need refinement
		if strings.Contains(function.Name, ".") {
			// This is likely a procedure
			proc := models.Neo4jProcedure{
				Name:        function.Name,
				Signature:   buildSignature(function),
				Description: function.Definition,
				Mode:        "READ", // Default to READ mode
			}
			neo4jSchema.Procedures = append(neo4jSchema.Procedures, proc)
		} else {
			// This is likely a function
			neoFunc := models.Neo4jFunction{
				Name:        function.Name,
				Signature:   buildSignature(function),
				Description: function.Definition,
				Category:    "default",
			}
			neo4jSchema.Functions = append(neo4jSchema.Functions, neoFunc)
		}
	}

	return neo4jSchema, warnings, nil
}

// Helper function to build a Neo4j signature from a unified function
func buildSignature(function models.Function) string {
	var sb strings.Builder

	// Build parameter list
	sb.WriteString("(")
	for i, arg := range function.Arguments {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(arg.Name)
		sb.WriteString(": ")
		sb.WriteString(arg.DataType)
	}
	sb.WriteString(") -> ")

	// Add return type
	sb.WriteString(function.ReturnType)

	return sb.String()
}
