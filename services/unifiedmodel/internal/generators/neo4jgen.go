package generators

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/models"
)

type Neo4jGenerator struct {
	BaseGenerator
}

func (n *Neo4jGenerator) GenerateCreateStatements(schema interface{}) ([]string, error) {
	neo4jSchema, ok := schema.(models.Neo4jSchema)
	if !ok {
		return nil, fmt.Errorf("expected Neo4jSchema, got %T", schema)
	}

	var statements []string

	// Create node constraints
	for _, constraint := range neo4jSchema.Constraints {
		if !constraint.IsRelationship {
			switch constraint.Type {
			case "UNIQUENESS":
				// Create uniqueness constraint
				stmt := fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE n.%s IS UNIQUE",
					constraint.Name,
					constraint.LabelOrType,
					strings.Join(constraint.PropertyKeys, ", n."))
				statements = append(statements, stmt)
			case "NODE_PROPERTY_EXISTENCE":
				// Create existence constraint
				stmt := fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE n.%s IS NOT NULL",
					constraint.Name,
					constraint.LabelOrType,
					strings.Join(constraint.PropertyKeys, " IS NOT NULL AND n."))
				statements = append(statements, stmt)
			}
		} else {
			// Handle relationship constraints
			switch constraint.Type {
			case "RELATIONSHIP_PROPERTY_EXISTENCE":
				// Create relationship property existence constraint
				stmt := fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE r.%s IS NOT NULL",
					constraint.Name,
					constraint.LabelOrType,
					strings.Join(constraint.PropertyKeys, " IS NOT NULL AND r."))
				statements = append(statements, stmt)
			case "RELATIONSHIP_UNIQUENESS":
				// Create relationship uniqueness constraint
				stmt := fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE r.%s IS UNIQUE",
					constraint.Name,
					constraint.LabelOrType,
					strings.Join(constraint.PropertyKeys, ", r."))
				statements = append(statements, stmt)
			}
		}
	}

	// Create indexes
	for _, index := range neo4jSchema.Indexes {
		// Determine if this is a relationship index
		isRelationshipIndex := false
		for _, relType := range neo4jSchema.RelationshipTypes {
			if relType.Name == index.LabelsOrTypes[0] {
				isRelationshipIndex = true
				break
			}
		}

		if isRelationshipIndex {
			// Create relationship index
			if index.Uniqueness == "UNIQUE" {
				stmt := fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR ()-[r:%s]-() ON (r.%s)",
					index.Name,
					strings.Join(index.LabelsOrTypes, ":"),
					strings.Join(index.Properties, ", r."))
				statements = append(statements, stmt)
			} else {
				stmt := fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR ()-[r:%s]-() ON (r.%s)",
					index.Name,
					strings.Join(index.LabelsOrTypes, ":"),
					strings.Join(index.Properties, ", r."))
				statements = append(statements, stmt)
			}
		} else {
			// Create node index
			if index.Uniqueness == "UNIQUE" {
				stmt := fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR (n:%s) ON (n.%s)",
					index.Name,
					strings.Join(index.LabelsOrTypes, ":"),
					strings.Join(index.Properties, ", n."))
				statements = append(statements, stmt)
			} else {
				stmt := fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR (n:%s) ON (n.%s)",
					index.Name,
					strings.Join(index.LabelsOrTypes, ":"),
					strings.Join(index.Properties, ", n."))
				statements = append(statements, stmt)
			}
		}
	}

	// Create fulltext indexes if specified
	for _, index := range neo4jSchema.Indexes {
		if index.Type == "FULLTEXT" {
			// Create fulltext index
			stmt := fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [n.%s]",
				index.Name,
				strings.Join(index.LabelsOrTypes, ":"),
				strings.Join(index.Properties, ", n."))
			statements = append(statements, stmt)
		}
	}

	// Create lookup indexes if specified
	for _, index := range neo4jSchema.Indexes {
		if index.Type == "LOOKUP" {
			// Create lookup index
			stmt := fmt.Sprintf("CREATE LOOKUP INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [n.%s]",
				index.Name,
				strings.Join(index.LabelsOrTypes, ":"),
				strings.Join(index.Properties, ", n."))
			statements = append(statements, stmt)
		}
	}

	// Create text indexes if specified
	for _, index := range neo4jSchema.Indexes {
		if index.Type == "TEXT" {
			// Create text index
			stmt := fmt.Sprintf("CREATE TEXT INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [n.%s]",
				index.Name,
				strings.Join(index.LabelsOrTypes, ":"),
				strings.Join(index.Properties, ", n."))
			statements = append(statements, stmt)
		}
	}

	return statements, nil
}
