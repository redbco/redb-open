package neo4j

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// CreateNeo4jUnifiedModel creates a UnifiedModel for Neo4j with database details
func CreateNeo4jUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Neo4j,
		Graphs:        make(map[string]unifiedmodel.Graph),
		Nodes:         make(map[string]unifiedmodel.Node),
		Relationships: make(map[string]unifiedmodel.Relationship),
		Indexes:       make(map[string]unifiedmodel.Index),
		Constraints:   make(map[string]unifiedmodel.Constraint),
		Procedures:    make(map[string]unifiedmodel.Procedure),
		Functions:     make(map[string]unifiedmodel.Function),
	}
	return um
}

// ConvertNeo4jLabelToNode converts LabelInfo to unifiedmodel.Node
func ConvertNeo4jLabelToNode(labelInfo LabelInfo) unifiedmodel.Node {
	node := unifiedmodel.Node{
		Label:      labelInfo.Name,
		Properties: make(map[string]unifiedmodel.Property),
		Indexes:    make(map[string]unifiedmodel.Index),
	}

	// Convert properties
	for _, prop := range labelInfo.Properties {
		node.Properties[prop.Name] = unifiedmodel.Property{
			Name: prop.Name,
			Type: prop.DataType,
		}
	}

	return node
}

// ConvertNeo4jRelationshipType converts RelationshipTypeInfo to unifiedmodel.Relationship
func ConvertNeo4jRelationshipType(relTypeInfo RelationshipTypeInfo) unifiedmodel.Relationship {
	relationship := unifiedmodel.Relationship{
		Type:       relTypeInfo.Name,
		FromLabel:  relTypeInfo.StartLabel,
		ToLabel:    relTypeInfo.EndLabel,
		Properties: make(map[string]unifiedmodel.Property),
	}

	// Convert properties
	for _, prop := range relTypeInfo.Properties {
		relationship.Properties[prop.Name] = unifiedmodel.Property{
			Name: prop.Name,
			Type: prop.DataType,
		}
	}

	return relationship
}

// ConvertNeo4jConstraint converts ConstraintInfo to unifiedmodel.Constraint
func ConvertNeo4jConstraint(constraintInfo ConstraintInfo) unifiedmodel.Constraint {
	constraintType := unifiedmodel.ConstraintTypeUnique
	switch constraintInfo.Type {
	case "UNIQUENESS":
		constraintType = unifiedmodel.ConstraintTypeUnique
	case "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE":
		constraintType = unifiedmodel.ConstraintTypeNotNull
	}

	return unifiedmodel.Constraint{
		Name:    constraintInfo.Name,
		Type:    constraintType,
		Columns: constraintInfo.PropertyKeys,
	}
}

// LabelInfo represents a node label in Neo4j
type LabelInfo struct {
	Name       string         `json:"name"`
	Properties []PropertyInfo `json:"properties"`
	Count      int64          `json:"count"`
}

// RelationshipTypeInfo represents a relationship type in Neo4j
type RelationshipTypeInfo struct {
	Name       string         `json:"name"`
	Properties []PropertyInfo `json:"properties"`
	Count      int64          `json:"count"`
	StartLabel string         `json:"startLabel,omitempty"`
	EndLabel   string         `json:"endLabel,omitempty"`
}

// PropertyInfo represents a property in Neo4j
type PropertyInfo struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
	Nullable bool   `json:"nullable"`
}

// ConstraintInfo represents a constraint in Neo4j
type ConstraintInfo struct {
	Name           string   `json:"name"`
	Type           string   `json:"type"` // "UNIQUENESS", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE"
	LabelOrType    string   `json:"labelOrType"`
	PropertyKeys   []string `json:"propertyKeys"`
	IsRelationship bool     `json:"isRelationship"`
}

// Neo4jReplicationSourceDetails contains information about a Neo4j replication source
type Neo4jReplicationSourceDetails struct {
	SourceName  string `json:"source_name"`
	DatabaseID  string `json:"database_id"`
	LabelOrType string `json:"label_or_type"`
}

// Neo4jReplicationChange represents a change in Neo4j replication
type Neo4jReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
