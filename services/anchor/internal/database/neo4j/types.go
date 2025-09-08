package neo4j

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
