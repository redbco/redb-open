package models

// Neo4jSchema represents the Neo4j schema structure
type Neo4jSchema struct {
	SchemaType        string                  `json:"schemaType"`
	Labels            []Neo4jLabel            `json:"labels"`
	RelationshipTypes []Neo4jRelationshipType `json:"relationshipTypes"`
	Constraints       []Neo4jConstraint       `json:"constraints"`
	Indexes           []Neo4jIndex            `json:"indexes"`
	Procedures        []Neo4jProcedure        `json:"procedures"`
	Functions         []Neo4jFunction         `json:"functions"`
}

// Neo4jLabel represents a node label in Neo4j
type Neo4jLabel struct {
	Name       string          `json:"name"`
	Properties []Neo4jProperty `json:"properties"`
	Count      int64           `json:"count"`
}

// Neo4jRelationshipType represents a relationship type in Neo4j
type Neo4jRelationshipType struct {
	Name       string          `json:"name"`
	Properties []Neo4jProperty `json:"properties"`
	Count      int64           `json:"count"`
	StartLabel string          `json:"startLabel,omitempty"`
	EndLabel   string          `json:"endLabel,omitempty"`
}

// Neo4jProperty represents a property in Neo4j
type Neo4jProperty struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
	Nullable bool   `json:"nullable"`
}

// Neo4jConstraint represents a constraint in Neo4j
type Neo4jConstraint struct {
	Name           string   `json:"name"`
	Type           string   `json:"type"` // "UNIQUENESS", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE"
	LabelOrType    string   `json:"labelOrType"`
	PropertyKeys   []string `json:"propertyKeys"`
	IsRelationship bool     `json:"isRelationship"`
}

// Neo4jIndex represents an index in Neo4j
type Neo4jIndex struct {
	Name               string   `json:"name"`
	Type               string   `json:"type"` // "BTREE", "FULLTEXT", "LOOKUP", "TEXT"
	LabelsOrTypes      []string `json:"labelsOrTypes"`
	Properties         []string `json:"properties"`
	Uniqueness         string   `json:"uniqueness"` // "UNIQUE", "NONUNIQUE"
	IndexProvider      string   `json:"indexProvider"`
	IndexConfig        string   `json:"indexConfig"`
	Ownership          string   `json:"ownership"`
	FailureMessage     string   `json:"failureMessage,omitempty"`
	PopulationProgress float64  `json:"populationProgress"`
	State              string   `json:"state"` // "ONLINE", "POPULATING", "FAILED"
}

// Neo4jProcedure represents a stored procedure in Neo4j
type Neo4jProcedure struct {
	Name          string `json:"name"`
	Signature     string `json:"signature"`
	Description   string `json:"description"`
	Mode          string `json:"mode"` // "READ", "WRITE", "SCHEMA", "DBMS"
	WorksOnSystem bool   `json:"worksOnSystem"`
}

// Neo4jFunction represents a function in Neo4j
type Neo4jFunction struct {
	Name        string `json:"name"`
	Signature   string `json:"signature"`
	Description string `json:"description"`
	Category    string `json:"category"`
}
