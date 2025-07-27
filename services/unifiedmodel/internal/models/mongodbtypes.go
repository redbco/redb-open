package models

// MongoDBSchema represents the MongoDB schema structure
type MongoDBSchema struct {
	SchemaType  string            `json:"schemaType"`
	Collections []MongoCollection `json:"collections"`
}

type MongoCollection struct {
	Name      string       `json:"name"`
	Fields    []MongoField `json:"fields"`
	TableType string       `json:"tableType"`
}

type MongoField struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	Required     bool        `json:"required"`
	IsPrimaryKey bool        `json:"isPrimaryKey,omitempty"`
	Default      interface{} `json:"default,omitempty"`
}

// MongoIndex represents a MongoDB index
type MongoIndex struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique,omitempty"`
}
