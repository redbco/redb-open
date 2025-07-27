package models

// EdgeDBSchema represents the EdgeDB schema structure
type EdgeDBSchema struct {
	SchemaType  string             `json:"schemaType"`
	Modules     []EdgeDBModule     `json:"modules"`
	Types       []EdgeDBType       `json:"types"`
	Scalars     []EdgeDBScalar     `json:"scalars"`
	Aliases     []EdgeDBAlias      `json:"aliases"`
	Constraints []EdgeDBConstraint `json:"constraints"`
	Functions   []EdgeDBFunction   `json:"functions"`
	Extensions  []EdgeDBExtension  `json:"extensions"`
}

type EdgeDBModule struct {
	Name string `json:"name"`
}

type EdgeDBType struct {
	Module      string             `json:"module"`
	Name        string             `json:"name"`
	IsAbstract  bool               `json:"isAbstract"`
	Properties  []EdgeDBProperty   `json:"properties"`
	Links       []EdgeDBLink       `json:"links"`
	Bases       []string           `json:"bases"`
	Constraints []EdgeDBConstraint `json:"constraints"`
}

type EdgeDBProperty struct {
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	Required bool        `json:"required"`
	ReadOnly bool        `json:"readOnly"`
	Default  interface{} `json:"default,omitempty"`
}

type EdgeDBLink struct {
	Name           string `json:"name"`
	Target         string `json:"target"`
	Required       bool   `json:"required"`
	ReadOnly       bool   `json:"readOnly"`
	Cardinality    string `json:"cardinality"`
	OnTargetDelete string `json:"onTargetDelete,omitempty"`
}

type EdgeDBScalar struct {
	Module      string             `json:"module"`
	Name        string             `json:"name"`
	BaseType    string             `json:"baseType"`
	Constraints []EdgeDBConstraint `json:"constraints"`
}

type EdgeDBAlias struct {
	Module string `json:"module"`
	Name   string `json:"name"`
	Type   string `json:"type"`
}

type EdgeDBConstraint struct {
	Module      string      `json:"module"`
	Name        string      `json:"name"`
	ParamTypes  []string    `json:"paramTypes,omitempty"`
	ReturnType  string      `json:"returnType"`
	Description string      `json:"description,omitempty"`
	Args        interface{} `json:"args,omitempty"`
}

type EdgeDBFunction struct {
	Module     string            `json:"module"`
	Name       string            `json:"name"`
	Parameters []EdgeDBParameter `json:"parameters"`
	ReturnType string            `json:"returnType"`
	Body       string            `json:"body"`
}

type EdgeDBParameter struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type EdgeDBExtension struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}
