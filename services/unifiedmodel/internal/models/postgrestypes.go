package models

// SchemaInfo represents the PostgreSQL schema structure
type PostgresSchema struct {
	SchemaType     string            `json:"schemaType"`
	Schemas        []PGSchema        `json:"schemas"`
	Tables         []PGTable         `json:"tables"`
	EnumTypes      []PGEnum          `json:"enumTypes"`
	CompositeTypes []PGCompositeType `json:"compositeTypes"`
	Domains        []PGDomain        `json:"domains"`
	Ranges         []PGRange         `json:"ranges"`
	Indexes        []PGIndex         `json:"indexes"`
}

type PGSchema struct {
	Name string `json:"name"`
}

type PGTable struct {
	Schema      string         `json:"schema"`
	Name        string         `json:"name"`
	TableType   string         `json:"tableType"`
	Columns     []PGColumn     `json:"columns"`
	Constraints []PGConstraint `json:"constraints,omitempty"`
}

type PGIndex struct {
	Name           string          `json:"name"`
	Schema         string          `json:"schema"`
	Table          string          `json:"table"`
	Columns        []PGIndexColumn `json:"columns"`
	IncludeColumns []string        `json:"includeColumns,omitempty"`
	IsUnique       bool            `json:"isUnique"`
	WhereClause    string          `json:"whereClause,omitempty"`
	Concurrently   bool            `json:"concurrently"`
	FillFactor     int             `json:"fillFactor,omitempty"`
	Tablespace     string          `json:"tablespace,omitempty"`
	Collation      string          `json:"collation,omitempty"`
	Comment        string          `json:"comment,omitempty"`
	Owner          string          `json:"owner,omitempty"`
}

type PGIndexColumn struct {
	Name         string `json:"name"`
	Order        int    `json:"order"`
	Expression   string `json:"expression,omitempty"`
	NullPosition int    `json:"nullPosition,omitempty"`
}

type PGEnum struct {
	Name         string   `json:"name"`
	Values       []string `json:"values"`
	Schema       *string  `json:"schema,omitempty"`
	IsExtensible *bool    `json:"isExtensible,omitempty"`
	Collation    string   `json:"collation,omitempty"`
	Comment      string   `json:"comment,omitempty"`
	Owner        string   `json:"owner,omitempty"`
}

type PGCompositeType struct {
	Name    string    `json:"name"`
	Fields  []PGField `json:"fields"`
	Schema  string    `json:"schema,omitempty"`
	Comment string    `json:"comment,omitempty"`
	Owner   string    `json:"owner,omitempty"`
}

type PGField struct {
	Name          string  `json:"name"`
	Type          string  `json:"type"`
	Collation     string  `json:"collation,omitempty"`
	IsNullable    bool    `json:"isNullable"`
	ColumnDefault *string `json:"columnDefault,omitempty"`
}

type PGDomain struct {
	Name            string  `json:"name"`
	BaseType        string  `json:"baseType"`
	Schema          string  `json:"schema,omitempty"`
	Collation       string  `json:"collation,omitempty"`
	IsNullable      bool    `json:"isNullable"`
	ColumnDefault   *string `json:"columnDefault,omitempty"`
	CheckConstraint *string `json:"checkConstraint,omitempty"`
	Comment         string  `json:"comment,omitempty"`
	Owner           string  `json:"owner,omitempty"`
}

type PGRange struct {
	Name                string `json:"name"`
	Schema              string `json:"schema,omitempty"`
	Subtype             string `json:"subtype"`
	CanonicalFunction   string `json:"canonicalFunction"`
	SubtypeDiffFunction string `json:"subtypeDiffFunction"`
	MultirangeType      string `json:"multirangeType"`
	Comment             string `json:"comment,omitempty"`
	Owner               string `json:"owner,omitempty"`
}

type PGColumn struct {
	Name             string         `json:"name"`
	Type             string         `json:"dataType"`
	IsArray          bool           `json:"isArray"`
	IsNullable       bool           `json:"isNullable"`
	IsPrimaryKey     bool           `json:"isPrimaryKey"`
	IsUnique         bool           `json:"isUnique,omitempty"`
	IsGenerated      bool           `json:"isGenerated"`
	IsAutoIncrement  bool           `json:"isAutoIncrement"`
	VarcharLength    int            `json:"varcharLength,omitempty"`
	ColumnDefault    *string        `json:"columnDefault,omitempty"`
	CustomTypeName   string         `json:"customTypeName,omitempty"`
	ArrayElementType string         `json:"arrayElementType,omitempty"`
	Constraints      []PGConstraint `json:"constraints,omitempty"`
	Collation        string         `json:"collation,omitempty"`
}

type PGConstraint struct {
	Type              string   `json:"type"`
	Name              string   `json:"name"`
	Table             string   `json:"table"`
	Columns           []string `json:"columns"`
	Deferrable        bool     `json:"deferrable,omitempty"`
	InitiallyDeferred string   `json:"initiallyDeferred,omitempty"`
	Enabled           bool     `json:"enabled,omitempty"`
	Validated         bool     `json:"validated,omitempty"`
	CheckExpression   string   `json:"checkExpression,omitempty"`
	ReferencedTable   string   `json:"referencedTable,omitempty"`
	ReferencedColumns []string `json:"referencedColumns,omitempty"`
	OnUpdate          string   `json:"onUpdate,omitempty"`
	OnDelete          string   `json:"onDelete,omitempty"`
	UsingIndex        string   `json:"usingIndex,omitempty"`
}
