package models

// MSSQLSchema represents the schema of a Microsoft SQL Server database
type MSSQLSchema struct {
	SchemaType string          `json:"schemaType"`
	Name       string          `json:"name"`
	Tables     []MSSQLTable    `json:"tables"`
	Schemas    []MSSQLSchema   `json:"schemas"`
	Functions  []MSSQLFunction `json:"functions"`
	Triggers   []MSSQLTrigger  `json:"triggers"`
	Sequences  []MSSQLSequence `json:"sequences"`
}

// MSSQLTable represents a table in MS-SQL
type MSSQLTable struct {
	Name        string            `json:"name"`
	Schema      string            `json:"schema"`
	TableType   string            `json:"tableType"`
	Columns     []MSSQLColumn     `json:"columns"`
	Constraints []MSSQLConstraint `json:"constraints"`
}

// MSSQLColumn represents a column in MS-SQL
type MSSQLColumn struct {
	Name            string  `json:"name"`
	IsNullable      bool    `json:"isNullable"`
	IsPrimaryKey    bool    `json:"isPrimaryKey"`
	IsUnique        bool    `json:"isUnique"`
	IsAutoIncrement bool    `json:"isAutoIncrement"`
	ColumnDefault   *string `json:"columnDefault,omitempty"`
	DataType        string  `json:"dataType"`
}

// MSSQLConstraint represents a constraint in MS-SQL
type MSSQLConstraint struct {
	Type              string   `json:"type"`
	Name              string   `json:"name"`
	Table             string   `json:"table"`
	Columns           []string `json:"columns"`
	Deferrable        bool     `json:"deferrable"`
	InitiallyDeferred string   `json:"initiallyDeferred"`
	Enabled           bool     `json:"enabled"`
	Validated         bool     `json:"validated"`
	CheckExpression   string   `json:"checkExpression,omitempty"`
	ReferencedTable   string   `json:"referencedTable,omitempty"`
	ReferencedColumns []string `json:"referencedColumns,omitempty"`
	OnUpdate          string   `json:"onUpdate,omitempty"`
	OnDelete          string   `json:"onDelete,omitempty"`
}

// MSSQLFunction represents a function in MS-SQL
type MSSQLFunction struct {
	Name            string                   `json:"name"`
	Schema          string                   `json:"schema"`
	Arguments       []MSSQLFunctionParameter `json:"arguments"`
	ReturnType      string                   `json:"returnType"`
	IsDeterministic bool                     `json:"isDeterministic"`
	Definition      string                   `json:"definition"`
}

// MSSQLFunctionParameter represents a function parameter in MS-SQL
type MSSQLFunctionParameter struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// MSSQLTrigger represents a trigger in MS-SQL
type MSSQLTrigger struct {
	Name       string `json:"name"`
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Event      string `json:"event"`
	Definition string `json:"definition"`
	Timing     string `json:"timing"`
}

// MSSQLSequence represents a sequence in MS-SQL
type MSSQLSequence struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	DataType  string `json:"dataType"`
	Start     int64  `json:"start"`
	Increment int64  `json:"increment"`
	MaxValue  int64  `json:"maxValue"`
	MinValue  int64  `json:"minValue"`
	CacheSize int64  `json:"cacheSize"`
	Cycle     bool   `json:"cycle"`
}
