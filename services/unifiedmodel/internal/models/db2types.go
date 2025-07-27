package models

// Db2Schema represents the Db2 database schema
type Db2Schema struct {
	SchemaType string          `json:"schemaType"`
	Schemas    []Db2UserSchema `json:"schemas"`
	Tables     []Db2Table      `json:"tables"`
	Sequences  []Db2Sequence   `json:"sequences"`
	Functions  []Db2Function   `json:"functions"`
	Triggers   []Db2Trigger    `json:"triggers"`
	Procedures []Db2Procedure  `json:"procedures"`
}

// Db2UserSchema represents a Db2 schema
type Db2UserSchema struct {
	Name string `json:"name"`
}

// Db2Table represents a Db2 table
type Db2Table struct {
	Name        string          `json:"name"`
	Schema      string          `json:"schema"`
	TableType   string          `json:"tableType"`
	Columns     []Db2Column     `json:"columns"`
	Constraints []Db2Constraint `json:"constraints"`
	Indexes     []Db2Index      `json:"indexes"`
}

// Db2Column represents a Db2 table column
type Db2Column struct {
	Name            string  `json:"name"`
	DataType        string  `json:"dataType"`
	DataLength      *int    `json:"dataLength,omitempty"`
	DataPrecision   *string `json:"dataPrecision,omitempty"`
	DataScale       *string `json:"dataScale,omitempty"`
	IsNullable      bool    `json:"isNullable"`
	IsPrimaryKey    bool    `json:"isPrimaryKey"`
	IsUnique        bool    `json:"isUnique"`
	IsAutoIncrement bool    `json:"isAutoIncrement"`
	IsArray         bool    `json:"isArray"`
	ColumnDefault   *string `json:"columnDefault,omitempty"`
}

// Db2Constraint represents a Db2 table constraint
type Db2Constraint struct {
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
	UsingIndex        string   `json:"usingIndex,omitempty"`
}

// Db2Index represents a Db2 index
type Db2Index struct {
	Name        string           `json:"name"`
	Schema      string           `json:"schema"`
	Table       string           `json:"table"`
	Columns     []Db2IndexColumn `json:"columns"`
	IsUnique    bool             `json:"isUnique"`
	IndexType   string           `json:"indexType"`
	WhereClause string           `json:"whereClause,omitempty"`
}

// Db2IndexColumn represents a column in a Db2 index
type Db2IndexColumn struct {
	ColumnName string `json:"columnName"`
	Order      int    `json:"order"` // 1 for ASC, -1 for DESC
}

// Db2Sequence represents a Db2 sequence
type Db2Sequence struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	DataType    string `json:"dataType"`
	StartValue  int64  `json:"startValue"`
	IncrementBy int64  `json:"incrementBy"`
	MaxValue    int64  `json:"maxValue"`
	MinValue    int64  `json:"minValue"`
	CacheSize   int64  `json:"cacheSize"`
	CycleFlag   string `json:"cycleFlag"` // "Y" or "N"
}

// Db2Function represents a Db2 function
type Db2Function struct {
	Name            string `json:"name"`
	Schema          string `json:"schema"`
	ReturnType      string `json:"returnType"`
	IsDeterministic string `json:"isDeterministic"` // "Y" or "N"
	Definition      string `json:"definition"`
}

// Db2Trigger represents a Db2 trigger
type Db2Trigger struct {
	Name          string `json:"name"`
	Schema        string `json:"schema"`
	TableName     string `json:"tableName"`
	TriggerEvent  string `json:"triggerEvent"`
	Definition    string `json:"definition"`
	TriggerTiming string `json:"triggerTiming"`
}

// Db2Procedure represents a Db2 stored procedure
type Db2Procedure struct {
	Name       string `json:"name"`
	Schema     string `json:"schema"`
	Definition string `json:"definition"`
}
