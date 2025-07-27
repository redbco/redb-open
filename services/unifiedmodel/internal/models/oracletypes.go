package models

// OracleSchema represents the Oracle database schema
type OracleSchema struct {
	SchemaType string             `json:"schemaType"`
	Schemas    []OracleUserSchema `json:"schemas"`
	Tables     []OracleTable      `json:"tables"`
	Sequences  []OracleSequence   `json:"sequences"`
	Functions  []OracleFunction   `json:"functions"`
	Triggers   []OracleTrigger    `json:"triggers"`
}

// OracleUserSchema represents an Oracle user/schema
type OracleUserSchema struct {
	Name string `json:"name"`
}

// OracleTable represents an Oracle table
type OracleTable struct {
	Name        string             `json:"name"`
	Schema      string             `json:"schema"`
	Columns     []OracleColumn     `json:"columns"`
	Constraints []OracleConstraint `json:"constraints"`
}

// OracleColumn represents an Oracle table column
type OracleColumn struct {
	Name          string  `json:"name"`
	DataType      string  `json:"dataType"`
	DataLength    *int    `json:"dataLength,omitempty"`
	DataPrecision *string `json:"dataPrecision,omitempty"`
	DataScale     *string `json:"dataScale,omitempty"`
	IsNullable    bool    `json:"isNullable"`
	IsPrimaryKey  bool    `json:"isPrimaryKey"`
	IsUnique      bool    `json:"isUnique"`
	ColumnDefault *string `json:"columnDefault,omitempty"`
}

// OracleConstraint represents an Oracle table constraint
type OracleConstraint struct {
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

// OracleSequence represents an Oracle sequence
type OracleSequence struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	StartValue  int64  `json:"startValue"`
	IncrementBy int64  `json:"incrementBy"`
	MaxValue    int64  `json:"maxValue"`
	MinValue    int64  `json:"minValue"`
	CacheSize   int64  `json:"cacheSize"`
	CycleFlag   string `json:"cycleFlag"` // "Y" or "N"
}

// OracleFunction represents an Oracle function
type OracleFunction struct {
	Name            string `json:"name"`
	Schema          string `json:"schema"`
	ReturnType      string `json:"returnType"`
	IsDeterministic string `json:"isDeterministic"` // "Y" or "N"
	Definition      string `json:"definition"`
}

// OracleTrigger represents an Oracle trigger
type OracleTrigger struct {
	Name          string `json:"name"`
	Schema        string `json:"schema"`
	TableName     string `json:"tableName"`
	TriggerEvent  string `json:"triggerEvent"`
	Definition    string `json:"definition"`
	TriggerTiming string `json:"triggerTiming"`
}
