package models

// ClickhouseSchema represents the schema of a Clickhouse database
type ClickhouseSchema struct {
	SchemaType string            `json:"schemaType"`
	Tables     []ClickhouseTable `json:"tables"`
	Databases  []Database        `json:"databases"`
	Functions  []Function        `json:"functions"`
	Views      []View            `json:"views"`
	Engines    []Engine          `json:"engines"`
	Settings   []Setting         `json:"settings"`
}

// ClickhouseTable represents a Clickhouse table
type ClickhouseTable struct {
	Name         string             `json:"name"`
	Database     string             `json:"database"`
	Columns      []ClickhouseColumn `json:"columns"`
	Engine       string             `json:"engine"`
	EngineParams map[string]string  `json:"engineParams"`
	OrderBy      []string           `json:"orderBy"`
	PartitionBy  string             `json:"partitionBy"`
	SampleBy     string             `json:"sampleBy"`
	TTL          string             `json:"ttl"`
	Settings     map[string]string  `json:"settings"`
	Comment      string             `json:"comment"`
}

// ClickhouseColumn represents a column in a Clickhouse table
type ClickhouseColumn struct {
	Name             string  `json:"name"`
	DataType         string  `json:"dataType"`
	DefaultValue     *string `json:"defaultValue"`
	DefaultExpr      *string `json:"defaultExpr"`
	IsNullable       bool    `json:"isNullable"`
	IsPrimaryKey     bool    `json:"isPrimaryKey"`
	IsUnique         bool    `json:"isUnique"`
	IsCompressed     bool    `json:"isCompressed"`
	CompressionCodec string  `json:"compressionCodec"`
	Comment          string  `json:"comment"`
}

// Database represents a Clickhouse database
type Database struct {
	Name         string            `json:"name"`
	Engine       string            `json:"engine"`
	EngineParams map[string]string `json:"engineParams"`
	Comment      string            `json:"comment"`
}

// View represents a Clickhouse view
type View struct {
	Name           string            `json:"name"`
	Database       string            `json:"database"`
	Query          string            `json:"query"`
	Engine         string            `json:"engine"`
	EngineParams   map[string]string `json:"engineParams"`
	IsMaterialized bool              `json:"isMaterialized"`
	Comment        string            `json:"comment"`
}

// Engine represents a Clickhouse table engine
type Engine struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Parameters  map[string]string `json:"parameters"`
	IsSupported bool              `json:"isSupported"`
}

// Setting represents a Clickhouse setting
type Setting struct {
	Name        string `json:"name"`
	Value       string `json:"value"`
	Description string `json:"description"`
	IsReadOnly  bool   `json:"isReadOnly"`
}
