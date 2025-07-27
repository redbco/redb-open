package models

// CockroachSchema represents the schema of a CockroachDB database
type CockroachSchema struct {
	SchemaType string           `json:"schemaType"`
	Tables     []CockroachTable `json:"tables"`
	EnumTypes  []Enum           `json:"enumTypes"`
	Schemas    []Schema         `json:"schemas"`
	Functions  []Function       `json:"functions"`
	Triggers   []Trigger        `json:"triggers"`
	Sequences  []Sequence       `json:"sequences"`
	Extensions []Extension      `json:"extensions"`
}

// CockroachTable represents a CockroachDB table
type CockroachTable struct {
	Name        string            `json:"name"`
	Schema      string            `json:"schema"`
	Columns     []CockroachColumn `json:"columns"`
	Constraints []Constraint      `json:"constraints"`
	Indexes     []Index           `json:"indexes"`
	TableType   string            `json:"tableType"`
	Owner       string            `json:"owner"`
	Comment     string            `json:"comment"`
}

// CockroachColumn represents a column in a CockroachDB table
type CockroachColumn struct {
	Name                 string  `json:"name"`
	DataType             string  `json:"dataType"`
	IsNullable           bool    `json:"isNullable"`
	IsPrimaryKey         bool    `json:"isPrimaryKey"`
	IsUnique             bool    `json:"isUnique"`
	IsAutoIncrement      bool    `json:"isAutoIncrement"`
	IsGenerated          bool    `json:"isGenerated"`
	DefaultIsFunction    bool    `json:"defaultIsFunction"`
	DefaultValueFunction string  `json:"defaultValueFunction"`
	DefaultValue         *string `json:"defaultValue"`
	Collation            string  `json:"collation"`
	Comment              string  `json:"comment"`
}

// CockroachReplicationSourceDetails contains information about a CockroachDB replication source
type CockroachReplicationSourceDetails struct {
	ChangefeedID   string `json:"changefeed_id"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
	SinkURI        string `json:"sink_uri"`
	WebhookURL     string `json:"webhook_url,omitempty"`
	ResolvedOption string `json:"resolved_option,omitempty"`
}

// CockroachReplicationChange represents a change in CockroachDB replication
type CockroachReplicationChange struct {
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"oldData"`
}
