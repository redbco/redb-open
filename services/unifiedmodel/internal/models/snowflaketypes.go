package models

// SnowflakeSchema represents the schema of a Snowflake database
type SnowflakeSchema struct {
	SchemaType string               `json:"schemaType"`
	Tables     []Table              `json:"tables"`
	Schemas    []Schema             `json:"schemas"`
	Functions  []Function           `json:"functions"`
	Procedures []Procedure          `json:"procedures"`
	Sequences  []Sequence           `json:"sequences"`
	Views      []SnowflakeView      `json:"views"`
	Stages     []SnowflakeStage     `json:"stages"`
	Warehouses []SnowflakeWarehouse `json:"warehouses"`
	Pipes      []SnowflakePipe      `json:"pipes"`
	Streams    []SnowflakeStream    `json:"streams"`
}

// SnowflakeStage represents a Snowflake stage
type SnowflakeStage struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	Database    string `json:"database"`
	URL         string `json:"url,omitempty"`
	StageType   string `json:"stageType"` // INTERNAL, EXTERNAL, etc.
	Credentials string `json:"credentials,omitempty"`
}

// SnowflakeWarehouse represents a Snowflake warehouse
type SnowflakeWarehouse struct {
	Name            string `json:"name"`
	Size            string `json:"size"` // X-Small, Small, Medium, Large, etc.
	MinClusterCount int    `json:"minClusterCount"`
	MaxClusterCount int    `json:"maxClusterCount"`
	AutoSuspend     int    `json:"autoSuspend"` // seconds
	AutoResume      bool   `json:"autoResume"`
	State           string `json:"state"` // STARTED, SUSPENDED, etc.
}

// SnowflakePipe represents a Snowflake pipe
type SnowflakePipe struct {
	Name                string `json:"name"`
	Schema              string `json:"schema"`
	Database            string `json:"database"`
	Definition          string `json:"definition"`
	PipeType            string `json:"pipeType"` // STANDARD, SNOWPIPE_STREAMING
	Owner               string `json:"owner"`
	NotificationChannel string `json:"notificationChannel,omitempty"`
}

// SnowflakeStream represents a Snowflake stream
type SnowflakeStream struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Database  string `json:"database"`
	TableName string `json:"tableName"`
	Mode      string `json:"mode"` // APPEND_ONLY, INSERT_ONLY, etc.
	Owner     string `json:"owner"`
}

// Procedure represents a stored procedure in Snowflake
type Procedure struct {
	Name            string              `json:"name"`
	Schema          string              `json:"schema"`
	Database        string              `json:"database"`
	Arguments       []FunctionParameter `json:"arguments"`
	ReturnType      string              `json:"returnType"`
	Language        string              `json:"language"`
	Definition      string              `json:"definition"`
	IsDeterministic bool                `json:"isDeterministic"`
	Owner           string              `json:"owner"`
}

// SnowflakeView represents a view in Snowflake
type SnowflakeView struct {
	Name           string `json:"name"`
	Schema         string `json:"schema"`
	Database       string `json:"database"`
	Definition     string `json:"definition"`
	IsMaterialized bool   `json:"isMaterialized"`
	Owner          string `json:"owner"`
	Comment        string `json:"comment"`
}
