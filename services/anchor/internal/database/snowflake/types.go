package snowflake

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// SnowflakeDetails contains information about a Snowflake database
type SnowflakeDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	Account          string `json:"account"`
	Region           string `json:"region"`
	Role             string `json:"role"`
	Warehouse        string `json:"warehouse"`
}

// SnowflakeSchema represents the schema of a Snowflake database
type SnowflakeSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Procedures []common.ProcedureInfo      `json:"procedures"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Views      []common.ViewInfo           `json:"views"`
	Stages     []SnowflakeStageInfo        `json:"stages"`
	Warehouses []SnowflakeWarehouseInfo    `json:"warehouses"`
	Pipes      []SnowflakePipeInfo         `json:"pipes"`
}

// SnowflakeStageInfo represents a Snowflake stage
type SnowflakeStageInfo struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	Database    string `json:"database"`
	URL         string `json:"url,omitempty"`
	StageType   string `json:"stageType"` // INTERNAL, EXTERNAL, etc.
	Credentials string `json:"credentials,omitempty"`
}

// SnowflakeWarehouseInfo represents a Snowflake warehouse
type SnowflakeWarehouseInfo struct {
	Name            string `json:"name"`
	Size            string `json:"size"` // X-Small, Small, Medium, Large, etc.
	MinClusterCount int    `json:"minClusterCount"`
	MaxClusterCount int    `json:"maxClusterCount"`
	AutoSuspend     int    `json:"autoSuspend"` // seconds
	AutoResume      bool   `json:"autoResume"`
	State           string `json:"state"` // STARTED, SUSPENDED, etc.
}

// SnowflakePipeInfo represents a Snowflake pipe
type SnowflakePipeInfo struct {
	Name                string `json:"name"`
	Schema              string `json:"schema"`
	Database            string `json:"database"`
	Definition          string `json:"definition"`
	PipeType            string `json:"pipeType"` // STANDARD, SNOWPIPE_STREAMING
	Owner               string `json:"owner"`
	NotificationChannel string `json:"notificationChannel,omitempty"`
}

// SnowflakeStreamChange represents a change captured by Snowflake streams
type SnowflakeStreamChange struct {
	Operation string
	Data      map[string]interface{}
	Metadata  map[string]interface{}
}

// SnowflakeStreamInfo represents a Snowflake stream
type SnowflakeStreamInfo struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Database  string `json:"database"`
	TableName string `json:"tableName"`
	Mode      string `json:"mode"` // APPEND_ONLY, INSERT_ONLY, etc.
	Owner     string `json:"owner"`
}
