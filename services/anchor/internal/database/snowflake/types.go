package snowflake

import (
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// ConvertSnowflakeStage converts SnowflakeStageInfo to unifiedmodel.ExternalTable (stages are similar to external tables)
func ConvertSnowflakeStage(stageInfo SnowflakeStageInfo) unifiedmodel.ExternalTable {
	return unifiedmodel.ExternalTable{
		Name:     stageInfo.Name,
		Location: stageInfo.URL,
		Format:   stageInfo.StageType,
	}
}

// ConvertSnowflakePipe converts SnowflakePipeInfo to unifiedmodel.Function (pipes are data loading functions)
func ConvertSnowflakePipe(pipeInfo SnowflakePipeInfo) unifiedmodel.Function {
	return unifiedmodel.Function{
		Name:       pipeInfo.Name,
		Language:   "sql", // Snowflake uses SQL
		Definition: pipeInfo.Definition,
	}
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
