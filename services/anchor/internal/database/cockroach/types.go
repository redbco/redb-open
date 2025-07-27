package cockroach

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// CockroachDetails contains information about a CockroachDB database
type CockroachDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	ClusterID        string `json:"clusterId"`
	NodeID           string `json:"nodeId"`
}

// CockroachSchema represents the schema of a CockroachDB database
type CockroachSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	EnumTypes  []common.EnumInfo           `json:"enumTypes"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Extensions []common.ExtensionInfo      `json:"extensions"`
}

type CockroachReplicationSourceDetails struct {
	ChangefeedID   string `json:"changefeed_id"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
	SinkURI        string `json:"sink_uri"`
	WebhookURL     string `json:"webhook_url,omitempty"`
	ResolvedOption string `json:"resolved_option,omitempty"`
}

type CockroachReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
