package cockroach

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
