package elasticsearch

import (
	"github.com/elastic/go-elasticsearch/v8"
)

// ElasticsearchClient wraps the Elasticsearch client with additional metadata
type ElasticsearchClient struct {
	Client      *elasticsearch.Client
	IsConnected int32
}

// Close closes the Elasticsearch client connection
func (c *ElasticsearchClient) Close() {
	// Elasticsearch client doesn't have an explicit close method
	// Just mark as disconnected
	c.IsConnected = 0
}

// TemplateInfo represents an Elasticsearch index template
type TemplateInfo struct {
	Name          string                 `json:"name"`
	IndexPatterns []string               `json:"indexPatterns"`
	Priority      int                    `json:"priority"`
	Version       int                    `json:"version,omitempty"`
	Settings      map[string]interface{} `json:"settings,omitempty"`
	Mappings      map[string]interface{} `json:"mappings,omitempty"`
}

// PipelineInfo represents an Elasticsearch ingest pipeline
type PipelineInfo struct {
	Name        string                   `json:"name"`
	Description string                   `json:"description,omitempty"`
	Processors  []map[string]interface{} `json:"processors"`
}

// AliasInfo represents an Elasticsearch alias
type AliasInfo struct {
	Name         string                 `json:"name"`
	Indices      []string               `json:"indices"`
	IsWriteIndex bool                   `json:"isWriteIndex,omitempty"`
	Filter       map[string]interface{} `json:"filter,omitempty"`
}

// ComponentInfo represents an Elasticsearch component template
type ComponentInfo struct {
	Name     string                 `json:"name"`
	Version  int                    `json:"version,omitempty"`
	Settings map[string]interface{} `json:"settings,omitempty"`
	Mappings map[string]interface{} `json:"mappings,omitempty"`
}

// ElasticsearchReplicationSourceDetails contains information about replication
type ElasticsearchReplicationSourceDetails struct {
	WatchID    string `json:"watch_id"`
	IndexName  string `json:"index_name"`
	DatabaseID string `json:"database_id"`
}

// ElasticsearchReplicationChange represents a change in Elasticsearch data
type ElasticsearchReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}
