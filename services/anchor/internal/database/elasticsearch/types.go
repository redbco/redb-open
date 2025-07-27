package elasticsearch

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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

// ElasticsearchDetails contains information about an Elasticsearch cluster
type ElasticsearchDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	ClusterName      string `json:"clusterName"`
	ClusterHealth    string `json:"clusterHealth"`
	NumberOfNodes    int    `json:"numberOfNodes"`
}

// ElasticsearchSchema represents the schema of an Elasticsearch cluster
type ElasticsearchSchema struct {
	Indices    []common.TableInfo `json:"indices"` // Using TableInfo to represent indices
	Templates  []TemplateInfo     `json:"templates"`
	Pipelines  []PipelineInfo     `json:"pipelines"`
	Aliases    []AliasInfo        `json:"aliases"`
	Components []ComponentInfo    `json:"components"`
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
