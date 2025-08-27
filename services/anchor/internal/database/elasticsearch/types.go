package elasticsearch

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
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

// CreateElasticsearchUnifiedModel creates a UnifiedModel for Elasticsearch with database details
func CreateElasticsearchUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Elasticsearch,
		SearchIndexes: make(map[string]unifiedmodel.SearchIndex),
		Pipelines:     make(map[string]unifiedmodel.Pipeline),
	}
	return um
}

// ConvertElasticsearchIndex converts common.TableInfo to unifiedmodel.SearchIndex for Elasticsearch
func ConvertElasticsearchIndex(indexInfo common.TableInfo) unifiedmodel.SearchIndex {
	searchIndex := unifiedmodel.SearchIndex{
		Name:   indexInfo.Name,
		Fields: make([]string, 0, len(indexInfo.Columns)),
	}

	// Convert columns to field names (Elasticsearch uses flexible schema)
	for _, col := range indexInfo.Columns {
		searchIndex.Fields = append(searchIndex.Fields, col.Name)
	}

	return searchIndex
}

// ConvertElasticsearchPipeline converts PipelineInfo to unifiedmodel.Pipeline for Elasticsearch
func ConvertElasticsearchPipeline(pipelineInfo PipelineInfo) unifiedmodel.Pipeline {
	// Convert processor names to steps
	steps := make([]string, 0, len(pipelineInfo.Processors))
	for _, processor := range pipelineInfo.Processors {
		// Extract processor type names from the processor map
		for processorType := range processor {
			steps = append(steps, processorType)
		}
	}

	return unifiedmodel.Pipeline{
		Name:  pipelineInfo.Name,
		Steps: steps,
	}
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
