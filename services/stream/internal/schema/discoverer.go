package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// Discoverer handles schema discovery from streaming messages
type Discoverer struct {
	mu           sync.RWMutex
	db           *database.PostgreSQL
	nodeID       string
	schemas      map[string]*TopicSchema // topic -> schema
	sampleCounts map[string]int64        // topic -> sample count
	logger       interface{}
}

// TopicSchema represents a discovered schema for a topic
type TopicSchema struct {
	TopicName       string
	Platform        string
	StreamID        string
	Fields          map[string]*FieldInfo
	MessagesSampled int64
	LastUpdated     time.Time
	Confidence      float64
}

// FieldInfo represents information about a field
type FieldInfo struct {
	Name           string
	DataType       string
	IsNullable     bool
	OccurrenceRate float64 // How often the field appears (0.0 - 1.0)
	SampleValues   []interface{}
}

// NewDiscoverer creates a new schema discoverer
func NewDiscoverer(db *database.PostgreSQL, nodeID string) *Discoverer {
	return &Discoverer{
		db:           db,
		nodeID:       nodeID,
		schemas:      make(map[string]*TopicSchema),
		sampleCounts: make(map[string]int64),
	}
}

// DiscoverSchema analyzes a message and updates the schema
func (d *Discoverer) DiscoverSchema(ctx context.Context, topic, platform, streamID string, message []byte) error {
	// Parse JSON message
	var data map[string]interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		// Not JSON, try other formats later
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	key := fmt.Sprintf("%s:%s", streamID, topic)

	schema, exists := d.schemas[key]
	if !exists {
		schema = &TopicSchema{
			TopicName:       topic,
			Platform:        platform,
			StreamID:        streamID,
			Fields:          make(map[string]*FieldInfo),
			MessagesSampled: 0,
			LastUpdated:     time.Now(),
		}
		d.schemas[key] = schema
	}

	// Update schema with fields from this message
	for fieldName, value := range data {
		field, exists := schema.Fields[fieldName]
		if !exists {
			field = &FieldInfo{
				Name:         fieldName,
				SampleValues: make([]interface{}, 0, 10),
			}
			schema.Fields[fieldName] = field
		}

		// Infer data type
		dataType := inferDataType(value)
		if field.DataType == "" {
			field.DataType = dataType
		}

		// Track if field can be null
		if value == nil {
			field.IsNullable = true
		}

		// Store sample value
		if len(field.SampleValues) < 10 {
			field.SampleValues = append(field.SampleValues, value)
		}
	}

	schema.MessagesSampled++
	schema.LastUpdated = time.Now()

	// Update occurrence rates
	for _, field := range schema.Fields {
		occurrences := int64(0)
		for fn := range data {
			if fn == field.Name {
				occurrences++
			}
		}
		field.OccurrenceRate = float64(occurrences) / float64(schema.MessagesSampled)

		// If occurrence rate < 1.0, field is nullable
		if field.OccurrenceRate < 1.0 {
			field.IsNullable = true
		}
	}

	// Calculate confidence score
	schema.Confidence = calculateConfidence(schema)

	// Periodically update resource registry
	if schema.MessagesSampled%100 == 0 {
		go d.updateResourceRegistry(context.Background(), schema)
	}

	return nil
}

func inferDataType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch value.(type) {
	case bool:
		return "boolean"
	case float64, int, int64, float32:
		return "number"
	case string:
		return "string"
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	default:
		return "unknown"
	}
}

func calculateConfidence(schema *TopicSchema) float64 {
	if schema.MessagesSampled < 10 {
		return 0.1
	} else if schema.MessagesSampled < 100 {
		return 0.5
	} else if schema.MessagesSampled < 1000 {
		return 0.8
	}
	return 0.95
}

func (d *Discoverer) updateResourceRegistry(ctx context.Context, schema *TopicSchema) error {
	// Create a UnifiedModel representation for this stream topic
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:    dbcapabilities.DatabaseType(schema.Platform),
		SearchDocuments: make(map[string]unifiedmodel.SearchDocument),
	}

	// Convert topic schema to SearchDocument (primary container for stream messages)
	fields := make(map[string]unifiedmodel.Field)
	for fieldName, fieldInfo := range schema.Fields {
		fields[fieldName] = unifiedmodel.Field{
			Name:     fieldInfo.Name,
			Type:     fieldInfo.DataType,
			Required: !fieldInfo.IsNullable,
		}
	}

	searchDoc := unifiedmodel.SearchDocument{
		Name:       schema.TopicName,
		DocumentID: schema.TopicName,
		Index:      schema.TopicName,
		Fields:     fields,
		Type:       schema.Platform, // e.g., "kafka", "pubsub", "kinesis"
		Analyzer:   "standard",
	}

	um.SearchDocuments[schema.TopicName] = searchDoc

	// Generate containers and items from unified model
	containers, items, err := unifiedmodel.PopulateResourcesFromUnifiedModel(
		um,
		schema.StreamID,  // dbID
		d.nodeID,         // nodeID
		"",               // tenantID - would need to be passed in
		"",               // workspaceID - would need to be passed in
		"",               // ownerID
		schema.TopicName, // databaseName
		nil,              // enrichedAnalysis
	)
	if err != nil {
		return fmt.Errorf("failed to populate resources: %w", err)
	}

	// TODO: Insert containers and items into resource_containers and resource_items tables
	// This would require:
	// 1. d.db.InsertResourceContainers(ctx, containers)
	// 2. d.db.InsertResourceItems(ctx, items)
	// For now, we log the counts
	_ = containers
	_ = items

	return nil
}

// GetSchema returns the current schema for a topic
func (d *Discoverer) GetSchema(streamID, topic string) (*TopicSchema, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", streamID, topic)
	schema, exists := d.schemas[key]
	return schema, exists
}
