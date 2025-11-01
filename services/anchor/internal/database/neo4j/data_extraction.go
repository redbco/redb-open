package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// NodeInstance represents a node instance with its data
type NodeInstance struct {
	InternalID string                 // Neo4j internal ID
	Label      string                 // Primary label
	Labels     []string               // All labels (for multi-label nodes)
	Properties map[string]interface{} // Property values
}

// RelationshipInstance represents a relationship instance with connected nodes
type RelationshipInstance struct {
	InternalID     string                 // Neo4j internal relationship ID
	Type           string                 // Relationship type
	StartNodeID    string                 // Start node internal ID
	StartNodeLabel string                 // Start node primary label
	EndNodeID      string                 // End node internal ID
	EndNodeLabel   string                 // End node primary label
	Properties     map[string]interface{} // Relationship properties
	Direction      string                 // "outgoing" or "incoming"
}

// GraphDataExport contains all extracted data from Neo4j
type GraphDataExport struct {
	Nodes         map[string][]NodeInstance         // Keyed by label
	Relationships map[string][]RelationshipInstance // Keyed by type
	IDMapping     map[string]int64                  // Neo4j ID -> PostgreSQL ID mapping
}

// FetchAllNodesWithData extracts all nodes of a given label with their property values
func FetchAllNodesWithData(driver neo4j.DriverWithContext, label string, limit int) ([]NodeInstance, error) {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// Query to fetch nodes with all their properties
	query := fmt.Sprintf("MATCH (n:`%s`) RETURN elementId(n) as id, labels(n) as labels, properties(n) as props", label)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("error fetching nodes for label %s: %v", label, err)
	}

	var nodes []NodeInstance
	for result.Next(ctx) {
		record := result.Record()

		id, _ := record.Get("id")
		labels, _ := record.Get("labels")
		props, _ := record.Get("props")

		node := NodeInstance{
			InternalID: fmt.Sprintf("%v", id),
			Label:      label,
			Labels:     convertToStringSlice(labels),
			Properties: convertToMap(props),
		}

		nodes = append(nodes, node)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating nodes for label %s: %v", label, err)
	}

	return nodes, nil
}

// FetchAllRelationshipsWithNodes extracts all relationships of a given type with start/end node info
func FetchAllRelationshipsWithNodes(driver neo4j.DriverWithContext, relType string, limit int) ([]RelationshipInstance, error) {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// Query to fetch relationships with connected node information
	query := fmt.Sprintf(`
		MATCH (start)-[r:%s]->(end)
		RETURN 
			elementId(r) as rel_id,
			type(r) as rel_type,
			elementId(start) as start_id,
			labels(start)[0] as start_label,
			elementId(end) as end_id,
			labels(end)[0] as end_label,
			properties(r) as props
	`, relType)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("error fetching relationships for type %s: %v", relType, err)
	}

	var relationships []RelationshipInstance
	for result.Next(ctx) {
		record := result.Record()

		relID, _ := record.Get("rel_id")
		relType, _ := record.Get("rel_type")
		startID, _ := record.Get("start_id")
		startLabel, _ := record.Get("start_label")
		endID, _ := record.Get("end_id")
		endLabel, _ := record.Get("end_label")
		props, _ := record.Get("props")

		rel := RelationshipInstance{
			InternalID:     fmt.Sprintf("%v", relID),
			Type:           fmt.Sprintf("%v", relType),
			StartNodeID:    fmt.Sprintf("%v", startID),
			StartNodeLabel: fmt.Sprintf("%v", startLabel),
			EndNodeID:      fmt.Sprintf("%v", endID),
			EndNodeLabel:   fmt.Sprintf("%v", endLabel),
			Properties:     convertToMap(props),
			Direction:      "outgoing",
		}

		relationships = append(relationships, rel)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating relationships for type %s: %v", relType, err)
	}

	return relationships, nil
}

// ExtractAllGraphData extracts all nodes and relationships from the graph
func ExtractAllGraphData(driver neo4j.DriverWithContext, um *unifiedmodel.UnifiedModel) (*GraphDataExport, error) {
	export := &GraphDataExport{
		Nodes:         make(map[string][]NodeInstance),
		Relationships: make(map[string][]RelationshipInstance),
		IDMapping:     make(map[string]int64),
	}

	// Extract all nodes by label
	for label := range um.Nodes {
		nodes, err := FetchAllNodesWithData(driver, label, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch nodes for label %s: %w", label, err)
		}
		export.Nodes[label] = nodes
	}

	// Extract all relationships by type
	for relType := range um.Relationships {
		rels, err := FetchAllRelationshipsWithNodes(driver, relType, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch relationships for type %s: %w", relType, err)
		}
		export.Relationships[relType] = rels
	}

	return export, nil
}

// InferPropertyType samples property values to determine the actual type
func InferPropertyType(driver neo4j.DriverWithContext, label string, propertyName string, sampleSize int) string {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// Sample nodes to analyze property types
	query := fmt.Sprintf(
		"MATCH (n:`%s`) WHERE n.`%s` IS NOT NULL RETURN n.`%s` as value LIMIT %d",
		label, propertyName, propertyName, sampleSize,
	)

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return "mixed"
	}

	var types = make(map[string]int)
	var totalSamples int

	for result.Next(ctx) {
		record := result.Record()
		value, ok := record.Get("value")
		if !ok {
			continue
		}

		totalSamples++
		detectedType := detectValueType(value)
		types[detectedType]++
	}

	// If all samples are the same type, return that type
	if totalSamples > 0 {
		for typeName, count := range types {
			// If 90% or more samples are same type, consider it that type
			if float64(count)/float64(totalSamples) >= 0.9 {
				return typeName
			}
		}
	}

	return "mixed"
}

// InferAllPropertyTypes infers types for all properties of a label
func InferAllPropertyTypes(driver neo4j.DriverWithContext, label string, propertyNames []string) map[string]string {
	types := make(map[string]string)

	for _, propName := range propertyNames {
		types[propName] = InferPropertyType(driver, label, propName, 100)
	}

	return types
}

// detectValueType determines the Go type of a value
func detectValueType(value interface{}) string {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64:
		return "float"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []interface{}:
		if len(v) > 0 {
			return "array"
		}
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		// Try to detect date types
		str := fmt.Sprintf("%v", value)
		if strings.Contains(str, "-") && (len(str) == 10 || len(str) >= 19) {
			return "date"
		}
		return "mixed"
	}
}

// Helper functions

func convertToStringSlice(value interface{}) []string {
	if value == nil {
		return []string{}
	}

	switch v := value.(type) {
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			result[i] = fmt.Sprintf("%v", item)
		}
		return result
	case []string:
		return v
	default:
		return []string{fmt.Sprintf("%v", value)}
	}
}

func convertToMap(value interface{}) map[string]interface{} {
	if value == nil {
		return make(map[string]interface{})
	}

	switch v := value.(type) {
	case map[string]interface{}:
		return v
	default:
		return make(map[string]interface{})
	}
}

// BuildIDMapping creates a mapping from Neo4j internal IDs to sequential PostgreSQL IDs
func BuildIDMapping(export *GraphDataExport) {
	nextID := int64(1)

	// Map node IDs
	for _, nodeList := range export.Nodes {
		for _, node := range nodeList {
			if _, exists := export.IDMapping[node.InternalID]; !exists {
				export.IDMapping[node.InternalID] = nextID
				nextID++
			}
		}
	}
}

// GetMappedID retrieves the PostgreSQL ID for a Neo4j internal ID
func (e *GraphDataExport) GetMappedID(neo4jID string) (int64, bool) {
	id, ok := e.IDMapping[neo4jID]
	return id, ok
}
