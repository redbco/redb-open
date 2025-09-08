package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// FetchData retrieves data from a specified label or relationship type
func FetchData(driver neo4j.DriverWithContext, labelOrType string, isRelationship bool, limit int) ([]map[string]interface{}, error) {
	if labelOrType == "" {
		return nil, fmt.Errorf("label or relationship type cannot be empty")
	}

	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	var query string
	if isRelationship {
		query = fmt.Sprintf("MATCH ()-[r:`%s`]->() RETURN r", labelOrType)
	} else {
		query = fmt.Sprintf("MATCH (n:`%s`) RETURN n", labelOrType)
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("error querying %s: %v", labelOrType, err)
	}

	var data []map[string]interface{}
	for result.Next(ctx) {
		record := result.Record()
		var node interface{}

		if isRelationship {
			node, _ = record.Get("r")
		} else {
			node, _ = record.Get("n")
		}

		// Convert Neo4j node/relationship to map
		nodeMap := nodeToMap(node)
		data = append(data, nodeMap)
	}

	return data, nil
}

// InsertData inserts data into a specified label or creates relationships
func InsertData(driver neo4j.DriverWithContext, labelOrType string, isRelationship bool, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	var totalRowsAffected int64

	// Start a transaction
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Close(ctx)

	for _, item := range data {
		var result neo4j.ResultWithContext

		if isRelationship {
			// For relationships, we need source and target nodes
			sourceId, sourceOk := item["_sourceId"]
			targetId, targetOk := item["_targetId"]
			sourceLabel, sourceLabelOk := item["_sourceLabel"]
			targetLabel, targetLabelOk := item["_targetLabel"]

			if !sourceOk || !targetOk {
				return totalRowsAffected, fmt.Errorf("relationship data must include _sourceId and _targetId")
			}

			// Remove special fields from properties
			properties := make(map[string]interface{})
			for k, v := range item {
				if !strings.HasPrefix(k, "_") {
					properties[k] = v
				}
			}

			var query string
			if sourceLabelOk && targetLabelOk {
				query = fmt.Sprintf(
					"MATCH (a:`%s`), (b:`%s`) WHERE id(a) = $sourceId AND id(b) = $targetId "+
						"CREATE (a)-[r:`%s` $props]->(b) RETURN r",
					sourceLabel, targetLabel, labelOrType)
			} else {
				query = fmt.Sprintf(
					"MATCH (a), (b) WHERE id(a) = $sourceId AND id(b) = $targetId "+
						"CREATE (a)-[r:`%s` $props]->(b) RETURN r",
					labelOrType)
			}

			params := map[string]interface{}{
				"sourceId": sourceId,
				"targetId": targetId,
				"props":    properties,
			}

			result, err = tx.Run(ctx, query, params)
		} else {
			// For nodes, create with label and properties
			query := fmt.Sprintf("CREATE (n:`%s` $props) RETURN n", labelOrType)
			params := map[string]interface{}{
				"props": item,
			}

			result, err = tx.Run(ctx, query, params)
		}

		if err != nil {
			tx.Rollback(ctx)
			return totalRowsAffected, fmt.Errorf("error inserting data: %v", err)
		}

		// Count affected rows
		if result.Next(ctx) {
			totalRowsAffected++
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return totalRowsAffected, fmt.Errorf("error committing transaction: %v", err)
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(driver neo4j.DriverWithContext) error {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// Delete all relationships and nodes
	_, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
	if err != nil {
		return fmt.Errorf("error wiping database: %v", err)
	}

	return nil
}

// nodeToMap converts a Neo4j node or relationship to a map
func nodeToMap(node interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	switch v := node.(type) {
	case neo4j.Node:
		// Add node ID
		result["_id"] = v.ElementId

		// Add labels
		labels := v.Labels
		if len(labels) > 0 {
			result["_labels"] = labels
		}

		// Add properties
		for key, value := range v.Props {
			result[key] = value
		}

	case neo4j.Relationship:
		// Add relationship ID and type
		result["_id"] = v.ElementId
		result["_type"] = v.Type

		// Add start and end node IDs
		result["_startNodeId"] = v.StartElementId
		result["_endNodeId"] = v.EndElementId

		// Add properties
		for key, value := range v.Props {
			result[key] = value
		}

	case map[string]interface{}:
		// If it's already a map, just return it
		return v
	}

	return result
}
