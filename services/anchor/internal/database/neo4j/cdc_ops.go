package neo4j

import (
	"context"
	"fmt"
	"strings"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
)

// ParseEvent converts a Neo4j-specific raw event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Determine event type (node or relationship)
	eventType, ok := rawEvent["type"].(string)
	if !ok {
		eventType = "node" // Default to node
	}
	event.Metadata["event_type"] = eventType

	// Extract operation type
	if operation, ok := rawEvent["operation"].(string); ok {
		switch strings.ToUpper(operation) {
		case "CREATE", "INSERT":
			event.Operation = adapter.CDCInsert
		case "UPDATE", "SET":
			event.Operation = adapter.CDCUpdate
		case "DELETE", "REMOVE":
			event.Operation = adapter.CDCDelete
		default:
			return nil, adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"parse_cdc_event",
				adapter.ErrInvalidData,
			).WithContext("error", fmt.Sprintf("unsupported operation: %s", operation))
		}
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Neo4j,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operation field")
	}

	// For Neo4j, we use labels as "table name" equivalent
	if labels, ok := rawEvent["labels"].([]interface{}); ok && len(labels) > 0 {
		// Use the first label as the table name
		if label, ok := labels[0].(string); ok {
			event.TableName = label
		}
		event.Metadata["labels"] = labels
	} else if label, ok := rawEvent["label"].(string); ok {
		event.TableName = label
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Neo4j,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing label/table_name field")
	}

	// Extract node/relationship properties
	if properties, ok := rawEvent["properties"].(map[string]interface{}); ok {
		event.Data = properties
	} else if node, ok := rawEvent["node"].(map[string]interface{}); ok {
		event.Data = node
	}

	// Extract old properties for updates/deletes
	if oldProperties, ok := rawEvent["old_properties"].(map[string]interface{}); ok {
		event.OldData = oldProperties
	}

	// Extract node ID or relationship ID as LSN
	if nodeId, ok := rawEvent["node_id"]; ok {
		event.LSN = fmt.Sprintf("%v", nodeId)
		event.Metadata["node_id"] = nodeId
	} else if relId, ok := rawEvent["relationship_id"]; ok {
		event.LSN = fmt.Sprintf("%v", relId)
		event.Metadata["relationship_id"] = relId
	}

	// Extract timestamp
	if timestamp, ok := rawEvent["timestamp"]; ok {
		switch ts := timestamp.(type) {
		case int64:
			event.Timestamp = time.Unix(ts, 0)
		case string:
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				event.Timestamp = t
			}
		case time.Time:
			event.Timestamp = ts
		}
		event.Metadata["timestamp"] = timestamp
	}

	// For relationships, store additional metadata
	if eventType == "relationship" {
		if startNodeId, ok := rawEvent["start_node_id"]; ok {
			event.Metadata["start_node_id"] = startNodeId
		}
		if endNodeId, ok := rawEvent["end_node_id"]; ok {
			event.Metadata["end_node_id"] = endNodeId
		}
		if relType, ok := rawEvent["relationship_type"].(string); ok {
			event.Metadata["relationship_type"] = relType
			// For relationships, use relationship type as table name if label is not set
			if event.TableName == "" {
				event.TableName = relType
			}
		}
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Neo4j, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to Neo4j.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_event", err)
	}

	// Determine if this is a node or relationship event
	eventType := "node"
	if et, ok := event.Metadata["event_type"].(string); ok {
		eventType = et
	}

	if eventType == "relationship" {
		return r.applyRelationshipCDCEvent(ctx, event)
	}

	// Default to node events
	return r.applyNodeCDCEvent(ctx, event)
}

// applyNodeCDCEvent applies CDC events for nodes.
func (r *ReplicationOps) applyNodeCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	session := r.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: event.SchemaName,
	})
	defer session.Close(ctx)

	switch event.Operation {
	case adapter.CDCInsert:
		return r.applyCDCInsertNode(ctx, session, event)
	case adapter.CDCUpdate:
		return r.applyCDCUpdateNode(ctx, session, event)
	case adapter.CDCDelete:
		return r.applyCDCDeleteNode(ctx, session, event)
	case adapter.CDCTruncate:
		return r.applyCDCTruncateNodes(ctx, session, event)
	default:
		return adapter.NewDatabaseError(
			dbcapabilities.Neo4j,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsertNode handles INSERT operations for nodes.
func (r *ReplicationOps) applyCDCInsertNode(ctx context.Context, session neo4jdriver.SessionWithContext, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Neo4j,
			"apply_cdc_insert_node",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Build CREATE query
	query := fmt.Sprintf("CREATE (n:%s) SET n = $properties RETURN id(n)", event.TableName)

	// Filter out metadata fields
	properties := make(map[string]interface{})
	for k, v := range event.Data {
		if !r.isMetadataField(k) {
			properties[k] = v
		}
	}

	_, err := session.Run(ctx, query, map[string]interface{}{
		"properties": properties,
	})
	if err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_insert_node", err)
	}

	return nil
}

// applyCDCUpdateNode handles UPDATE operations for nodes.
func (r *ReplicationOps) applyCDCUpdateNode(ctx context.Context, session neo4jdriver.SessionWithContext, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Neo4j,
			"apply_cdc_update_node",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Build MATCH query based on node_id if available, otherwise use properties
	var query string
	params := make(map[string]interface{})

	if nodeId, ok := event.Metadata["node_id"]; ok {
		// Update by node ID
		query = fmt.Sprintf("MATCH (n:%s) WHERE id(n) = $nodeId SET n += $properties RETURN n", event.TableName)
		params["nodeId"] = nodeId
	} else {
		// Update by properties (use OldData if available)
		matchData := event.OldData
		if len(matchData) == 0 {
			matchData = event.Data
		}

		// Build WHERE clause
		whereClauses := make([]string, 0)
		for k, v := range matchData {
			if !r.isMetadataField(k) {
				paramName := fmt.Sprintf("match_%s", k)
				whereClauses = append(whereClauses, fmt.Sprintf("n.%s = $%s", k, paramName))
				params[paramName] = v
			}
		}

		if len(whereClauses) == 0 {
			return adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"apply_cdc_update_node",
				adapter.ErrInvalidData,
			).WithContext("error", "no properties to match for update")
		}

		query = fmt.Sprintf("MATCH (n:%s) WHERE %s SET n += $properties RETURN n",
			event.TableName,
			strings.Join(whereClauses, " AND "))
	}

	// Filter out metadata fields
	properties := make(map[string]interface{})
	for k, v := range event.Data {
		if !r.isMetadataField(k) {
			properties[k] = v
		}
	}
	params["properties"] = properties

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_update_node", err)
	}

	return nil
}

// applyCDCDeleteNode handles DELETE operations for nodes.
func (r *ReplicationOps) applyCDCDeleteNode(ctx context.Context, session neo4jdriver.SessionWithContext, event *adapter.CDCEvent) error {
	var query string
	params := make(map[string]interface{})

	if nodeId, ok := event.Metadata["node_id"]; ok {
		// Delete by node ID
		query = fmt.Sprintf("MATCH (n:%s) WHERE id(n) = $nodeId DETACH DELETE n", event.TableName)
		params["nodeId"] = nodeId
	} else {
		// Delete by properties
		deleteData := event.OldData
		if len(deleteData) == 0 {
			deleteData = event.Data
		}

		if len(deleteData) == 0 {
			return adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"apply_cdc_delete_node",
				adapter.ErrInvalidData,
			).WithContext("error", "no data to identify node for DELETE")
		}

		// Build WHERE clause
		whereClauses := make([]string, 0)
		for k, v := range deleteData {
			if !r.isMetadataField(k) {
				paramName := fmt.Sprintf("match_%s", k)
				whereClauses = append(whereClauses, fmt.Sprintf("n.%s = $%s", k, paramName))
				params[paramName] = v
			}
		}

		if len(whereClauses) == 0 {
			return adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"apply_cdc_delete_node",
				adapter.ErrInvalidData,
			).WithContext("error", "no properties to match for DELETE")
		}

		query = fmt.Sprintf("MATCH (n:%s) WHERE %s DETACH DELETE n",
			event.TableName,
			strings.Join(whereClauses, " AND "))
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_delete_node", err)
	}

	return nil
}

// applyCDCTruncateNodes handles TRUNCATE operations (delete all nodes with a label).
func (r *ReplicationOps) applyCDCTruncateNodes(ctx context.Context, session neo4jdriver.SessionWithContext, event *adapter.CDCEvent) error {
	query := fmt.Sprintf("MATCH (n:%s) DETACH DELETE n", event.TableName)

	_, err := session.Run(ctx, query, nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_truncate_nodes", err)
	}

	return nil
}

// applyRelationshipCDCEvent applies CDC events for relationships.
func (r *ReplicationOps) applyRelationshipCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	session := r.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: event.SchemaName,
	})
	defer session.Close(ctx)

	relType := event.TableName
	if rt, ok := event.Metadata["relationship_type"].(string); ok {
		relType = rt
	}

	switch event.Operation {
	case adapter.CDCInsert:
		// Create relationship
		startNodeId := event.Metadata["start_node_id"]
		endNodeId := event.Metadata["end_node_id"]

		if startNodeId == nil || endNodeId == nil {
			return adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"apply_cdc_insert_relationship",
				adapter.ErrInvalidData,
			).WithContext("error", "missing start_node_id or end_node_id")
		}

		query := fmt.Sprintf("MATCH (a), (b) WHERE id(a) = $startNodeId AND id(b) = $endNodeId CREATE (a)-[r:%s]->(b) SET r = $properties RETURN id(r)", relType)

		properties := make(map[string]interface{})
		for k, v := range event.Data {
			if !r.isMetadataField(k) {
				properties[k] = v
			}
		}

		_, err := session.Run(ctx, query, map[string]interface{}{
			"startNodeId": startNodeId,
			"endNodeId":   endNodeId,
			"properties":  properties,
		})
		if err != nil {
			return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_insert_relationship", err)
		}

	case adapter.CDCUpdate:
		// Update relationship properties
		relId := event.Metadata["relationship_id"]
		if relId == nil {
			return adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"apply_cdc_update_relationship",
				adapter.ErrInvalidData,
			).WithContext("error", "missing relationship_id")
		}

		query := "MATCH ()-[r]->() WHERE id(r) = $relId SET r += $properties RETURN r"

		properties := make(map[string]interface{})
		for k, v := range event.Data {
			if !r.isMetadataField(k) {
				properties[k] = v
			}
		}

		_, err := session.Run(ctx, query, map[string]interface{}{
			"relId":      relId,
			"properties": properties,
		})
		if err != nil {
			return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_update_relationship", err)
		}

	case adapter.CDCDelete:
		// Delete relationship
		relId := event.Metadata["relationship_id"]
		if relId == nil {
			return adapter.NewDatabaseError(
				dbcapabilities.Neo4j,
				"apply_cdc_delete_relationship",
				adapter.ErrInvalidData,
			).WithContext("error", "missing relationship_id")
		}

		query := "MATCH ()-[r]->() WHERE id(r) = $relId DELETE r"

		_, err := session.Run(ctx, query, map[string]interface{}{
			"relId": relId,
		})
		if err != nil {
			return adapter.WrapError(dbcapabilities.Neo4j, "apply_cdc_delete_relationship", err)
		}
	}

	return nil
}

// TransformData applies transformation rules to event data.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	if len(rules) == 0 {
		return data, nil
	}

	transformedData := make(map[string]interface{})

	// Create transformation service client if endpoint is provided
	var transformClient transformationv1.TransformationServiceClient
	var grpcConn *grpc.ClientConn
	if transformationServiceEndpoint != "" {
		// Connect to transformation service
		conn, err := grpc.Dial(transformationServiceEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			transformClient = transformationv1.NewTransformationServiceClient(conn)
			grpcConn = conn
			defer conn.Close()
		}
	}

	// Apply each transformation rule
	for _, rule := range rules {
		// Get source value
		sourceValue, exists := data[rule.SourceColumn]
		if !exists {
			// Source column doesn't exist - skip this rule
			continue
		}

		// Apply transformation based on type
		var transformedValue interface{}
		var err error

		// Check if there's a custom transformation name
		if rule.TransformationName != "" && rule.TransformationName != "direct_mapping" && grpcConn != nil {
			// Call transformation service for custom transformations
			transformedValue, err = callTransformationService(ctx, transformClient, rule.TransformationName, sourceValue)
			if err != nil {
				// Log warning and fall back to source value
				transformedValue = sourceValue
			}
		} else {
			// Handle basic transformations locally
			transformType := rule.TransformationType
			if transformType == "" && rule.TransformationName != "" {
				transformType = rule.TransformationName
			}

			switch transformType {
			case adapter.TransformDirect, "direct_mapping":
				transformedValue = sourceValue

			case adapter.TransformUppercase:
				if str, ok := sourceValue.(string); ok {
					transformedValue = strings.ToUpper(str)
				} else {
					transformedValue = sourceValue
				}

			case adapter.TransformLowercase:
				if str, ok := sourceValue.(string); ok {
					transformedValue = strings.ToLower(str)
				} else {
					transformedValue = sourceValue
				}

			case adapter.TransformCast:
				// Type casting would be implemented here
				// For now, just pass through
				transformedValue = sourceValue

			case adapter.TransformDefault:
				if sourceValue == nil {
					// Use default value from parameters
					if defaultVal, ok := rule.Parameters["default_value"]; ok {
						transformedValue = defaultVal
					} else {
						transformedValue = nil
					}
				} else {
					transformedValue = sourceValue
				}

			default:
				// Unknown transformation type - pass through source value
				transformedValue = sourceValue
			}
		}

		transformedData[rule.TargetColumn] = transformedValue
	}

	return transformedData, nil
}

// callTransformationService calls the transformation service to apply a custom transformation
func callTransformationService(ctx context.Context, client transformationv1.TransformationServiceClient, transformationName string, value interface{}) (interface{}, error) {
	// Convert value to string for transformation
	var inputStr string
	switch v := value.(type) {
	case string:
		inputStr = v
	case nil:
		return nil, nil
	default:
		// Convert other types to string
		inputStr = fmt.Sprintf("%v", v)
	}

	// Call transformation service
	transformReq := &transformationv1.TransformRequest{
		FunctionName: transformationName,
		Input:        inputStr,
	}

	transformResp, err := client.Transform(ctx, transformReq)
	if err != nil {
		return nil, fmt.Errorf("transformation service error: %v", err)
	}

	if transformResp.Status != commonv1.Status_STATUS_SUCCESS {
		return nil, fmt.Errorf("transformation failed: %s", transformResp.StatusMessage)
	}

	return transformResp.Output, nil
}

// Helper methods

// isMetadataField checks if a field name is a metadata field that should be skipped.
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
	metadataFields := map[string]bool{
		"message_type":      true,
		"raw_data_b64":      true,
		"data_length":       true,
		"database_id":       true,
		"timestamp":         true,
		"schema_name":       true,
		"operation":         true,
		"table_name":        true,
		"node_id":           true,
		"relationship_id":   true,
		"start_node_id":     true,
		"end_node_id":       true,
		"event_type":        true,
		"labels":            true,
		"relationship_type": true,
		"_cdc_timestamp":    true,
		"_cdc_operation":    true,
	}
	return metadataFields[fieldName] || strings.HasPrefix(fieldName, "_cdc_")
}
