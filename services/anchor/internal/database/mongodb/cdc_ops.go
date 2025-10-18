package mongodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
)

// ParseEvent converts a MongoDB Change Stream event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract operation type
	operationType, ok := rawEvent["operationType"].(string)
	if !ok {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operationType field")
	}

	// Map MongoDB operation types to CDCOperation
	switch operationType {
	case "insert":
		event.Operation = adapter.CDCInsert
	case "update", "replace":
		event.Operation = adapter.CDCUpdate
	case "delete":
		event.Operation = adapter.CDCDelete
	default:
		// Skip invalidate, drop, rename, dropDatabase events for now
		return nil, adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", operationType)
	}

	// Extract namespace information
	if ns, ok := rawEvent["ns"].(map[string]interface{}); ok {
		if db, ok := ns["db"].(string); ok {
			event.SchemaName = db
		}
		if coll, ok := ns["coll"].(string); ok {
			event.TableName = coll
		}
	}

	if event.TableName == "" {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing collection name")
	}

	// Extract document data based on operation type
	switch event.Operation {
	case adapter.CDCInsert:
		if fullDoc, ok := rawEvent["fullDocument"].(map[string]interface{}); ok {
			event.Data = fullDoc
		}

	case adapter.CDCUpdate:
		// For updates, we get the full document after update (if fullDocument is set to "updateLookup")
		if fullDoc, ok := rawEvent["fullDocument"].(map[string]interface{}); ok {
			event.Data = fullDoc
		}

		// Also capture the update description
		if updateDesc, ok := rawEvent["updateDescription"].(map[string]interface{}); ok {
			event.Metadata["update_description"] = updateDesc

			// Try to reconstruct old data from updatedFields and removedFields
			if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
				// Store what was updated
				event.Metadata["updated_fields"] = updatedFields
			}
			if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
				// Store what was removed
				event.Metadata["removed_fields"] = removedFields
			}
		}

	case adapter.CDCDelete:
		// For deletes, we need the documentKey (typically _id)
		if docKey, ok := rawEvent["documentKey"].(map[string]interface{}); ok {
			event.OldData = docKey
		}
	}

	// Extract cluster time as LSN equivalent
	if clusterTime, ok := rawEvent["clusterTime"]; ok {
		event.LSN = fmt.Sprintf("%v", clusterTime)
	}

	// Extract transaction info if available
	if txnNumber, ok := rawEvent["txnNumber"]; ok {
		event.TransactionID = fmt.Sprintf("%v", txnNumber)
	}

	// Store resume token for positioning
	if resumeToken, ok := rawEvent["_id"]; ok {
		event.Metadata["resume_token"] = resumeToken
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to MongoDB.
// This handles INSERT, UPDATE, and DELETE operations.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "apply_cdc_event", err)
	}

	// Get the collection
	collection := r.conn.db.Collection(event.TableName)

	// Route to appropriate handler based on operation
	switch event.Operation {
	case adapter.CDCInsert:
		return r.applyCDCInsert(ctx, collection, event)
	case adapter.CDCUpdate:
		return r.applyCDCUpdate(ctx, collection, event)
	case adapter.CDCDelete:
		return r.applyCDCDelete(ctx, collection, event)
	default:
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for MongoDB.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, collection *mongo.Collection, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Convert map to BSON document
	doc := bson.M{}
	for k, v := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		doc[k] = v
	}

	if len(doc) == 0 {
		return nil // No actual data columns
	}

	// Insert the document
	_, err := collection.InsertOne(ctx, doc)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for MongoDB.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, collection *mongo.Collection, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Build update document
	updateDoc := bson.M{}
	for k, v := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		updateDoc[k] = v
	}

	if len(updateDoc) == 0 {
		return nil // No actual data to update
	}

	// Build filter from old data or _id from current data
	filter := bson.M{}

	// Try to get _id from old data
	if len(event.OldData) > 0 {
		if id, ok := event.OldData["_id"]; ok {
			filter["_id"] = id
		} else {
			// Use all fields from old data as filter
			for k, v := range event.OldData {
				if !r.isMetadataField(k) {
					filter[k] = v
				}
			}
		}
	} else {
		// Fall back to _id from new data
		if id, ok := event.Data["_id"]; ok {
			filter["_id"] = id
		}
	}

	if len(filter) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no filter criteria for update")
	}

	// Use replaceOne to replace the entire document (similar to MongoDB's replace operation)
	result, err := collection.ReplaceOne(ctx, filter, updateDoc)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "apply_cdc_update", err)
	}

	// Log warning if no documents were modified
	if result.ModifiedCount == 0 {
		// Document may not exist or is identical to update
	}

	return nil
}

// applyCDCDelete handles DELETE operations for MongoDB.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, collection *mongo.Collection, event *adapter.CDCEvent) error {
	// For DELETE, we need old data to identify which document to delete
	whereData := event.OldData
	if len(whereData) == 0 {
		// Try using current data as fallback
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify document for DELETE")
	}

	// Build filter for delete
	filter := bson.M{}
	for k, v := range whereData {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		filter[k] = v
	}

	if len(filter) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no filter criteria for DELETE")
	}

	// Delete the document
	result, err := collection.DeleteOne(ctx, filter)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "apply_cdc_delete", err)
	}

	// Log warning if no documents were deleted
	if result.DeletedCount == 0 {
		// Document may have already been deleted
	}

	return nil
}

// TransformData applies transformation rules to event data.
// This implementation handles basic transformations and calls the transformation service for custom transformations.
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

// callTransformationService calls the transformation service to apply a custom transformation.
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
		"message_type":       true,
		"raw_data_b64":       true,
		"data_length":        true,
		"is_update":          true,
		"database_id":        true,
		"timestamp":          true,
		"schema_name":        true,
		"operation":          true,
		"table_name":         true,
		"resume_token":       true,
		"cluster_time":       true,
		"update_description": true,
	}
	return metadataFields[fieldName]
}
