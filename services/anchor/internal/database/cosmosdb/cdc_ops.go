package cosmosdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
)

// ParseEvent converts a CosmosDB Change Feed event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract operation type
	// CosmosDB Change Feed only provides create and replace operations
	// Deletes require TTL or explicit tracking
	operationType, ok := rawEvent["operation"].(string)
	if !ok {
		// Default to upsert if not specified
		operationType = "upsert"
	}

	// Map CosmosDB operations to CDCOperation
	switch operationType {
	case "create":
		event.Operation = adapter.CDCInsert
	case "replace", "upsert":
		event.Operation = adapter.CDCUpdate
	case "delete":
		event.Operation = adapter.CDCDelete
	default:
		return nil, adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", operationType)
	}

	// Extract container name (table name)
	if containerName, ok := rawEvent["container_name"].(string); ok {
		event.TableName = containerName
	} else if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing container_name or table_name field")
	}

	// Extract document data
	if doc, ok := rawEvent["document"].(map[string]interface{}); ok {
		event.Data = doc
	} else if data, ok := rawEvent["data"].(map[string]interface{}); ok {
		event.Data = data
	}

	// Extract old data if available (for updates/deletes)
	if oldDoc, ok := rawEvent["old_document"].(map[string]interface{}); ok {
		event.OldData = oldDoc
	} else if oldData, ok := rawEvent["old_data"].(map[string]interface{}); ok {
		event.OldData = oldData
	}

	// Extract LSN (logical sequence number) if available
	if lsn, ok := rawEvent["_lsn"].(string); ok {
		event.LSN = lsn
	} else if lsn, ok := rawEvent["lsn"].(float64); ok {
		event.LSN = fmt.Sprintf("%.0f", lsn)
	}

	// Store continuation token in metadata
	if continuationToken, ok := rawEvent["continuation_token"].(string); ok {
		event.Metadata["continuation_token"] = continuationToken
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.CosmosDB, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to CosmosDB.
// This handles INSERT, UPDATE, and DELETE operations.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "apply_cdc_event", err)
	}

	// Get database and container clients
	dbClient, err := r.conn.client.NewDatabase(r.conn.dbName)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "get_database_client", err)
	}

	containerClient, err := dbClient.NewContainer(event.TableName)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "get_container_client", err)
	}

	// Route to appropriate handler based on operation
	switch event.Operation {
	case adapter.CDCInsert:
		return r.applyCDCInsert(ctx, containerClient, event)
	case adapter.CDCUpdate:
		return r.applyCDCUpdate(ctx, containerClient, event)
	case adapter.CDCDelete:
		return r.applyCDCDelete(ctx, containerClient, event)
	default:
		return adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for CosmosDB.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, container *azcosmos.ContainerClient, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Convert data to JSON
	doc := make(map[string]interface{})
	for k, v := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		doc[k] = v
	}

	if len(doc) == 0 {
		return nil // No actual data to insert
	}

	// Marshal document
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "marshal_document", err)
	}

	// Extract partition key value from document
	// Note: This assumes "id" is the partition key, adjust as needed
	partitionKey := azcosmos.NewPartitionKeyString("")
	if id, ok := doc["id"].(string); ok {
		partitionKey = azcosmos.NewPartitionKeyString(id)
	}

	// Create item
	_, err = container.CreateItem(ctx, partitionKey, docBytes, nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for CosmosDB.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, container *azcosmos.ContainerClient, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Convert data to JSON
	doc := make(map[string]interface{})
	for k, v := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		doc[k] = v
	}

	if len(doc) == 0 {
		return nil // No actual data to update
	}

	// Marshal document
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "marshal_document", err)
	}

	// Extract partition key value from document
	partitionKey := azcosmos.NewPartitionKeyString("")
	if id, ok := doc["id"].(string); ok {
		partitionKey = azcosmos.NewPartitionKeyString(id)
	}

	// Upsert item (creates if doesn't exist, replaces if exists)
	_, err = container.UpsertItem(ctx, partitionKey, docBytes, nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "apply_cdc_update", err)
	}

	return nil
}

// applyCDCDelete handles DELETE operations for CosmosDB.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, container *azcosmos.ContainerClient, event *adapter.CDCEvent) error {
	// For DELETE, we need the document ID and partition key
	whereData := event.OldData
	if len(whereData) == 0 {
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify document for DELETE")
	}

	// Extract document ID
	docID, ok := whereData["id"].(string)
	if !ok {
		return adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "missing document id for DELETE")
	}

	// Extract partition key
	partitionKey := azcosmos.NewPartitionKeyString(docID)

	// Delete item
	_, err := container.DeleteItem(ctx, partitionKey, docID, nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CosmosDB, "apply_cdc_delete", err)
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
		conn, err := grpc.Dial(transformationServiceEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			transformClient = transformationv1.NewTransformationServiceClient(conn)
			grpcConn = conn
			defer conn.Close()
		}
	}

	// Apply each transformation rule
	for _, rule := range rules {
		sourceValue, exists := data[rule.SourceColumn]
		if !exists {
			continue
		}

		var transformedValue interface{}
		var err error

		if rule.TransformationName != "" && rule.TransformationName != "direct_mapping" && grpcConn != nil {
			transformedValue, err = callTransformationService(ctx, transformClient, rule.TransformationName, sourceValue)
			if err != nil {
				transformedValue = sourceValue
			}
		} else {
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
				transformedValue = sourceValue
			case adapter.TransformDefault:
				if sourceValue == nil {
					if defaultVal, ok := rule.Parameters["default_value"]; ok {
						transformedValue = defaultVal
					} else {
						transformedValue = nil
					}
				} else {
					transformedValue = sourceValue
				}
			default:
				transformedValue = sourceValue
			}
		}

		transformedData[rule.TargetColumn] = transformedValue
	}

	return transformedData, nil
}

// callTransformationService calls the transformation service to apply a custom transformation.
func callTransformationService(ctx context.Context, client transformationv1.TransformationServiceClient, transformationName string, value interface{}) (interface{}, error) {
	var inputStr string
	switch v := value.(type) {
	case string:
		inputStr = v
	case nil:
		return nil, nil
	default:
		inputStr = fmt.Sprintf("%v", v)
	}

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

// isMetadataField checks if a field name is a metadata field that should be skipped.
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
	metadataFields := map[string]bool{
		"message_type":       true,
		"raw_data_b64":       true,
		"data_length":        true,
		"database_id":        true,
		"timestamp":          true,
		"schema_name":        true,
		"operation":          true,
		"table_name":         true,
		"container_name":     true,
		"continuation_token": true,
		"_lsn":               true,
		"_ts":                true,
		"_etag":              true,
		"_attachments":       true,
		"_self":              true,
		"_rid":               true,
	}
	return metadataFields[fieldName]
}
