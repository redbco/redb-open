package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
)

// ParseEvent converts a DynamoDB Streams event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract event name (INSERT, MODIFY, REMOVE)
	eventName, ok := rawEvent["event_name"].(string)
	if !ok {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing event_name field")
	}

	// Map DynamoDB event names to CDCOperation
	switch eventName {
	case "INSERT":
		event.Operation = adapter.CDCInsert
	case "MODIFY":
		event.Operation = adapter.CDCUpdate
	case "REMOVE":
		event.Operation = adapter.CDCDelete
	default:
		return nil, adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("event_name", eventName)
	}

	// Extract table name from event source or metadata
	// DynamoDB doesn't include table name directly, so we need to get it from config or metadata
	// For now, we'll require it to be passed in metadata
	if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing table_name field")
	}

	// Extract data based on operation type
	switch event.Operation {
	case adapter.CDCInsert:
		if newImage, ok := rawEvent["new_image"].(map[string]types.AttributeValue); ok {
			data, err := attributeValueMapToInterface(newImage)
			if err != nil {
				return nil, adapter.WrapError(dbcapabilities.DynamoDB, "parse_new_image", err)
			}
			event.Data = data
		}

	case adapter.CDCUpdate:
		if newImage, ok := rawEvent["new_image"].(map[string]types.AttributeValue); ok {
			data, err := attributeValueMapToInterface(newImage)
			if err != nil {
				return nil, adapter.WrapError(dbcapabilities.DynamoDB, "parse_new_image", err)
			}
			event.Data = data
		}
		if oldImage, ok := rawEvent["old_image"].(map[string]types.AttributeValue); ok {
			oldData, err := attributeValueMapToInterface(oldImage)
			if err != nil {
				return nil, adapter.WrapError(dbcapabilities.DynamoDB, "parse_old_image", err)
			}
			event.OldData = oldData
		}

	case adapter.CDCDelete:
		// For DELETE, we get the keys and old image
		if keys, ok := rawEvent["keys"].(map[string]types.AttributeValue); ok {
			keyData, err := attributeValueMapToInterface(keys)
			if err != nil {
				return nil, adapter.WrapError(dbcapabilities.DynamoDB, "parse_keys", err)
			}
			event.OldData = keyData
		}
		if oldImage, ok := rawEvent["old_image"].(map[string]types.AttributeValue); ok {
			oldData, err := attributeValueMapToInterface(oldImage)
			if err != nil {
				return nil, adapter.WrapError(dbcapabilities.DynamoDB, "parse_old_image", err)
			}
			// Merge keys and old image
			for k, v := range oldData {
				event.OldData[k] = v
			}
		}
	}

	// Extract sequence number as LSN
	if seqNum, ok := rawEvent["sequence_number"].(string); ok {
		event.LSN = seqNum
	}

	// Store stream view type in metadata
	if streamViewType, ok := rawEvent["stream_view_type"].(string); ok {
		event.Metadata["stream_view_type"] = streamViewType
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.DynamoDB, "parse_cdc_event", err)
	}

	return event, nil
}

// attributeValueMapToInterface converts DynamoDB AttributeValue map to map[string]interface{}.
func attributeValueMapToInterface(av map[string]types.AttributeValue) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for key, value := range av {
		converted, err := attributeValueToInterface(value)
		if err != nil {
			return nil, err
		}
		result[key] = converted
	}

	return result, nil
}

// attributeValueToInterface converts a single AttributeValue to interface{}.
func attributeValueToInterface(av types.AttributeValue) (interface{}, error) {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value, nil
	case *types.AttributeValueMemberN:
		return v.Value, nil // Keep as string to preserve precision
	case *types.AttributeValueMemberB:
		return v.Value, nil
	case *types.AttributeValueMemberSS:
		return v.Value, nil
	case *types.AttributeValueMemberNS:
		return v.Value, nil
	case *types.AttributeValueMemberBS:
		return v.Value, nil
	case *types.AttributeValueMemberM:
		return attributeValueMapToInterface(v.Value)
	case *types.AttributeValueMemberL:
		result := make([]interface{}, len(v.Value))
		for i, item := range v.Value {
			converted, err := attributeValueToInterface(item)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case *types.AttributeValueMemberNULL:
		return nil, nil
	case *types.AttributeValueMemberBOOL:
		return v.Value, nil
	default:
		return nil, fmt.Errorf("unknown attribute value type: %T", av)
	}
}

// ApplyCDCEvent applies a standardized CDC event to DynamoDB.
// This handles INSERT, UPDATE, and DELETE operations.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.DynamoDB, "apply_cdc_event", err)
	}

	// Route to appropriate handler based on operation
	switch event.Operation {
	case adapter.CDCInsert:
		return r.applyCDCInsert(ctx, event)
	case adapter.CDCUpdate:
		return r.applyCDCUpdate(ctx, event)
	case adapter.CDCDelete:
		return r.applyCDCDelete(ctx, event)
	default:
		return adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for DynamoDB.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Convert data map to DynamoDB attribute value map
	item := make(map[string]types.AttributeValue)
	for k, v := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		av, err := attributevalue.Marshal(v)
		if err != nil {
			return adapter.WrapError(dbcapabilities.DynamoDB, "marshal_attribute", err)
		}
		item[k] = av
	}

	if len(item) == 0 {
		return nil // No actual data to insert
	}

	// Insert the item
	_, err := r.conn.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(event.TableName),
		Item:      item,
	})
	if err != nil {
		return adapter.WrapError(dbcapabilities.DynamoDB, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for DynamoDB.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// For DynamoDB, we use PutItem which replaces the entire item
	// This is similar to how DynamoDB Streams MODIFY events work
	item := make(map[string]types.AttributeValue)
	for k, v := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		av, err := attributevalue.Marshal(v)
		if err != nil {
			return adapter.WrapError(dbcapabilities.DynamoDB, "marshal_attribute", err)
		}
		item[k] = av
	}

	if len(item) == 0 {
		return nil // No actual data to update
	}

	// Put the item (replaces if exists)
	_, err := r.conn.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(event.TableName),
		Item:      item,
	})
	if err != nil {
		return adapter.WrapError(dbcapabilities.DynamoDB, "apply_cdc_update", err)
	}

	return nil
}

// applyCDCDelete handles DELETE operations for DynamoDB.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
	// For DELETE, we need the key attributes
	whereData := event.OldData
	if len(whereData) == 0 {
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify item for DELETE")
	}

	// Build key map (only include key attributes)
	// Note: This assumes we know which attributes are keys
	// In a real implementation, you'd query the table schema first
	key := make(map[string]types.AttributeValue)
	for k, v := range whereData {
		// Skip metadata fields
		if r.isMetadataField(k) {
			continue
		}
		av, err := attributevalue.Marshal(v)
		if err != nil {
			return adapter.WrapError(dbcapabilities.DynamoDB, "marshal_key", err)
		}
		key[k] = av
	}

	if len(key) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.DynamoDB,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no key attributes for DELETE")
	}

	// Delete the item
	_, err := r.conn.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(event.TableName),
		Key:       key,
	})
	if err != nil {
		return adapter.WrapError(dbcapabilities.DynamoDB, "apply_cdc_delete", err)
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
		"message_type":     true,
		"raw_data_b64":     true,
		"data_length":      true,
		"database_id":      true,
		"timestamp":        true,
		"schema_name":      true,
		"operation":        true,
		"table_name":       true,
		"sequence_number":  true,
		"stream_view_type": true,
		"event_name":       true,
		"event_source":     true,
	}
	return metadataFields[fieldName]
}
