package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
)

// ParseEvent converts a Redis keyspace notification event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract key (Redis doesn't have tables, key is the identifier)
	if key, ok := rawEvent["key"].(string); ok {
		event.TableName = "redis_keys" // Generic table name for Redis
		event.Metadata["key"] = key
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Redis,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing key field")
	}

	// Extract operation type
	if operation, ok := rawEvent["operation"].(string); ok {
		switch strings.ToUpper(operation) {
		case "INSERT":
			event.Operation = adapter.CDCInsert
		case "UPDATE":
			event.Operation = adapter.CDCUpdate
		case "DELETE":
			event.Operation = adapter.CDCDelete
		default:
			return nil, adapter.NewDatabaseError(
				dbcapabilities.Redis,
				"parse_cdc_event",
				adapter.ErrInvalidData,
			).WithContext("error", fmt.Sprintf("unsupported operation: %s", operation))
		}
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Redis,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operation field")
	}

	// Extract data
	if data, ok := rawEvent["data"]; ok {
		// Convert data to map format
		event.Data = make(map[string]interface{})
		if key, ok := rawEvent["key"].(string); ok {
			event.Data["key"] = key
			event.Data["value"] = data
		}
	}

	// Extract old data
	if oldData, ok := rawEvent["old_data"]; ok {
		event.OldData = make(map[string]interface{})
		if key, ok := rawEvent["key"].(string); ok {
			event.OldData["key"] = key
			event.OldData["value"] = oldData
		}
	}

	// Extract command metadata
	if command, ok := rawEvent["command"].(string); ok {
		event.Metadata["redis_command"] = command
	}

	// Use key as LSN equivalent
	if key, ok := rawEvent["key"].(string); ok {
		event.LSN = fmt.Sprintf("%s_%d", key, time.Now().UnixNano())
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to Redis.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_event", err)
	}

	// Extract key from data
	key, ok := event.Data["key"].(string)
	if !ok {
		return adapter.NewDatabaseError(
			dbcapabilities.Redis,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing key in event data")
	}

	// Route to appropriate handler based on operation
	switch event.Operation {
	case adapter.CDCInsert, adapter.CDCUpdate:
		return r.applyCDCSet(ctx, event, key)
	case adapter.CDCDelete:
		return r.applyCDCDelete(ctx, event, key)
	case adapter.CDCTruncate:
		return r.applyCDCTruncate(ctx, event)
	default:
		return adapter.NewDatabaseError(
			dbcapabilities.Redis,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCSet handles INSERT/UPDATE operations for Redis.
func (r *ReplicationOps) applyCDCSet(ctx context.Context, event *adapter.CDCEvent, key string) error {
	value, ok := event.Data["value"]
	if !ok {
		return adapter.NewDatabaseError(
			dbcapabilities.Redis,
			"apply_cdc_set",
			adapter.ErrInvalidData,
		).WithContext("error", "missing value in event data")
	}

	// Determine the appropriate Redis command based on value type
	switch v := value.(type) {
	case string:
		// String value - use SET
		err := r.conn.client.Set(ctx, key, v, 0).Err()
		if err != nil {
			return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_set", err)
		}

	case []interface{}:
		// List value - use RPUSH
		// First delete existing key
		r.conn.client.Del(ctx, key)
		if len(v) > 0 {
			err := r.conn.client.RPush(ctx, key, v...).Err()
			if err != nil {
				return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_set_list", err)
			}
		}

	case map[string]interface{}:
		// Hash value - use HSET
		// First delete existing key
		r.conn.client.Del(ctx, key)
		if len(v) > 0 {
			err := r.conn.client.HSet(ctx, key, v).Err()
			if err != nil {
				return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_set_hash", err)
			}
		}

	default:
		// Default to string representation
		err := r.conn.client.Set(ctx, key, fmt.Sprintf("%v", v), 0).Err()
		if err != nil {
			return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_set", err)
		}
	}

	return nil
}

// applyCDCDelete handles DELETE operations for Redis.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent, key string) error {
	err := r.conn.client.Del(ctx, key).Err()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_delete", err)
	}
	return nil
}

// applyCDCTruncate handles TRUNCATE operations (FLUSHDB) for Redis.
func (r *ReplicationOps) applyCDCTruncate(ctx context.Context, event *adapter.CDCEvent) error {
	// TRUNCATE in Redis context means FLUSHDB
	err := r.conn.client.FlushDB(ctx).Err()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Redis, "apply_cdc_truncate", err)
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
