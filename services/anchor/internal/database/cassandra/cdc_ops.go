package cassandra

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

// ParseEvent converts a Cassandra polling event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract table name
	if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing table_name field")
	}

	// Extract keyspace (schema)
	if keyspace, ok := rawEvent["keyspace"].(string); ok {
		event.SchemaName = keyspace
		event.Metadata["keyspace"] = keyspace
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
				dbcapabilities.Cassandra,
				"parse_cdc_event",
				adapter.ErrInvalidData,
			).WithContext("error", fmt.Sprintf("unsupported operation: %s", operation))
		}
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operation field")
	}

	// Extract data
	if data, ok := rawEvent["data"].(map[string]interface{}); ok {
		event.Data = data
	}

	// Extract old data
	if oldData, ok := rawEvent["old_data"].(map[string]interface{}); ok {
		event.OldData = oldData
	}

	// Use a combination of timestamp and data for LSN
	event.LSN = fmt.Sprintf("%s_%d", event.TableName, time.Now().UnixNano())

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to Cassandra.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "apply_cdc_event", err)
	}

	// Route to appropriate handler based on operation
	switch event.Operation {
	case adapter.CDCInsert:
		return r.applyCDCInsert(ctx, event)
	case adapter.CDCUpdate:
		return r.applyCDCUpdate(ctx, event)
	case adapter.CDCDelete:
		return r.applyCDCDelete(ctx, event)
	case adapter.CDCTruncate:
		return r.applyCDCTruncate(ctx, event)
	default:
		return adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for Cassandra.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Build column names and placeholders
	columns := make([]string, 0, len(event.Data))
	placeholders := make([]string, 0, len(event.Data))
	values := make([]interface{}, 0, len(event.Data))

	for col, val := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}
		columns = append(columns, QuoteIdentifier(col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	if len(columns) == 0 {
		return nil // No actual data columns
	}

	// Build full table name
	var fullTableName string
	if event.SchemaName != "" {
		fullTableName = fmt.Sprintf("%s.%s", QuoteIdentifier(event.SchemaName), QuoteIdentifier(event.TableName))
	} else {
		fullTableName = QuoteIdentifier(event.TableName)
	}

	// Build INSERT statement
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		fullTableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Execute the insert
	err := r.conn.session.Query(query, values...).WithContext(ctx).Exec()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for Cassandra.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Build full table name
	var fullTableName string
	if event.SchemaName != "" {
		fullTableName = fmt.Sprintf("%s.%s", QuoteIdentifier(event.SchemaName), QuoteIdentifier(event.TableName))
	} else {
		fullTableName = QuoteIdentifier(event.TableName)
	}

	// Build SET clause
	setClauses := make([]string, 0, len(event.Data))
	values := make([]interface{}, 0)

	for col, val := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", QuoteIdentifier(col)))
		values = append(values, val)
	}

	if len(setClauses) == 0 {
		return nil // No actual data columns
	}

	// Build WHERE clause from old data (if available) or current data
	whereData := event.OldData
	if len(whereData) == 0 {
		whereData = event.Data
	}

	whereClauses := make([]string, 0, len(whereData))
	for col, val := range whereData {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}

		if val == nil {
			// Cassandra doesn't support IS NULL in WHERE for updates on primary key
			// We'll need to identify primary key columns differently
			// For now, skip null values in WHERE clause
			continue
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", QuoteIdentifier(col)))
			values = append(values, val)
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no WHERE conditions for UPDATE")
	}

	// Build UPDATE statement
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		fullTableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	// Execute the update
	err := r.conn.session.Query(query, values...).WithContext(ctx).Exec()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "apply_cdc_update", err)
	}

	return nil
}

// applyCDCDelete handles DELETE operations for Cassandra.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
	// For DELETE, we need old data to identify which row to delete
	whereData := event.OldData
	if len(whereData) == 0 {
		// Try using current data as fallback
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify row for DELETE")
	}

	// Build full table name
	var fullTableName string
	if event.SchemaName != "" {
		fullTableName = fmt.Sprintf("%s.%s", QuoteIdentifier(event.SchemaName), QuoteIdentifier(event.TableName))
	} else {
		fullTableName = QuoteIdentifier(event.TableName)
	}

	// Build WHERE clause
	whereClauses := make([]string, 0, len(whereData))
	values := make([]interface{}, 0)

	for col, val := range whereData {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}

		if val == nil {
			// Cassandra doesn't support IS NULL in WHERE for DELETE
			continue
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", QuoteIdentifier(col)))
			values = append(values, val)
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no WHERE conditions for DELETE")
	}

	// Build DELETE statement
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		fullTableName,
		strings.Join(whereClauses, " AND "),
	)

	// Execute the delete
	err := r.conn.session.Query(query, values...).WithContext(ctx).Exec()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "apply_cdc_delete", err)
	}

	return nil
}

// applyCDCTruncate handles TRUNCATE operations for Cassandra.
func (r *ReplicationOps) applyCDCTruncate(ctx context.Context, event *adapter.CDCEvent) error {
	// Build full table name
	var fullTableName string
	if event.SchemaName != "" {
		fullTableName = fmt.Sprintf("%s.%s", QuoteIdentifier(event.SchemaName), QuoteIdentifier(event.TableName))
	} else {
		fullTableName = QuoteIdentifier(event.TableName)
	}

	query := fmt.Sprintf("TRUNCATE %s", fullTableName)

	err := r.conn.session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "apply_cdc_truncate", err)
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
		"message_type": true,
		"raw_data_b64": true,
		"data_length":  true,
		"database_id":  true,
		"timestamp":    true,
		"schema_name":  true,
		"keyspace":     true,
		"operation":    true,
		"table_name":   true,
	}
	return metadataFields[fieldName]
}
