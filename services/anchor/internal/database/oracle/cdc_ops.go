//go:build enterprise
// +build enterprise

package oracle

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

// ParseEvent converts an Oracle-specific raw event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract operation
	if op, ok := rawEvent["operation"].(string); ok {
		event.Operation = adapter.CDCOperation(strings.ToUpper(op))
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operation field")
	}

	// Extract table name
	if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else if table, ok := rawEvent["table"].(string); ok {
		event.TableName = table
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing table_name field")
	}

	// Extract schema name (owner in Oracle)
	if schemaName, ok := rawEvent["schema_name"].(string); ok {
		event.SchemaName = schemaName
	} else if owner, ok := rawEvent["owner"].(string); ok {
		event.SchemaName = owner
	}

	// Extract data
	if data, ok := rawEvent["data"].(map[string]interface{}); ok {
		event.Data = data
	}

	// Extract old data
	if oldData, ok := rawEvent["old_data"].(map[string]interface{}); ok {
		event.OldData = oldData
	}

	// Extract metadata
	if databaseID, ok := rawEvent["database_id"].(string); ok {
		event.Metadata["database_id"] = databaseID
	}
	if scn, ok := rawEvent["scn"]; ok {
		event.Metadata["scn"] = scn
		event.LSN = fmt.Sprintf("%v", scn) // Use SCN as LSN
	}
	if timestamp, ok := rawEvent["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
			event.Timestamp = t
		}
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.Oracle, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to Oracle.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "apply_cdc_event", err)
	}

	// Route to appropriate handler
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
			dbcapabilities.Oracle,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for Oracle.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	var columns []string
	var placeholders []string
	var values []interface{}
	paramNum := 1

	for col, val := range event.Data {
		if r.isMetadataField(col) {
			continue
		}
		columns = append(columns, QuoteIdentifier(col))
		placeholders = append(placeholders, fmt.Sprintf(":%d", paramNum))
		values = append(values, val)
		paramNum++
	}

	if len(columns) == 0 {
		return nil
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdentifier(event.TableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for Oracle.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	var setClauses []string
	var values []interface{}
	paramNum := 1

	for col, val := range event.Data {
		if r.isMetadataField(col) {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = :%d", QuoteIdentifier(col), paramNum))
		values = append(values, val)
		paramNum++
	}

	if len(setClauses) == 0 {
		return nil
	}

	// Build WHERE clause
	var whereClauses []string
	whereData := event.OldData
	if len(whereData) == 0 {
		whereData = event.Data
	}

	for col, val := range whereData {
		if r.isMetadataField(col) {
			continue
		}
		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", QuoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = :%d", QuoteIdentifier(col), paramNum))
			values = append(values, val)
			paramNum++
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no where clause could be built")
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		QuoteIdentifier(event.TableName),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "apply_cdc_update", err)
	}

	return nil
}

// applyCDCDelete handles DELETE operations for Oracle.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
	var whereClauses []string
	var values []interface{}
	paramNum := 1

	whereData := event.OldData
	if len(whereData) == 0 {
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data for WHERE clause")
	}

	for col, val := range whereData {
		if r.isMetadataField(col) {
			continue
		}
		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", QuoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = :%d", QuoteIdentifier(col), paramNum))
			values = append(values, val)
			paramNum++
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no where clause could be built")
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		QuoteIdentifier(event.TableName),
		strings.Join(whereClauses, " AND "),
	)

	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "apply_cdc_delete", err)
	}

	return nil
}

// applyCDCTruncate handles TRUNCATE operations for Oracle.
func (r *ReplicationOps) applyCDCTruncate(ctx context.Context, event *adapter.CDCEvent) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", QuoteIdentifier(event.TableName))

	_, err := r.conn.db.ExecContext(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "apply_cdc_truncate", err)
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

			case "trim": // Handle trim locally
				if str, ok := sourceValue.(string); ok {
					transformedValue = strings.TrimSpace(str)
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

// isMetadataField checks if a field is metadata.
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
	metadataFields := map[string]bool{
		"timestamp":   true,
		"operation":   true,
		"table_name":  true,
		"schema_name": true,
		"owner":       true,
		"database_id": true,
		"scn":         true,
		"lsn":         true,
		"_redb_meta":  true,
	}
	return metadataFields[strings.ToLower(fieldName)]
}
