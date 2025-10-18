package mssql

import (
	"context"
	"encoding/hex"
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

// ParseEvent converts a SQL Server CDC event to a standardized CDCEvent.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	event := &adapter.CDCEvent{
		Timestamp: time.Now(),
		Metadata:  make(map[string]interface{}),
	}

	// Extract operation type from __$operation field
	// 1 = delete, 2 = insert, 3 = update (before image), 4 = update (after image)
	var operationCode int
	if opCode, ok := rawEvent["__$operation"].(int64); ok {
		operationCode = int(opCode)
	} else if opCode, ok := rawEvent["__$operation"].(int); ok {
		operationCode = opCode
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing __$operation field")
	}

	// Map SQL Server CDC operation codes to CDCOperation
	switch operationCode {
	case 1:
		event.Operation = adapter.CDCDelete
	case 2:
		event.Operation = adapter.CDCInsert
	case 3, 4:
		event.Operation = adapter.CDCUpdate
	default:
		return nil, adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation_code", fmt.Sprintf("%d", operationCode))
	}

	// Extract table name
	if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing table_name field")
	}

	// Extract data - all columns except CDC metadata columns
	event.Data = make(map[string]interface{})
	for key, value := range rawEvent {
		if !r.isCDCMetadataField(key) && !r.isMetadataField(key) {
			event.Data[key] = value
		}
	}

	// For updates, if we have operation code 3 (before image), treat it as old data
	if operationCode == 3 {
		event.OldData = event.Data
		event.Data = nil
	}

	// Extract LSN
	if lsn, ok := rawEvent["__$start_lsn"].([]byte); ok {
		event.LSN = hex.EncodeToString(lsn)
	}

	// Extract sequence value
	if seqVal, ok := rawEvent["__$seqval"].([]byte); ok {
		event.Metadata["sequence_value"] = hex.EncodeToString(seqVal)
	}

	// Extract update mask
	if updateMask, ok := rawEvent["__$update_mask"].([]byte); ok {
		event.Metadata["update_mask"] = hex.EncodeToString(updateMask)
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to SQL Server.
// This handles INSERT, UPDATE, and DELETE operations.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "apply_cdc_event", err)
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
			dbcapabilities.SQLServer,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for SQL Server.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Build column names and value placeholders
	columns := make([]string, 0, len(event.Data))
	placeholders := make([]string, 0, len(event.Data))
	values := make([]interface{}, 0, len(event.Data))

	i := 1
	for col, val := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("@p%d", i))
		values = append(values, val)
		i++
	}

	if len(columns) == 0 {
		return nil // No actual data columns
	}

	// Build INSERT statement
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		r.quoteIdentifier(event.TableName),
		strings.Join(r.quoteIdentifiers(columns), ", "),
		strings.Join(placeholders, ", "),
	)

	// Execute the insert
	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for SQL Server.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Build SET clause
	setClauses := make([]string, 0, len(event.Data))
	values := make([]interface{}, 0)
	paramIdx := 1

	for col, val := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = @p%d", r.quoteIdentifier(col), paramIdx))
		values = append(values, val)
		paramIdx++
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
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", r.quoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = @p%d", r.quoteIdentifier(col), paramIdx))
			values = append(values, val)
			paramIdx++
		}
	}

	if len(whereClauses) == 0 {
		whereClauses = []string{"1=1"} // Fallback if no WHERE conditions
	}

	// Build UPDATE statement
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		r.quoteIdentifier(event.TableName),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	// Execute the update
	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "apply_cdc_update", err)
	}

	return nil
}

// applyCDCDelete handles DELETE operations for SQL Server.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
	// For DELETE, we need old data to identify which row to delete
	whereData := event.OldData
	if len(whereData) == 0 {
		// Try using current data as fallback
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify row for DELETE")
	}

	// Build WHERE clause
	whereClauses := make([]string, 0, len(whereData))
	values := make([]interface{}, 0)
	paramIdx := 1

	for col, val := range whereData {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}

		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", r.quoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = @p%d", r.quoteIdentifier(col), paramIdx))
			values = append(values, val)
			paramIdx++
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no WHERE conditions for DELETE")
	}

	// Build DELETE statement
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		r.quoteIdentifier(event.TableName),
		strings.Join(whereClauses, " AND "),
	)

	// Execute the delete
	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "apply_cdc_delete", err)
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

// Helper methods

// isCDCMetadataField checks if a field is a SQL Server CDC metadata field.
func (r *ReplicationOps) isCDCMetadataField(fieldName string) bool {
	cdcFields := map[string]bool{
		"__$start_lsn":   true,
		"__$end_lsn":     true,
		"__$seqval":      true,
		"__$operation":   true,
		"__$update_mask": true,
		"__$command_id":  true,
	}
	return cdcFields[fieldName]
}

// isMetadataField checks if a field name is a metadata field that should be skipped.
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
	metadataFields := map[string]bool{
		"message_type":   true,
		"raw_data_b64":   true,
		"data_length":    true,
		"database_id":    true,
		"timestamp":      true,
		"schema_name":    true,
		"operation":      true,
		"table_name":     true,
		"sequence_value": true,
		"update_mask":    true,
	}
	return metadataFields[fieldName]
}

// quoteIdentifier quotes a SQL Server identifier (table or column name).
func (r *ReplicationOps) quoteIdentifier(identifier string) string {
	// SQL Server uses square brackets for identifiers
	// Escape any closing brackets in the identifier
	escaped := strings.ReplaceAll(identifier, "]", "]]")
	return fmt.Sprintf("[%s]", escaped)
}

// quoteIdentifiers quotes multiple SQL Server identifiers.
func (r *ReplicationOps) quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = r.quoteIdentifier(id)
	}
	return quoted
}
