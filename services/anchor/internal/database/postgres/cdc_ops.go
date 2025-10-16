package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
)

// ParseEvent converts a PostgreSQL-specific raw event to a standardized CDCEvent.
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
			dbcapabilities.PostgreSQL,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operation field")
	}

	// Extract table name
	if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.PostgreSQL,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing table_name field")
	}

	// Extract schema name (optional)
	if schemaName, ok := rawEvent["schema_name"].(string); ok {
		event.SchemaName = schemaName
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
	if slotName, ok := rawEvent["slot_name"].(string); ok {
		event.Metadata["slot_name"] = slotName
		event.LSN = slotName // Use slot name as LSN for now
	}
	if timestamp, ok := rawEvent["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
			event.Timestamp = t
		}
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to PostgreSQL.
// This handles INSERT, UPDATE, and DELETE operations.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "apply_cdc_event", err)
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
			dbcapabilities.PostgreSQL,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.PostgreSQL,
			"apply_cdc_insert",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to insert")
	}

	// Build column names and placeholders
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
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		values = append(values, val)
		i++
	}

	if len(columns) == 0 {
		// No actual data columns found - this might be a protocol issue
		return nil // Skip silently
	}

	// Build INSERT statement
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		r.quoteIdentifier(event.TableName),
		strings.Join(r.quoteIdentifiers(columns), ", "),
		strings.Join(placeholders, ", "),
	)

	// Execute the insert
	_, err := r.conn.pool.Exec(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.PostgreSQL,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Build SET clause
	setClauses := make([]string, 0, len(event.Data))
	values := make([]interface{}, 0, len(event.Data))
	paramIdx := 1

	for col, val := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", r.quoteIdentifier(col), paramIdx))
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
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", r.quoteIdentifier(col), paramIdx))
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
	result, err := r.conn.pool.Exec(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "apply_cdc_update", err)
	}

	// Log warning if no rows were affected
	if result.RowsAffected() == 0 {
		// This is not an error, but might indicate the row was already updated or deleted
		// The caller can check statistics for this
	}

	return nil
}

// applyCDCDelete handles DELETE operations.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
	// For DELETE, we need old data to identify which row to delete
	whereData := event.OldData
	if len(whereData) == 0 {
		// Try using current data as fallback
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.PostgreSQL,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify row for DELETE")
	}

	// Build WHERE clause
	whereClauses := make([]string, 0, len(whereData))
	values := make([]interface{}, 0, len(whereData))
	paramIdx := 1

	for col, val := range whereData {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}

		if val == nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", r.quoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", r.quoteIdentifier(col), paramIdx))
			values = append(values, val)
			paramIdx++
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.PostgreSQL,
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
	result, err := r.conn.pool.Exec(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "apply_cdc_delete", err)
	}

	// Log warning if no rows were affected
	if result.RowsAffected() == 0 {
		// Row may have already been deleted
	}

	return nil
}

// applyCDCTruncate handles TRUNCATE operations.
func (r *ReplicationOps) applyCDCTruncate(ctx context.Context, event *adapter.CDCEvent) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", r.quoteIdentifier(event.TableName))

	_, err := r.conn.pool.Exec(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "apply_cdc_truncate", err)
	}

	return nil
}

// TransformData applies transformation rules to event data.
// This implementation handles basic transformations and calls the transformation service for custom transformations.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	if len(rules) == 0 {
		return data, nil
	}

	// Log the transformation process
	// Note: We don't have direct access to logger here, but we can use fmt.Printf for debugging
	// In production, consider passing logger or using a global logger

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

		// Check if there's a custom transformation name (e.g., "reverse", "base64_encode")
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
		"is_update":    true,
		"database_id":  true,
		"slot_name":    true,
		"timestamp":    true,
		"schema_name":  true,
		"operation":    true,
		"table_name":   true,
	}
	return metadataFields[fieldName]
}

// quoteIdentifier quotes a PostgreSQL identifier (table or column name).
func (r *ReplicationOps) quoteIdentifier(identifier string) string {
	// PostgreSQL uses double quotes for identifiers
	// Escape any double quotes in the identifier
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// quoteIdentifiers quotes multiple PostgreSQL identifiers.
func (r *ReplicationOps) quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = r.quoteIdentifier(id)
	}
	return quoted
}

// GetPool returns the underlying connection pool (for internal use).
func (r *ReplicationOps) GetPool() *pgxpool.Pool {
	return r.conn.pool
}
