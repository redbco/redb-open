package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ParseEvent converts a MySQL-specific raw event to a standardized CDCEvent.
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
			dbcapabilities.MySQL,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing operation field")
	}

	// Extract table name
	if tableName, ok := rawEvent["table_name"].(string); ok {
		event.TableName = tableName
	} else {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.MySQL,
			"parse_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("error", "missing table_name field")
	}

	// Extract schema name (database name in MySQL)
	if schemaName, ok := rawEvent["schema_name"].(string); ok {
		event.SchemaName = schemaName
	} else if dbName, ok := rawEvent["database_name"].(string); ok {
		event.SchemaName = dbName
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
	if binlogFile, ok := rawEvent["binlog_file"].(string); ok {
		event.Metadata["binlog_file"] = binlogFile
	}
	if binlogPos, ok := rawEvent["binlog_position"]; ok {
		event.Metadata["binlog_position"] = binlogPos
		event.LSN = fmt.Sprintf("%v", binlogPos) // Use binlog position as LSN
	}
	if timestamp, ok := rawEvent["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
			event.Timestamp = t
		}
	}

	// Validate the event
	if err := event.Validate(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "parse_cdc_event", err)
	}

	return event, nil
}

// ApplyCDCEvent applies a standardized CDC event to MySQL.
// This handles INSERT, UPDATE, and DELETE operations.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	// Validate event
	if err := event.Validate(); err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "apply_cdc_event", err)
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
			dbcapabilities.MySQL,
			"apply_cdc_event",
			adapter.ErrInvalidData,
		).WithContext("operation", string(event.Operation))
	}
}

// applyCDCInsert handles INSERT operations for MySQL.
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MySQL,
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
		columns = append(columns, col)
		placeholders = append(placeholders, "?") // MySQL uses ? placeholders
		values = append(values, val)
	}

	if len(columns) == 0 {
		// No actual data columns found
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
	_, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "apply_cdc_insert", err)
	}

	return nil
}

// applyCDCUpdate handles UPDATE operations for MySQL.
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
	if len(event.Data) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MySQL,
			"apply_cdc_update",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to update")
	}

	// Build SET clause
	setClauses := make([]string, 0, len(event.Data))
	values := make([]interface{}, 0)

	for col, val := range event.Data {
		// Skip metadata fields
		if r.isMetadataField(col) {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", r.quoteIdentifier(col)))
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
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", r.quoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", r.quoteIdentifier(col)))
			values = append(values, val)
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
	result, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "apply_cdc_update", err)
	}

	// Log warning if no rows were affected
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		// This is not an error, but might indicate the row was already updated or deleted
	}

	return nil
}

// applyCDCDelete handles DELETE operations for MySQL.
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
	// For DELETE, we need old data to identify which row to delete
	whereData := event.OldData
	if len(whereData) == 0 {
		// Try using current data as fallback
		whereData = event.Data
	}

	if len(whereData) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MySQL,
			"apply_cdc_delete",
			adapter.ErrInvalidData,
		).WithContext("error", "no data to identify row for DELETE")
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
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", r.quoteIdentifier(col)))
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", r.quoteIdentifier(col)))
			values = append(values, val)
		}
	}

	if len(whereClauses) == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.MySQL,
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
	result, err := r.conn.db.ExecContext(ctx, query, values...)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "apply_cdc_delete", err)
	}

	// Log warning if no rows were affected
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		// Row may have already been deleted
	}

	return nil
}

// applyCDCTruncate handles TRUNCATE operations for MySQL.
func (r *ReplicationOps) applyCDCTruncate(ctx context.Context, event *adapter.CDCEvent) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", r.quoteIdentifier(event.TableName))

	_, err := r.conn.db.ExecContext(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MySQL, "apply_cdc_truncate", err)
	}

	return nil
}

// TransformData applies transformation rules to event data.
// This is a basic implementation that handles common transformation types.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule) (map[string]interface{}, error) {
	if len(rules) == 0 {
		return data, nil
	}

	transformedData := make(map[string]interface{})

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

		switch rule.TransformationType {
		case adapter.TransformDirect:
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
			// Unknown transformation type - pass through
			transformedValue = sourceValue
		}

		transformedData[rule.TargetColumn] = transformedValue
	}

	// If no transformations were applied, return original data
	if len(transformedData) == 0 {
		return data, nil
	}

	return transformedData, nil
}

// Helper methods

// isMetadataField checks if a field name is a metadata field that should be skipped.
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
	metadataFields := map[string]bool{
		"message_type":    true,
		"raw_data_b64":    true,
		"data_length":     true,
		"is_update":       true,
		"database_id":     true,
		"binlog_file":     true,
		"binlog_position": true,
		"timestamp":       true,
		"schema_name":     true,
		"database_name":   true,
		"operation":       true,
		"table_name":      true,
	}
	return metadataFields[fieldName]
}

// quoteIdentifier quotes a MySQL identifier (table or column name).
func (r *ReplicationOps) quoteIdentifier(identifier string) string {
	// MySQL uses backticks for identifiers
	// Escape any backticks in the identifier
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}

// quoteIdentifiers quotes multiple MySQL identifiers.
func (r *ReplicationOps) quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = r.quoteIdentifier(id)
	}
	return quoted
}

// GetDB returns the underlying database connection (for internal use).
func (r *ReplicationOps) GetDB() *sql.DB {
	return r.conn.db
}
