package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// SnowflakeStreamDetails contains information about a Snowflake stream
type SnowflakeStreamDetails struct {
	StreamName string `json:"stream_name"`
	TableName  string `json:"table_name"`
	DatabaseID string `json:"database_id"`
	SchemaName string `json:"schema_name"`
	Offset     string `json:"offset,omitempty"`
	TaskName   string `json:"task_name,omitempty"`
	TaskState  string `json:"task_state,omitempty"`
}

// CreateReplicationSource sets up a replication source using Snowflake streams
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*SnowflakeStreamDetails, error) {
	// Generate unique names for stream and task
	streamName := fmt.Sprintf("stream_%s_%s", databaseID, common.GenerateUniqueID())
	taskName := fmt.Sprintf("task_%s_%s", databaseID, common.GenerateUniqueID())

	// Get current schema
	var schemaName string
	err := db.QueryRow("SELECT CURRENT_SCHEMA()").Scan(&schemaName)
	if err != nil {
		return nil, fmt.Errorf("error getting current schema: %v", err)
	}

	// Create stream on the table
	streamSQL := fmt.Sprintf("CREATE STREAM %s ON TABLE %s SHOW_INITIAL_ROWS = TRUE",
		quoteIdentifier(streamName), quoteIdentifier(tableName))
	_, err = db.Exec(streamSQL)
	if err != nil {
		return nil, fmt.Errorf("error creating stream: %v", err)
	}

	details := &SnowflakeStreamDetails{
		StreamName: streamName,
		TableName:  tableName,
		DatabaseID: databaseID,
		SchemaName: schemaName,
		TaskName:   taskName,
	}

	// Start listening for stream changes
	go listenForStreamChanges(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing stream
func ReconnectToReplicationSource(db *sql.DB, details *SnowflakeStreamDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the stream still exists
	var exists bool
	err := db.QueryRow(`
		SELECT COUNT(*) > 0 
		FROM INFORMATION_SCHEMA.STREAMS 
		WHERE STREAM_NAME = ? 
		AND STREAM_SCHEMA = ?`,
		details.StreamName, details.SchemaName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking stream existence: %v", err)
	}

	if !exists {
		return fmt.Errorf("stream %s does not exist in schema %s", details.StreamName, details.SchemaName)
	}

	// Start listening for stream changes
	go listenForStreamChanges(db, details, eventHandler)

	return nil
}

func listenForStreamChanges(db *sql.DB, details *SnowflakeStreamDetails, eventHandler func(map[string]interface{})) {
	// Set up polling interval
	pollInterval := 5 * time.Second
	ctx := context.Background()

	log.Printf("Starting to listen for changes on stream %s for table %s", details.StreamName, details.TableName)

	for {
		changes, offset, err := getStreamChanges(ctx, db, details)
		if err != nil {
			log.Printf("Error getting stream changes: %v", err)
			time.Sleep(pollInterval)
			continue
		}

		// Update the offset if we got changes
		if offset != "" {
			details.Offset = offset
		}

		// Process changes
		for _, change := range changes {
			event := map[string]interface{}{
				"table":     details.TableName,
				"operation": change.Operation,
				"data":      change.Data,
				"metadata":  change.Metadata,
			}
			eventHandler(event)
		}

		// Wait before polling again
		time.Sleep(pollInterval)
	}
}

func getStreamChanges(ctx context.Context, db *sql.DB, details *SnowflakeStreamDetails) ([]SnowflakeStreamChange, string, error) {
	// Build query to consume stream data
	query := fmt.Sprintf(`
		SELECT 
			METADATA$ACTION, 
			METADATA$ISUPDATE,
			METADATA$ROW_ID,
			METADATA$TIMESTAMP,
			*
		FROM %s.%s
	`, quoteIdentifier(details.SchemaName), quoteIdentifier(details.StreamName))

	// Add offset condition if we have one
	if details.Offset != "" {
		query += fmt.Sprintf(" WHERE METADATA$TIMESTAMP > TO_TIMESTAMP_LTZ('%s')", details.Offset)
	}

	// Add order by to ensure consistent processing
	query += " ORDER BY METADATA$TIMESTAMP"

	// Execute query
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("error querying stream: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, "", fmt.Errorf("error getting column names: %v", err)
	}

	var changes []SnowflakeStreamChange
	var lastTimestamp string

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the slice
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, lastTimestamp, fmt.Errorf("error scanning row: %v", err)
		}

		// Extract metadata and data
		var action, isUpdate, rowID, timestamp interface{}
		data := make(map[string]interface{})
		metadata := make(map[string]interface{})

		for i, col := range columns {
			if col == "METADATA$ACTION" {
				action = values[i]
				metadata["action"] = action
			} else if col == "METADATA$ISUPDATE" {
				isUpdate = values[i]
				metadata["is_update"] = isUpdate
			} else if col == "METADATA$ROW_ID" {
				rowID = values[i]
				metadata["row_id"] = rowID
			} else if col == "METADATA$TIMESTAMP" {
				timestamp = values[i]
				metadata["timestamp"] = timestamp
				// Update lastTimestamp for offset tracking
				if ts, ok := timestamp.(string); ok {
					lastTimestamp = ts
				}
			} else if !strings.HasPrefix(col, "METADATA$") {
				// Regular data column
				data[col] = values[i]
			}
		}

		// Determine operation type
		operation := "UNKNOWN"
		if actionStr, ok := action.(string); ok {
			switch actionStr {
			case "INSERT":
				operation = "INSERT"
			case "DELETE":
				operation = "DELETE"
			default:
				if isUpdateBool, ok := isUpdate.(bool); ok && isUpdateBool {
					operation = "UPDATE"
				}
			}
		}

		change := SnowflakeStreamChange{
			Operation: operation,
			Data:      data,
			Metadata:  metadata,
		}

		changes = append(changes, change)
	}

	if err := rows.Err(); err != nil {
		return changes, lastTimestamp, fmt.Errorf("error iterating rows: %v", err)
	}

	return changes, lastTimestamp, nil
}

// CreateTaskForStream creates a Snowflake task to process stream changes
func CreateTaskForStream(db *sql.DB, details *SnowflakeStreamDetails, targetTable string) error {
	// Create a task that processes the stream and inserts data into a target table
	taskSQL := fmt.Sprintf(`
		CREATE OR REPLACE TASK %s
		WAREHOUSE = CURRENT_WAREHOUSE()
		SCHEDULE = '1 minute'
		WHEN SYSTEM$STREAM_HAS_DATA('%s')
		AS
		INSERT INTO %s
		SELECT * FROM %s
	`,
		quoteIdentifier(details.TaskName),
		quoteIdentifier(details.StreamName),
		quoteIdentifier(targetTable),
		quoteIdentifier(details.StreamName))

	_, err := db.Exec(taskSQL)
	if err != nil {
		return fmt.Errorf("error creating task: %v", err)
	}

	// Resume the task (tasks are created in suspended state)
	_, err = db.Exec(fmt.Sprintf("ALTER TASK %s RESUME", quoteIdentifier(details.TaskName)))
	if err != nil {
		return fmt.Errorf("error resuming task: %v", err)
	}

	details.TaskState = "RUNNING"
	return nil
}

// PauseReplicationTask pauses a Snowflake task
func PauseReplicationTask(db *sql.DB, details *SnowflakeStreamDetails) error {
	if details.TaskName == "" {
		return fmt.Errorf("no task name provided")
	}

	_, err := db.Exec(fmt.Sprintf("ALTER TASK %s SUSPEND", quoteIdentifier(details.TaskName)))
	if err != nil {
		return fmt.Errorf("error suspending task: %v", err)
	}

	details.TaskState = "SUSPENDED"
	return nil
}

// ResumeReplicationTask resumes a Snowflake task
func ResumeReplicationTask(db *sql.DB, details *SnowflakeStreamDetails) error {
	if details.TaskName == "" {
		return fmt.Errorf("no task name provided")
	}

	_, err := db.Exec(fmt.Sprintf("ALTER TASK %s RESUME", quoteIdentifier(details.TaskName)))
	if err != nil {
		return fmt.Errorf("error resuming task: %v", err)
	}

	details.TaskState = "RUNNING"
	return nil
}

// CleanupReplicationSource removes the stream and associated task
func CleanupReplicationSource(db *sql.DB, details *SnowflakeStreamDetails) error {
	// Drop the task if it exists
	if details.TaskName != "" {
		_, err := db.Exec(fmt.Sprintf("DROP TASK IF EXISTS %s", quoteIdentifier(details.TaskName)))
		if err != nil {
			return fmt.Errorf("error dropping task: %v", err)
		}
	}

	// Drop the stream
	_, err := db.Exec(fmt.Sprintf("DROP STREAM IF EXISTS %s", quoteIdentifier(details.StreamName)))
	if err != nil {
		return fmt.Errorf("error dropping stream: %v", err)
	}

	return nil
}

// GetReplicationStatus returns the status of a replication stream
func GetReplicationStatus(db *sql.DB, details *SnowflakeStreamDetails) (map[string]interface{}, error) {
	status := make(map[string]interface{})

	// Get stream information
	var stalenessSecs, recordCount int64
	var offsetTimestamp string

	err := db.QueryRow(`
		SELECT 
			SYSTEM$STREAM_GET_TABLE_TIMESTAMP_OFFSET('%s') as OFFSET_TIMESTAMP,
			SYSTEM$STREAM_GET_TABLE_STALENESS('%s') as STALENESS_SECONDS,
			COUNT(*) as RECORD_COUNT
		FROM %s
	`,
		details.StreamName,
		details.StreamName,
		details.StreamName).Scan(&offsetTimestamp, &stalenessSecs, &recordCount)

	if err != nil {
		// If the query fails, try a simpler approach
		err = db.QueryRow(`SELECT COUNT(*) FROM ` + quoteIdentifier(details.StreamName)).Scan(&recordCount)
		if err != nil {
			return status, fmt.Errorf("error getting stream status: %v", err)
		}
	}

	status["stream_name"] = details.StreamName
	status["table_name"] = details.TableName
	status["record_count"] = recordCount

	if offsetTimestamp != "" {
		status["offset_timestamp"] = offsetTimestamp
	}

	if stalenessSecs > 0 {
		status["staleness_seconds"] = stalenessSecs
	}

	// Get task information if a task exists
	if details.TaskName != "" {
		var taskState, lastRunTime string
		var errorCount int

		err = db.QueryRow(`
			SELECT 
				STATE,
				IFNULL(LAST_COMPLETED_TIME::STRING, '') as LAST_RUN_TIME,
				ERROR_COUNT
			FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
				TASK_NAME => ?
			))
			ORDER BY SCHEDULED_TIME DESC
			LIMIT 1
		`, details.TaskName).Scan(&taskState, &lastRunTime, &errorCount)

		if err == nil {
			status["task_name"] = details.TaskName
			status["task_state"] = taskState
			if lastRunTime != "" {
				status["last_run_time"] = lastRunTime
			}
			status["error_count"] = errorCount
		}
	}

	return status, nil
}

// ConsumeStreamChanges consumes all changes from a stream and returns them
func ConsumeStreamChanges(db *sql.DB, details *SnowflakeStreamDetails) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT * FROM %s`, quoteIdentifier(details.StreamName))

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying stream: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names: %v", err)
	}

	var changes []map[string]interface{}

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the slice
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// Create a map for this row
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		changes = append(changes, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return changes, nil
}

// SerializeChanges converts a slice of changes to JSON
func SerializeChanges(changes []map[string]interface{}) (string, error) {
	jsonData, err := json.Marshal(changes)
	if err != nil {
		return "", fmt.Errorf("error serializing changes: %v", err)
	}
	return string(jsonData), nil
}

// DeserializeChanges converts JSON to a slice of changes
func DeserializeChanges(jsonData string) ([]map[string]interface{}, error) {
	var changes []map[string]interface{}
	err := json.Unmarshal([]byte(jsonData), &changes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing changes: %v", err)
	}
	return changes, nil
}
