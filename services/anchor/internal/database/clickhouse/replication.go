package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource sets up a replication source
// Note: Clickhouse replication works differently from PostgreSQL
// This implementation uses materialized views to track changes
func CreateReplicationSource(conn ClickhouseConn, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*ClickhouseReplicationSourceDetails, error) {
	ctx := context.Background()

	// Generate unique names for the materialized view and buffer table
	bufferTableName := fmt.Sprintf("_buffer_%s_%s", tableName, common.GenerateUniqueID())
	mvName := fmt.Sprintf("_mv_%s_%s", tableName, common.GenerateUniqueID())

	// Get columns for the source table
	columns, err := getColumns(conn, tableName)
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %v", err)
	}

	// Create a buffer table with the same structure plus operation type
	createBufferSQL := fmt.Sprintf("CREATE TABLE %s (operation String, %s) ENGINE = Buffer(currentDatabase(), %s, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
		bufferTableName, getColumnDefinitions(conn, tableName), bufferTableName)

	err = conn.Exec(ctx, createBufferSQL)
	if err != nil {
		return nil, fmt.Errorf("error creating buffer table: %v", err)
	}

	// Create triggers for INSERT, UPDATE, DELETE operations
	// Note: Clickhouse doesn't support traditional triggers
	// We'll use materialized views instead to track changes

	// Create a materialized view to capture inserts
	createMVSQL := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW %s TO %s AS
		SELECT 'INSERT' as operation, %s
		FROM %s
	`, mvName, bufferTableName, strings.Join(columns, ", "), tableName)

	err = conn.Exec(ctx, createMVSQL)
	if err != nil {
		// Clean up buffer table if MV creation fails
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", bufferTableName))
		return nil, fmt.Errorf("error creating materialized view: %v", err)
	}

	details := &ClickhouseReplicationSourceDetails{
		TableName:  tableName,
		DatabaseID: databaseID,
	}

	// Start listening for replication events
	go listenForReplicationEvents(conn, bufferTableName, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(conn ClickhouseConn, details *ClickhouseReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// For Clickhouse, we need to recreate the replication setup
	// as there's no direct equivalent to PostgreSQL's replication slots

	// First, clean up any existing replication setup for this table
	cleanupReplication(conn, details.TableName)

	// Then create a new replication source
	_, err := CreateReplicationSource(conn, details.TableName, details.DatabaseID, eventHandler)
	if err != nil {
		return fmt.Errorf("error recreating replication source: %v", err)
	}

	return nil
}

func cleanupReplication(conn ClickhouseConn, tableName string) {
	ctx := context.Background()

	// Find and drop materialized views related to this table
	query := `
		SELECT name 
		FROM system.tables 
		WHERE database = currentDatabase() 
		AND engine = 'MaterializedView' 
		AND name LIKE ?
	`

	rows, err := conn.Query(ctx, query, fmt.Sprintf("_mv_%s_%%", tableName))
	if err != nil {
		log.Printf("Error finding materialized views: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var mvName string
		if err := rows.Scan(&mvName); err != nil {
			log.Printf("Error scanning materialized view name: %v", err)
			continue
		}

		err = conn.Exec(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s", mvName))
		if err != nil {
			log.Printf("Error dropping materialized view %s: %v", mvName, err)
		}
	}

	// Find and drop buffer tables related to this table
	query = `
		SELECT name 
		FROM system.tables 
		WHERE database = currentDatabase() 
		AND engine = 'Buffer' 
		AND name LIKE ?
	`

	rows, err = conn.Query(ctx, query, fmt.Sprintf("_buffer_%s_%%", tableName))
	if err != nil {
		log.Printf("Error finding buffer tables: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var bufferName string
		if err := rows.Scan(&bufferName); err != nil {
			log.Printf("Error scanning buffer table name: %v", err)
			continue
		}

		err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", bufferName))
		if err != nil {
			log.Printf("Error dropping buffer table %s: %v", bufferName, err)
		}
	}
}

func listenForReplicationEvents(conn ClickhouseConn, bufferTableName string, eventHandler func(map[string]interface{})) {
	ctx := context.Background()

	for {
		// Query the buffer table for changes
		query := fmt.Sprintf("SELECT * FROM %s", bufferTableName)
		rows, err := conn.Query(ctx, query)
		if err != nil {
			log.Printf("Error querying buffer table: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Process each change
		columnNames := rows.Columns()
		for rows.Next() {
			// Create a slice to hold the values
			values := make([]interface{}, len(columnNames))
			for i := range values {
				values[i] = new(interface{})
			}

			if err := rows.Scan(values...); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}

			// Extract operation type (first column)
			operation := ""
			if opVal, ok := values[0].(*interface{}); ok && *opVal != nil {
				operation = fmt.Sprintf("%v", *opVal)
			}

			// Build data map
			data := make(map[string]interface{})
			for i := 1; i < len(columnNames); i++ {
				if val, ok := values[i].(*interface{}); ok && *val != nil {
					data[columnNames[i]] = *val
				}
			}

			// Create event
			event := map[string]interface{}{
				"table":     bufferTableName,
				"operation": operation,
				"data":      data,
			}

			// Send event to handler
			eventHandler(event)
		}
		rows.Close()

		// Clear processed events from buffer table
		err = conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", bufferTableName))
		if err != nil {
			log.Printf("Error truncating buffer table: %v", err)
		}

		// Wait before checking for new events
		time.Sleep(1 * time.Second)
	}
}

func getColumnDefinitions(conn ClickhouseConn, tableName string) string {
	ctx := context.Background()

	query := `
		SELECT 
			name, 
			type,
			default_expression
		FROM system.columns 
		WHERE database = currentDatabase() 
		AND table = ?
		ORDER BY position
	`

	rows, err := conn.Query(ctx, query, tableName)
	if err != nil {
		log.Printf("Error getting column definitions: %v", err)
		return ""
	}
	defer rows.Close()

	var columnDefs []string
	for rows.Next() {
		var name, dataType string
		var defaultExpr sql.NullString

		if err := rows.Scan(&name, &dataType, &defaultExpr); err != nil {
			log.Printf("Error scanning column definition: %v", err)
			continue
		}

		colDef := fmt.Sprintf("%s %s", name, dataType)
		if defaultExpr.Valid && defaultExpr.String != "" {
			colDef += fmt.Sprintf(" DEFAULT %s", defaultExpr.String)
		}

		columnDefs = append(columnDefs, colDef)
	}

	return strings.Join(columnDefs, ", ")
}
