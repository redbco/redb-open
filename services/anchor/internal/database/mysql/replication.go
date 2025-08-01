package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// CreateReplicationSource creates a replication source for a MySQL table
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*MySQLReplicationSourceDetails, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Check if the table exists
	var tableExists int
	err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tableName).Scan(&tableExists)
	if err != nil {
		return nil, fmt.Errorf("error checking if table exists: %w", err)
	}
	if tableExists == 0 {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get current binary log position
	var binlogFile string
	var binlogPosition uint32
	err = db.QueryRow("SHOW MASTER STATUS").Scan(&binlogFile, &binlogPosition, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error getting binary log position: %w", err)
	}

	// Create replication details
	details := &MySQLReplicationSourceDetails{
		BinlogFile:     binlogFile,
		BinlogPosition: binlogPosition,
		TableName:      tableName,
		DatabaseID:     databaseID,
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(db *sql.DB, details *MySQLReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	if details == nil {
		return fmt.Errorf("replication details cannot be nil")
	}

	// Check if the table exists
	var tableExists int
	err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", details.TableName).Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %w", err)
	}
	if tableExists == 0 {
		return fmt.Errorf("table %s does not exist", details.TableName)
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return nil
}

// listenForReplicationEvents listens for replication events from a MySQL table
func listenForReplicationEvents(db *sql.DB, details *MySQLReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	// In a real implementation, this would use a MySQL binlog client library
	// For this example, we'll simulate by polling the table for changes

	// Keep track of the last time we checked
	lastCheck := time.Now()

	for {
		// Sleep to avoid excessive polling
		time.Sleep(1 * time.Second)

		// Get changes since last check
		changes, err := getReplicationChanges(db, details.TableName, lastCheck)
		if err != nil {
			// Log error but continue
			fmt.Printf("Error getting replication changes: %v\n", err)
			continue
		}

		// Update last check time
		lastCheck = time.Now()

		// Process changes
		for _, change := range changes {
			// Call event handler with the change data
			if change.Operation == "INSERT" || change.Operation == "UPDATE" {
				eventHandler(change.Data)
			}
		}
	}
}

// getReplicationChanges gets changes to a table since a specific time
// This is a simplified implementation that polls for changes
func getReplicationChanges(db *sql.DB, tableName string, since time.Time) ([]MySQLReplicationChange, error) {
	// In a real implementation, this would use the binary log
	// For this example, we'll use a timestamp column if available

	// Check if the table has a timestamp column we can use
	var timestampColumn string
	query := `
		SELECT COLUMN_NAME 
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = DATABASE() 
		AND TABLE_NAME = ? 
		AND (DATA_TYPE = 'timestamp' OR DATA_TYPE = 'datetime')
		LIMIT 1`

	err := db.QueryRow(query, tableName).Scan(&timestampColumn)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("error checking for timestamp column: %w", err)
	}

	var changes []MySQLReplicationChange

	// If we have a timestamp column, use it to get changes
	if timestampColumn != "" {
		// Get rows updated since last check
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ?",
			QuoteIdentifier(tableName),
			QuoteIdentifier(timestampColumn))

		rows, err := db.Query(query, since.Format("2006-01-02 15:04:05"))
		if err != nil {
			return nil, fmt.Errorf("error querying for changes: %w", err)
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("error getting column names: %w", err)
		}

		// Process rows
		for rows.Next() {
			// Create a slice of interface{} to hold the values
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			// Scan the row into the slice of interface{}
			if err := rows.Scan(valuePtrs...); err != nil {
				return nil, fmt.Errorf("error scanning row: %w", err)
			}

			// Create a map for this row
			rowMap := make(map[string]interface{})
			for i, col := range columns {
				val := values[i]
				// Handle NULL values
				if val == nil {
					rowMap[col] = nil
					continue
				}

				// Convert bytes to string for text types
				switch v := val.(type) {
				case []byte:
					rowMap[col] = string(v)
				default:
					rowMap[col] = v
				}
			}

			// Add to changes
			changes = append(changes, MySQLReplicationChange{
				Operation: "UPDATE", // Assume update for simplicity
				Data:      rowMap,
			})
		}
	}

	return changes, nil
}
