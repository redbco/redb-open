package mariadb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource creates a replication source for a MariaDB table
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*MariaDBReplicationSourceDetails, error) {
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
	details := &MariaDBReplicationSourceDetails{
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
func ReconnectToReplicationSource(db *sql.DB, details *MariaDBReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
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

// listenForReplicationEvents listens for replication events from a MariaDB table
func listenForReplicationEvents(db *sql.DB, details *MariaDBReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	// In a real implementation, this would use a MariaDB binlog client library
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
func getReplicationChanges(db *sql.DB, tableName string, since time.Time) ([]MariaDBReplicationChange, error) {
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
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("error checking for timestamp column: %w", err)
	}

	var changes []MariaDBReplicationChange

	// If we have a timestamp column, use it to get changes
	if timestampColumn != "" {
		// Get rows updated since last check
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ?",
			common.QuoteIdentifier(tableName),
			common.QuoteIdentifier(timestampColumn))

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
			changes = append(changes, MariaDBReplicationChange{
				Operation: "UPDATE", // Assume update for simplicity
				Data:      rowMap,
			})
		}
	}

	return changes, nil
}

// SetupReplicationSlave sets up a MariaDB server as a replication slave
func SetupReplicationSlave(db *sql.DB, masterHost string, masterPort int, masterUser string, masterPassword string, masterLogFile string, masterLogPos uint32) error {
	// Stop any existing replication
	_, err := db.Exec("STOP SLAVE")
	if err != nil {
		// Ignore error if slave was not running
		fmt.Printf("Warning: Could not stop slave: %v\n", err)
	}

	// Configure replication
	_, err = db.Exec(`
		CHANGE MASTER TO
		MASTER_HOST = ?,
		MASTER_PORT = ?,
		MASTER_USER = ?,
		MASTER_PASSWORD = ?,
		MASTER_LOG_FILE = ?,
		MASTER_LOG_POS = ?`,
		masterHost,
		masterPort,
		masterUser,
		masterPassword,
		masterLogFile,
		masterLogPos)
	if err != nil {
		return fmt.Errorf("error configuring replication: %w", err)
	}

	// Start replication
	_, err = db.Exec("START SLAVE")
	if err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}

	return nil
}

// GetReplicationStatus gets the current status of replication
func GetReplicationStatus(db *sql.DB) (map[string]interface{}, error) {
	rows, err := db.Query("SHOW SLAVE STATUS")
	if err != nil {
		return nil, fmt.Errorf("error getting replication status: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting column names: %w", err)
	}

	// Check if we have any rows
	if !rows.Next() {
		return nil, fmt.Errorf("no replication status available")
	}

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

	// Create a map for the status
	status := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]
		// Handle NULL values
		if val == nil {
			status[col] = nil
			continue
		}

		// Convert bytes to string for text types
		switch v := val.(type) {
		case []byte:
			status[col] = string(v)
		default:
			status[col] = v
		}
	}

	return status, nil
}

// EnableGTIDReplication enables GTID-based replication for MariaDB
func EnableGTIDReplication(db *sql.DB) error {
	// Check if GTID is supported
	var gtidMode string
	err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode)
	if err != nil {
		// MariaDB uses a different variable for GTID
		err = db.QueryRow("SELECT @@GLOBAL.gtid_domain_id").Scan(&gtidMode)
		if err != nil {
			return fmt.Errorf("GTID replication not supported: %w", err)
		}
	}

	// Stop slave
	_, err = db.Exec("STOP SLAVE")
	if err != nil {
		// Ignore error if slave was not running
		fmt.Printf("Warning: Could not stop slave: %v\n", err)
	}

	// Enable GTID
	_, err = db.Exec("SET GLOBAL gtid_slave_pos = ''")
	if err != nil {
		return fmt.Errorf("error setting GTID slave position: %w", err)
	}

	// Configure replication to use GTID
	_, err = db.Exec("CHANGE MASTER TO MASTER_USE_GTID = slave_pos")
	if err != nil {
		return fmt.Errorf("error configuring GTID replication: %w", err)
	}

	// Start slave
	_, err = db.Exec("START SLAVE")
	if err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}

	return nil
}

// CreateReplicationFilter creates a filter for replication to include or exclude specific tables
func CreateReplicationFilter(db *sql.DB, includeTables []string, excludeTables []string) error {
	// Stop slave
	_, err := db.Exec("STOP SLAVE")
	if err != nil {
		// Ignore error if slave was not running
		fmt.Printf("Warning: Could not stop slave: %v\n", err)
	}

	// Set replication filters
	if len(includeTables) > 0 {
		// Create comma-separated list of tables
		tableList := strings.Join(includeTables, ",")
		_, err = db.Exec(fmt.Sprintf("CHANGE MASTER TO REPLICATE_DO_TABLE = '%s'", tableList))
		if err != nil {
			return fmt.Errorf("error setting replication include filter: %w", err)
		}
	}

	if len(excludeTables) > 0 {
		// Create comma-separated list of tables
		tableList := strings.Join(excludeTables, ",")
		_, err = db.Exec(fmt.Sprintf("CHANGE MASTER TO REPLICATE_IGNORE_TABLE = '%s'", tableList))
		if err != nil {
			return fmt.Errorf("error setting replication exclude filter: %w", err)
		}
	}

	// Start slave
	_, err = db.Exec("START SLAVE")
	if err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}

	return nil
}
