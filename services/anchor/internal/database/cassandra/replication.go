package cassandra

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// CreateReplicationSource sets up a replication source
// Note: Cassandra doesn't have built-in logical replication like PostgreSQL,
// so this implementation uses CDC (Change Data Capture) if available or polling as a fallback
func CreateReplicationSource(session *gocql.Session, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*CassandraReplicationSourceDetails, error) {
	// Extract keyspace and table
	parts := strings.Split(tableName, ".")
	var keyspace, table string
	if len(parts) == 2 {
		keyspace = parts[0]
		table = parts[1]
	} else {
		keyspace = GetKeyspace(session)
		table = tableName
	}

	if keyspace == "" {
		return nil, fmt.Errorf("keyspace not specified and no default keyspace in session")
	}

	details := &CassandraReplicationSourceDetails{
		Keyspace:   keyspace,
		TableName:  table,
		DatabaseID: databaseID,
	}

	// Start listening for changes
	go listenForChanges(session, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(session *gocql.Session, details *CassandraReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the table still exists
	var count int
	err := session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		details.Keyspace, details.TableName).Scan(&count)
	if err != nil {
		return fmt.Errorf("error checking table existence: %v", err)
	}

	if count == 0 {
		return fmt.Errorf("table %s.%s does not exist", details.Keyspace, details.TableName)
	}

	// Start listening for changes
	go listenForChanges(session, details, eventHandler)

	return nil
}

func listenForChanges(session *gocql.Session, details *CassandraReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	// Try to use CDC if available
	if hasCDCSupport(session) {
		listenWithCDC(session, details, eventHandler)
	} else {
		// Fallback to polling
		listenWithPolling(session, details, eventHandler)
	}
}

func hasCDCSupport(session *gocql.Session) bool {
	// Check if CDC is supported (DSE 6.0+ or Cassandra with CDC plugin)
	var version string
	err := session.Query("SELECT release_version FROM system.local").Scan(&version)
	if err != nil {
		return false
	}

	// Check for DSE (DataStax Enterprise) which has CDC support
	return strings.Contains(strings.ToLower(version), "dse")
}

func listenWithCDC(session *gocql.Session, details *CassandraReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	log.Printf("Starting CDC listener for %s.%s", details.Keyspace, details.TableName)

	// This is a simplified implementation
	// In a real implementation, you would:
	// 1. Enable CDC on the table if not already enabled
	// 2. Set up a CDC consumer to read from the CDC log
	// 3. Process CDC events and convert them to the expected format

	// For now, we'll just use polling as a placeholder
	listenWithPolling(session, details, eventHandler)
}

func listenWithPolling(session *gocql.Session, details *CassandraReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	log.Printf("Starting polling listener for %s.%s", details.Keyspace, details.TableName)

	// Get primary key columns to track changes
	var primaryKey []string
	iter := session.Query(`
		SELECT column_name FROM system_schema.columns 
		WHERE keyspace_name = ? AND table_name = ? AND kind IN ('partition_key', 'clustering')
		ORDER BY position`,
		details.Keyspace, details.TableName).Iter()

	var pkColumn string
	for iter.Scan(&pkColumn) {
		primaryKey = append(primaryKey, pkColumn)
	}
	if err := iter.Close(); err != nil {
		log.Printf("Error getting primary key: %v", err)
		return
	}

	// Get all columns
	var columns []string
	iter = session.Query(`
		SELECT column_name FROM system_schema.columns 
		WHERE keyspace_name = ? AND table_name = ?`,
		details.Keyspace, details.TableName).Iter()

	var columnName string
	for iter.Scan(&columnName) {
		columns = append(columns, columnName)
	}
	if err := iter.Close(); err != nil {
		log.Printf("Error getting columns: %v", err)
		return
	}

	// Use the columns we got from the query
	columnNames := columns

	// Initial state - fetch all rows
	currentState, err := fetchAllRows(session, details.Keyspace, details.TableName, columnNames, primaryKey)
	if err != nil {
		log.Printf("Error fetching initial state: %v", err)
		return
	}

	// Polling loop
	for {
		time.Sleep(5 * time.Second)

		// Fetch current state
		newState, err := fetchAllRows(session, details.Keyspace, details.TableName, columnNames, primaryKey)
		if err != nil {
			log.Printf("Error fetching current state: %v", err)
			continue
		}

		// Compare states to detect changes
		changes := detectChanges(currentState, newState, primaryKey)

		// Process changes
		for _, change := range changes {
			event := map[string]interface{}{
				"table":     fmt.Sprintf("%s.%s", details.Keyspace, details.TableName),
				"operation": change.Operation,
				"data":      change.Data,
				"old_data":  change.OldData,
			}
			eventHandler(event)
		}

		// Update current state
		currentState = newState
	}
}

func fetchAllRows(session *gocql.Session, keyspace, table string, columns, primaryKey []string) (map[string]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(columns, ", "),
		QuoteIdentifier(keyspace),
		QuoteIdentifier(table))

	iter := session.Query(query).Iter()

	result := make(map[string]map[string]interface{})

	row := make(map[string]interface{})
	for iter.MapScan(row) {
		// Create a key from primary key columns
		key := createRowKey(row, primaryKey)

		// Create a copy of the row
		rowCopy := make(map[string]interface{})
		for k, v := range row {
			rowCopy[k] = ConvertCassandraValueToGo(v)
		}

		result[key] = rowCopy

		// Clear the map for the next iteration
		for k := range row {
			delete(row, k)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching rows: %v", err)
	}

	return result, nil
}

func createRowKey(row map[string]interface{}, primaryKey []string) string {
	var keyParts []string
	for _, col := range primaryKey {
		val := row[col]
		keyParts = append(keyParts, fmt.Sprintf("%v", val))
	}
	return strings.Join(keyParts, ":")
}

func detectChanges(oldState, newState map[string]map[string]interface{}, primaryKey []string) []CassandraReplicationChange {
	var changes []CassandraReplicationChange

	// Detect deletions
	for key, oldRow := range oldState {
		if _, exists := newState[key]; !exists {
			changes = append(changes, CassandraReplicationChange{
				Operation: "DELETE",
				OldData:   oldRow,
				Data:      nil,
			})
		}
	}

	// Detect inserts and updates
	for key, newRow := range newState {
		oldRow, exists := oldState[key]
		if !exists {
			// Insert
			changes = append(changes, CassandraReplicationChange{
				Operation: "INSERT",
				OldData:   nil,
				Data:      newRow,
			})
		} else if !mapsEqual(oldRow, newRow) {
			// Update
			changes = append(changes, CassandraReplicationChange{
				Operation: "UPDATE",
				OldData:   oldRow,
				Data:      newRow,
			})
		}
	}

	return changes
}

func mapsEqual(m1, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			return false
		}

		// Compare values
		if !reflect.DeepEqual(v1, v2) {
			return false
		}
	}

	return true
}
