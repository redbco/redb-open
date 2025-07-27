package mssql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateReplicationSource sets up a replication source
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*MSSQLReplicationSourceDetails, error) {
	// Generate unique names for publication
	pubName := fmt.Sprintf("pub_%s_%s", databaseID, common.GenerateUniqueID())
	subName := fmt.Sprintf("sub_%s_%s", databaseID, common.GenerateUniqueID())

	// Check if SQL Server has replication enabled
	var replicationEnabled int
	err := db.QueryRow(`
		SELECT COUNT(*) 
		FROM sys.databases 
		WHERE name = 'distribution'
	`).Scan(&replicationEnabled)
	if err != nil {
		return nil, fmt.Errorf("error checking replication status: %v", err)
	}

	if replicationEnabled == 0 {
		return nil, fmt.Errorf("SQL Server replication is not configured on this instance")
	}

	// Split schema and table name
	parts := strings.Split(tableName, ".")
	var schema, table string
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	} else {
		schema = "dbo"
		table = tableName
	}

	// Create publication
	_, err = db.Exec(fmt.Sprintf(`
		EXEC sp_replicationdboption 
			@dbname = DB_NAME(),
			@optname = 'publish',
			@value = 'true'
		
		EXEC sp_addpublication 
			@publication = '%s',
			@description = 'Replication for %s.%s',
			@sync_method = 'native',
			@allow_push = 'true',
			@allow_pull = 'true',
			@allow_anonymous = 'false',
			@enabled_for_internet = 'false',
			@independent_agent = 'true'
		
		EXEC sp_addarticle 
			@publication = '%s',
			@article = '%s',
			@source_owner = '%s',
			@source_table = '%s',
			@type = 'logbased',
			@description = 'Replication article for %s.%s',
			@creation_script = NULL,
			@pre_creation_cmd = 'drop',
			@schema_option = 0x000000000803509F
	`, pubName, schema, table, pubName, table, schema, table, schema, table))
	if err != nil {
		return nil, fmt.Errorf("error creating publication: %v", err)
	}

	details := &MSSQLReplicationSourceDetails{
		PublicationName: pubName,
		TableName:       fmt.Sprintf("%s.%s", schema, table),
		DatabaseID:      databaseID,
		SubscriptionID:  subName,
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(db *sql.DB, details *MSSQLReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the publication still exists
	var exists int
	err := db.QueryRow(`
		SELECT COUNT(*) 
		FROM syspublications 
		WHERE name = @p1
	`, details.PublicationName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking publication: %v", err)
	}

	if exists == 0 {
		return fmt.Errorf("publication %s does not exist", details.PublicationName)
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return nil
}

func listenForReplicationEvents(db *sql.DB, details *MSSQLReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	// Create a change tracking query
	// Note: This requires that change tracking is enabled on the database and table

	// Enable change tracking if not already enabled
	_, err := db.Exec(`
		IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
		BEGIN
			ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)
		END
	`)
	if err != nil {
		log.Printf("Error enabling change tracking on database: %v", err)
		return
	}

	// Enable change tracking on the table if not already enabled
	_, err = db.Exec(fmt.Sprintf(`
		IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('%s'))
		BEGIN
			ALTER TABLE %s ENABLE CHANGE_TRACKING
		END
	`, details.TableName, details.TableName))
	if err != nil {
		log.Printf("Error enabling change tracking on table %s: %v", details.TableName, err)
		return
	}

	// Get the current change tracking version
	var currentVersion int64
	err = db.QueryRow(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).Scan(&currentVersion)
	if err != nil {
		log.Printf("Error getting current change tracking version: %v", err)
		return
	}

	for {
		changes, err := getReplicationChanges(db, details.TableName, currentVersion)
		if err != nil {
			log.Printf("Error getting replication changes: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, change := range changes {
			event := map[string]interface{}{
				"table":     details.TableName,
				"operation": change.Operation,
				"data":      change.Data,
				"old_data":  change.OldData,
			}
			eventHandler(event)
		}

		// Update the current version
		err = db.QueryRow(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`).Scan(&currentVersion)
		if err != nil {
			log.Printf("Error getting current change tracking version: %v", err)
		}

		time.Sleep(1 * time.Second)
	}
}

func getReplicationChanges(db *sql.DB, tableName string, lastVersion int64) ([]MSSQLReplicationChange, error) {
	// Query for changes since the last version
	query := fmt.Sprintf(`
		SELECT 
			CT.SYS_CHANGE_OPERATION,
			CT.SYS_CHANGE_VERSION,
			CT.SYS_CHANGE_CREATION_VERSION,
			CT.SYS_CHANGE_COLUMNS,
			T.*
		FROM CHANGETABLE(CHANGES %s, %d) AS CT
		LEFT JOIN %s AS T
		ON CT.SYS_CHANGE_OPERATION != 'D' AND CT.__$update_mask IS NOT NULL
		ORDER BY CT.SYS_CHANGE_VERSION
	`, tableName, lastVersion, tableName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying for changes: %v", err)
	}
	defer rows.Close()

	var changes []MSSQLReplicationChange

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %v", err)
	}

	// Process each row
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// Extract operation type
		var operation string
		if values[0] != nil {
			operation = values[0].(string)
		}

		// Create data map
		data := make(map[string]interface{})
		for i := 4; i < len(columns); i++ {
			data[columns[i]] = values[i]
		}

		change := MSSQLReplicationChange{
			Operation: operation,
			Data:      data,
		}

		changes = append(changes, change)
	}

	return changes, nil
}
