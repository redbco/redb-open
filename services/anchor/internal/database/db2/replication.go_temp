package db2

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

// CreateReplicationSource sets up a replication source for Db2
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*Db2ReplicationSourceDetails, error) {
	// Parse schema and table name
	parts := strings.Split(tableName, ".")
	var schema, table string
	if len(parts) > 1 {
		schema = parts[0]
		table = parts[1]
	} else {
		// Use current schema if not specified
		err := db.QueryRow("VALUES CURRENT SCHEMA").Scan(&schema)
		if err != nil {
			return nil, fmt.Errorf("error getting current schema: %v", err)
		}
		table = parts[0]
	}

	// Check if the table exists
	var exists int
	err := db.QueryRow("SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?",
		schema, table).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists == 0 {
		return nil, fmt.Errorf("table %s.%s does not exist", schema, table)
	}

	// Check if the table has a primary key
	err = db.QueryRow("SELECT COUNT(*) FROM SYSCAT.KEYCOLUSE WHERE TABSCHEMA = ? AND TABNAME = ?",
		schema, table).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("error checking if table has primary key: %v", err)
	}
	if exists == 0 {
		return nil, fmt.Errorf("table %s.%s does not have a primary key, which is required for replication", schema, table)
	}

	// For Db2, we'll use triggers to capture changes
	// Create triggers for INSERT, UPDATE, DELETE operations
	err = createReplicationTriggers(db, schema, table)
	if err != nil {
		return nil, fmt.Errorf("error creating replication triggers: %v", err)
	}

	details := &Db2ReplicationSourceDetails{
		SourceSchema: schema,
		SourceTable:  table,
		DatabaseID:   databaseID,
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(db *sql.DB, details *Db2ReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Verify that the table still exists
	var exists int
	err := db.QueryRow("SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?",
		details.SourceSchema, details.SourceTable).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}
	if exists == 0 {
		return fmt.Errorf("table %s.%s does not exist", details.SourceSchema, details.SourceTable)
	}

	// Verify that the replication triggers exist
	err = db.QueryRow(`
		SELECT COUNT(*) FROM SYSCAT.TRIGGERS 
		WHERE TABSCHEMA = ? AND TABNAME = ? AND TRIGNAME LIKE 'REPL_%'`,
		details.SourceSchema, details.SourceTable).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error checking if replication triggers exist: %v", err)
	}
	if exists < 3 { // We should have 3 triggers (INSERT, UPDATE, DELETE)
		// Recreate the triggers
		err = createReplicationTriggers(db, details.SourceSchema, details.SourceTable)
		if err != nil {
			return fmt.Errorf("error recreating replication triggers: %v", err)
		}
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return nil
}

func createReplicationTriggers(db *sql.DB, schema, table string) error {
	// Create a change log table if it doesn't exist
	changeLogTable := fmt.Sprintf("%s_CHANGE_LOG", table)
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			CHANGE_ID INTEGER GENERATED ALWAYS AS IDENTITY,
			OPERATION VARCHAR(10) NOT NULL,
			CHANGE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
			TABLE_NAME VARCHAR(128) NOT NULL,
			DATA CLOB,
			OLD_DATA CLOB,
			PRIMARY KEY (CHANGE_ID)
		)`, schema, changeLogTable))
	if err != nil {
		return fmt.Errorf("error creating change log table: %v", err)
	}

	// Create INSERT trigger
	_, err = db.Exec(fmt.Sprintf(`
		CREATE OR REPLACE TRIGGER %s.REPL_INS_%s
		AFTER INSERT ON %s.%s
		REFERENCING NEW AS N
		FOR EACH ROW
		BEGIN
			DECLARE v_data CLOB;
			SET v_data = (SELECT JSON_OBJECT(
				%s
			) FROM SYSIBM.SYSDUMMY1);
			
			INSERT INTO %s.%s (OPERATION, TABLE_NAME, DATA)
			VALUES ('INSERT', '%s.%s', v_data);
		END`,
		schema, table, schema, table,
		getColumnJsonMapping("N", schema, table, db),
		schema, changeLogTable, schema, table))
	if err != nil {
		return fmt.Errorf("error creating INSERT trigger: %v", err)
	}

	// Create UPDATE trigger
	_, err = db.Exec(fmt.Sprintf(`
		CREATE OR REPLACE TRIGGER %s.REPL_UPD_%s
		AFTER UPDATE ON %s.%s
		REFERENCING NEW AS N OLD AS O
		FOR EACH ROW
		BEGIN
			DECLARE v_data CLOB;
			DECLARE v_old_data CLOB;
			
			SET v_data = (SELECT JSON_OBJECT(
				%s
			) FROM SYSIBM.SYSDUMMY1);
			
			SET v_old_data = (SELECT JSON_OBJECT(
				%s
			) FROM SYSIBM.SYSDUMMY1);
			
			INSERT INTO %s.%s (OPERATION, TABLE_NAME, DATA, OLD_DATA)
			VALUES ('UPDATE', '%s.%s', v_data, v_old_data);
		END`,
		schema, table, schema, table,
		getColumnJsonMapping("N", schema, table, db),
		getColumnJsonMapping("O", schema, table, db),
		schema, changeLogTable, schema, table))
	if err != nil {
		return fmt.Errorf("error creating UPDATE trigger: %v", err)
	}

	// Create DELETE trigger
	_, err = db.Exec(fmt.Sprintf(`
		CREATE OR REPLACE TRIGGER %s.REPL_DEL_%s
		AFTER DELETE ON %s.%s
		REFERENCING OLD AS O
		FOR EACH ROW
		BEGIN
			DECLARE v_old_data CLOB;
			
			SET v_old_data = (SELECT JSON_OBJECT(
				%s
			) FROM SYSIBM.SYSDUMMY1);
			
			INSERT INTO %s.%s (OPERATION, TABLE_NAME, OLD_DATA)
			VALUES ('DELETE', '%s.%s', v_old_data);
		END`,
		schema, table, schema, table,
		getColumnJsonMapping("O", schema, table, db),
		schema, changeLogTable, schema, table))
	if err != nil {
		return fmt.Errorf("error creating DELETE trigger: %v", err)
	}

	return nil
}

func getColumnJsonMapping(prefix string, schema, table string, db *sql.DB) string {
	// Get all columns for the table
	query := `
		SELECT COLNAME
		FROM SYSCAT.COLUMNS
		WHERE TABSCHEMA = ? AND TABNAME = ?
		ORDER BY COLNO
	`

	rows, err := db.Query(query, schema, table)
	if err != nil {
		log.Printf("Error getting columns for JSON mapping: %v", err)
		return "'error' : 'failed to get columns'"
	}
	defer rows.Close()

	var mappings []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			log.Printf("Error scanning column name: %v", err)
			continue
		}

		// Format: 'column_name' : N.column_name
		mappings = append(mappings, fmt.Sprintf("'%s' : %s.%s", colName, prefix, colName))
	}

	return strings.Join(mappings, ", ")
}

func listenForReplicationEvents(db *sql.DB, details *Db2ReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	changeLogTable := fmt.Sprintf("%s.%s_CHANGE_LOG", details.SourceSchema, details.SourceTable)
	lastProcessedID := 0

	for {
		// Query for new changes
		query := fmt.Sprintf(`
			SELECT CHANGE_ID, OPERATION, DATA, OLD_DATA
			FROM %s
			WHERE CHANGE_ID > ?
			ORDER BY CHANGE_ID
		`, changeLogTable)

		rows, err := db.Query(query, lastProcessedID)
		if err != nil {
			log.Printf("Error querying for replication changes: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		hasChanges := false
		for rows.Next() {
			hasChanges = true
			var changeID int
			var operation string
			var data, oldData sql.NullString

			if err := rows.Scan(&changeID, &operation, &data, &oldData); err != nil {
				log.Printf("Error scanning replication change: %v", err)
				continue
			}

			// Update last processed ID
			if changeID > lastProcessedID {
				lastProcessedID = changeID
			}

			// Parse the data
			change := Db2ReplicationChange{
				Operation: operation,
				Data:      make(map[string]interface{}),
				OldData:   make(map[string]interface{}),
			}

			if data.Valid {
				if err := json.Unmarshal([]byte(data.String), &change.Data); err != nil {
					log.Printf("Error parsing data JSON: %v", err)
				}
			}

			if oldData.Valid {
				if err := json.Unmarshal([]byte(oldData.String), &change.OldData); err != nil {
					log.Printf("Error parsing old data JSON: %v", err)
				}
			}

			// Create event
			event := map[string]interface{}{
				"table":     fmt.Sprintf("%s.%s", details.SourceSchema, details.SourceTable),
				"operation": change.Operation,
				"data":      change.Data,
				"old_data":  change.OldData,
			}

			// Send event to handler
			eventHandler(event)
		}
		rows.Close()

		// If no changes, wait a bit before checking again
		if !hasChanges {
			time.Sleep(1 * time.Second)
		}
	}
}
