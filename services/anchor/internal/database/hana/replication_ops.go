//go:build enterprise
// +build enterprise

package hana

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for SAP HANA using triggers.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether this database supports CDC/replication.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"trigger-based", "sda", "sdi"}
}

// CheckPrerequisites checks if all prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if we can create triggers
	var canCreateTrigger int
	err := r.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYS.PRIVILEGES 
		WHERE PRIVILEGE = 'CREATE ANY TRIGGER'
	`).Scan(&canCreateTrigger)

	if err != nil {
		log.Printf("Warning: Could not check trigger privileges: %v", err)
	}

	return nil
}

// Connect establishes a CDC connection (not separately needed for HANA triggers).
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// HANA triggers use the same connection
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.HANA, "connect", "trigger-based uses same connection")
}

// GetLag returns the replication lag information.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	lag := make(map[string]interface{})
	lag["mechanism"] = "trigger-based"
	lag["lag_seconds"] = 0 // Trigger-based CDC has minimal lag
	return lag, nil
}

// ListSlots lists replication slots (HANA uses triggers, not slots).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	// HANA doesn't use replication slots
	return []map[string]interface{}{}, nil
}

// DropSlot drops a replication slot (HANA uses triggers, not slots).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.HANA, "drop slot", "hana does not use slots")
}

// ListPublications lists publications (not applicable for HANA).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	// HANA doesn't use publications
	return []map[string]interface{}{}, nil
}

// DropPublication drops a publication (not applicable for HANA).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.HANA, "drop publication", "hana does not use publications")
}

// GetStatus returns the status of the replication connection.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := make(map[string]interface{})
	status["cdc_mechanism"] = "triggers"
	status["connected"] = r.conn.IsConnected()

	// Check for CDC log tables
	var cdcTables int
	err := r.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYS.TABLES 
		WHERE SCHEMA_NAME = CURRENT_SCHEMA 
		AND TABLE_NAME LIKE '%_CDC_LOG'
	`).Scan(&cdcTables)
	if err == nil {
		status["cdc_log_tables"] = cdcTables
	}

	return status, nil
}

// SetupCDCForTable enables CDC for a specific table by creating a change log table and triggers.
func (r *ReplicationOps) SetupCDCForTable(ctx context.Context, schema, tableName string) error {
	// Check if the table exists
	var exists int
	err := r.conn.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?",
		schema, tableName).Scan(&exists)
	if err != nil {
		return adapter.WrapError(dbcapabilities.HANA, "setup_cdc", err)
	}
	if exists == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.HANA,
			"setup_cdc",
			adapter.ErrTableNotFound,
		).WithContext("table", fmt.Sprintf("%s.%s", schema, tableName))
	}

	// Create CDC log table
	cdcLogTableName := fmt.Sprintf("%s_CDC_LOG", tableName)

	// Get table columns
	query := `
		SELECT COLUMN_NAME, DATA_TYPE_NAME 
		FROM SYS.TABLE_COLUMNS 
		WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
		ORDER BY POSITION
	`
	rows, err := r.conn.db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return adapter.WrapError(dbcapabilities.HANA, "setup_cdc", err)
	}
	defer rows.Close()

	var columns []string
	var columnDefs []string

	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			return adapter.WrapError(dbcapabilities.HANA, "setup_cdc", err)
		}
		columns = append(columns, colName)
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", QuoteIdentifier(colName), dataType))
	}

	// Add CDC metadata columns
	columnDefs = append(columnDefs,
		"\"CDC_OPERATION\" VARCHAR(10)",
		"\"CDC_TIMESTAMP\" TIMESTAMP",
		"\"CDC_COMMIT_ID\" BIGINT GENERATED ALWAYS AS IDENTITY")

	// Create CDC log table
	createLogTableSQL := fmt.Sprintf(
		"CREATE TABLE %s.%s (%s)",
		QuoteIdentifier(schema),
		QuoteIdentifier(cdcLogTableName),
		strings.Join(columnDefs, ", "))

	_, err = r.conn.db.ExecContext(ctx, createLogTableSQL)
	if err != nil {
		// Table might already exist
		log.Printf("Warning: Could not create CDC log table: %v", err)
	}

	// Create INSERT trigger
	insertTriggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s_CDC_INSERT_TRG
		AFTER INSERT ON %s.%s
		REFERENCING NEW ROW AS NEW_ROW
		FOR EACH ROW
		BEGIN
			INSERT INTO %s.%s (%s, "CDC_OPERATION", "CDC_TIMESTAMP")
			VALUES (%s, 'INSERT', CURRENT_TIMESTAMP);
		END`,
		tableName,
		QuoteIdentifier(schema), QuoteIdentifier(tableName),
		QuoteIdentifier(schema), QuoteIdentifier(cdcLogTableName),
		strings.Join(quotedColumns(columns), ", "),
		newRowReferences(columns))

	_, err = r.conn.db.ExecContext(ctx, insertTriggerSQL)
	if err != nil {
		log.Printf("Warning: Could not create INSERT trigger: %v", err)
	}

	// Create UPDATE trigger
	updateTriggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s_CDC_UPDATE_TRG
		AFTER UPDATE ON %s.%s
		REFERENCING OLD ROW AS OLD_ROW NEW ROW AS NEW_ROW
		FOR EACH ROW
		BEGIN
			INSERT INTO %s.%s (%s, "CDC_OPERATION", "CDC_TIMESTAMP")
			VALUES (%s, 'UPDATE', CURRENT_TIMESTAMP);
		END`,
		tableName,
		QuoteIdentifier(schema), QuoteIdentifier(tableName),
		QuoteIdentifier(schema), QuoteIdentifier(cdcLogTableName),
		strings.Join(quotedColumns(columns), ", "),
		newRowReferences(columns))

	_, err = r.conn.db.ExecContext(ctx, updateTriggerSQL)
	if err != nil {
		log.Printf("Warning: Could not create UPDATE trigger: %v", err)
	}

	// Create DELETE trigger
	deleteTriggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s_CDC_DELETE_TRG
		AFTER DELETE ON %s.%s
		REFERENCING OLD ROW AS OLD_ROW
		FOR EACH ROW
		BEGIN
			INSERT INTO %s.%s (%s, "CDC_OPERATION", "CDC_TIMESTAMP")
			VALUES (%s, 'DELETE', CURRENT_TIMESTAMP);
		END`,
		tableName,
		QuoteIdentifier(schema), QuoteIdentifier(tableName),
		QuoteIdentifier(schema), QuoteIdentifier(cdcLogTableName),
		strings.Join(quotedColumns(columns), ", "),
		oldRowReferences(columns))

	_, err = r.conn.db.ExecContext(ctx, deleteTriggerSQL)
	if err != nil {
		log.Printf("Warning: Could not create DELETE trigger: %v", err)
	}

	return nil
}

// GetCDCChanges retrieves changes from the CDC log table.
func GetCDCChanges(ctx context.Context, db *sql.DB, schema, tableName string, lastCommitID int64) ([]HanaReplicationChange, int64, error) {
	cdcLogTableName := fmt.Sprintf("%s_CDC_LOG", tableName)

	// Get column names for the source table
	query := `
		SELECT COLUMN_NAME 
		FROM SYS.TABLE_COLUMNS 
		WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
		ORDER BY POSITION
	`
	rows, err := db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, lastCommitID, err
	}

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			rows.Close()
			return nil, lastCommitID, err
		}
		columns = append(columns, colName)
	}
	rows.Close()

	// Build column list with quotes
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(col)
	}

	// Query CDC log table for new changes
	selectSQL := fmt.Sprintf(`
		SELECT %s, "CDC_OPERATION", "CDC_TIMESTAMP", "CDC_COMMIT_ID"
		FROM %s.%s
		WHERE "CDC_COMMIT_ID" > ?
		ORDER BY "CDC_COMMIT_ID"`,
		strings.Join(quotedCols, ", "),
		QuoteIdentifier(schema),
		QuoteIdentifier(cdcLogTableName))

	rows, err = db.QueryContext(ctx, selectSQL, lastCommitID)
	if err != nil {
		return nil, lastCommitID, err
	}
	defer rows.Close()

	var changes []HanaReplicationChange
	var maxCommitID int64 = lastCommitID

	for rows.Next() {
		values := make([]interface{}, len(columns)+3) // +3 for operation, timestamp, commit_id
		valuePtrs := make([]interface{}, len(values))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		data := make(map[string]interface{})
		for i, col := range columns {
			data[col] = values[i]
		}

		operation := fmt.Sprintf("%v", values[len(columns)])
		commitID := values[len(columns)+2].(int64)

		change := HanaReplicationChange{
			Operation: operation,
			Data:      data,
		}

		changes = append(changes, change)

		if commitID > maxCommitID {
			maxCommitID = commitID
		}
	}

	return changes, maxCommitID, nil
}

// Helper functions
func quotedColumns(columns []string) []string {
	result := make([]string, len(columns))
	for i, col := range columns {
		result[i] = QuoteIdentifier(col)
	}
	return result
}

func newRowReferences(columns []string) string {
	refs := make([]string, len(columns))
	for i, col := range columns {
		refs[i] = fmt.Sprintf(":NEW_ROW.%s", QuoteIdentifier(col))
	}
	return strings.Join(refs, ", ")
}

func oldRowReferences(columns []string) string {
	refs := make([]string, len(columns))
	for i, col := range columns {
		refs[i] = fmt.Sprintf(":OLD_ROW.%s", QuoteIdentifier(col))
	}
	return strings.Join(refs, ", ")
}
