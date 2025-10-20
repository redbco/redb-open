//go:build enterprise
// +build enterprise

package db2

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for IBM DB2.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether this database supports CDC/replication.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"trigger-based", "sql-replication", "q-replication"}
}

// CheckPrerequisites checks if all prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if database has archiving enabled
	var logArchiveMethod sql.NullString
	err := r.conn.db.QueryRowContext(ctx, `
		SELECT VALUE 
		FROM SYSIBMADM.DBCFG 
		WHERE NAME = 'logarchmeth1'
	`).Scan(&logArchiveMethod)

	if err != nil {
		// May not have permissions, just log
		log.Printf("Warning: Could not check log archive method: %v", err)
	}

	return nil
}

// Connect establishes a CDC connection (not applicable for DB2 trigger-based CDC).
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// DB2 trigger-based CDC doesn't require a separate connection
	// Return unsupported as we handle CDC through the main connection
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.DB2, "connect", "trigger-based uses same connection")
}

// GetLag returns the replication lag information.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	lag := make(map[string]interface{})
	lag["mechanism"] = "trigger-based"
	lag["lag_seconds"] = 0 // Trigger-based CDC has minimal lag
	return lag, nil
}

// ListSlots lists replication slots (not applicable for DB2 trigger-based CDC).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	// DB2 doesn't use replication slots like PostgreSQL
	return []map[string]interface{}{}, nil
}

// DropSlot drops a replication slot (not applicable for DB2 trigger-based CDC).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	// DB2 doesn't use replication slots
	return adapter.NewUnsupportedOperationError(dbcapabilities.DB2, "drop slot", "db2 does not use slots")
}

// ListPublications lists publications (not applicable for DB2).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	// DB2 doesn't use publications like PostgreSQL
	return []map[string]interface{}{}, nil
}

// DropPublication drops a publication (not applicable for DB2).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	// DB2 doesn't use publications
	return adapter.NewUnsupportedOperationError(dbcapabilities.DB2, "drop publication", "db2 does not use publications")
}

// GetStatus returns the status of the replication connection.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := make(map[string]interface{})
	status["cdc_mechanism"] = "trigger-based"
	status["connected"] = r.conn.IsConnected()

	// Check if any CDC tables exist
	var cdcTableCount int
	err := r.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYSCAT.TABLES 
		WHERE TABNAME LIKE '%_CHANGE_LOG' 
	`).Scan(&cdcTableCount)

	if err == nil {
		status["cdc_tables"] = cdcTableCount
	}

	return status, nil
}

// SetupCDCForTable enables CDC for a specific table by creating a change log table and triggers.
func (r *ReplicationOps) SetupCDCForTable(ctx context.Context, schema, tableName string) error {
	// Check if the table exists
	var exists int
	err := r.conn.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?",
		schema, tableName).Scan(&exists)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DB2, "setup_cdc", err)
	}
	if exists == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.DB2,
			"setup_cdc",
			adapter.ErrTableNotFound,
		).WithContext("table", fmt.Sprintf("%s.%s", schema, tableName))
	}

	// Create CDC log table
	changeLogTableName := fmt.Sprintf("%s_CHANGE_LOG", tableName)

	// Get table columns
	query := `
		SELECT COLNAME, TYPENAME 
		FROM SYSCAT.COLUMNS 
		WHERE TABSCHEMA = ? AND TABNAME = ?
		ORDER BY COLNO
	`
	rows, err := r.conn.db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DB2, "setup_cdc", err)
	}
	defer rows.Close()

	var columns []string
	var columnDefs []string

	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			return adapter.WrapError(dbcapabilities.DB2, "setup_cdc", err)
		}
		columns = append(columns, colName)
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", QuoteIdentifier(colName), dataType))
	}

	// Add CDC metadata columns
	columnDefs = append(columnDefs,
		"CHANGE_OPERATION VARCHAR(10)",
		"CHANGE_TIMESTAMP TIMESTAMP",
		"CHANGE_ID BIGINT GENERATED ALWAYS AS IDENTITY")

	// Create CDC log table
	createLogTableSQL := fmt.Sprintf(
		"CREATE TABLE %s.%s (%s)",
		QuoteIdentifier(schema),
		QuoteIdentifier(changeLogTableName),
		strings.Join(columnDefs, ", "))

	_, err = r.conn.db.ExecContext(ctx, createLogTableSQL)
	if err != nil {
		// Table might already exist
		log.Printf("Warning: Could not create CDC log table: %v", err)
	}

	// Create INSERT trigger
	insertTriggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s_CHG_INS
		AFTER INSERT ON %s.%s
		REFERENCING NEW AS N
		FOR EACH ROW
		BEGIN ATOMIC
			INSERT INTO %s.%s (%s, CHANGE_OPERATION, CHANGE_TIMESTAMP)
			VALUES (%s, 'INSERT', CURRENT_TIMESTAMP);
		END`,
		tableName,
		QuoteIdentifier(schema), QuoteIdentifier(tableName),
		QuoteIdentifier(schema), QuoteIdentifier(changeLogTableName),
		strings.Join(columns, ", "),
		newRowReferences(columns))

	_, err = r.conn.db.ExecContext(ctx, insertTriggerSQL)
	if err != nil {
		log.Printf("Warning: Could not create INSERT trigger: %v", err)
	}

	// Create UPDATE trigger
	updateTriggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s_CHG_UPD
		AFTER UPDATE ON %s.%s
		REFERENCING NEW AS N
		FOR EACH ROW
		BEGIN ATOMIC
			INSERT INTO %s.%s (%s, CHANGE_OPERATION, CHANGE_TIMESTAMP)
			VALUES (%s, 'UPDATE', CURRENT_TIMESTAMP);
		END`,
		tableName,
		QuoteIdentifier(schema), QuoteIdentifier(tableName),
		QuoteIdentifier(schema), QuoteIdentifier(changeLogTableName),
		strings.Join(columns, ", "),
		newRowReferences(columns))

	_, err = r.conn.db.ExecContext(ctx, updateTriggerSQL)
	if err != nil {
		log.Printf("Warning: Could not create UPDATE trigger: %v", err)
	}

	// Create DELETE trigger
	deleteTriggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s_CHG_DEL
		AFTER DELETE ON %s.%s
		REFERENCING OLD AS O
		FOR EACH ROW
		BEGIN ATOMIC
			INSERT INTO %s.%s (%s, CHANGE_OPERATION, CHANGE_TIMESTAMP)
			VALUES (%s, 'DELETE', CURRENT_TIMESTAMP);
		END`,
		tableName,
		QuoteIdentifier(schema), QuoteIdentifier(tableName),
		QuoteIdentifier(schema), QuoteIdentifier(changeLogTableName),
		strings.Join(columns, ", "),
		oldRowReferences(columns))

	_, err = r.conn.db.ExecContext(ctx, deleteTriggerSQL)
	if err != nil {
		log.Printf("Warning: Could not create DELETE trigger: %v", err)
	}

	return nil
}

// GetCDCChanges retrieves changes from the CDC log table.
func GetCDCChanges(ctx context.Context, db *sql.DB, schema, tableName string, lastChangeID int64) ([]Db2ReplicationChange, int64, error) {
	changeLogTableName := fmt.Sprintf("%s_CHANGE_LOG", tableName)

	// Get column names for the source table
	query := `
		SELECT COLNAME 
		FROM SYSCAT.COLUMNS 
		WHERE TABSCHEMA = ? AND TABNAME = ?
		ORDER BY COLNO
	`
	rows, err := db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, lastChangeID, err
	}

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			rows.Close()
			return nil, lastChangeID, err
		}
		columns = append(columns, colName)
	}
	rows.Close()

	// Query CDC log table for new changes
	selectSQL := fmt.Sprintf(`
		SELECT %s, CHANGE_OPERATION, CHANGE_TIMESTAMP, CHANGE_ID
		FROM %s.%s
		WHERE CHANGE_ID > ?
		ORDER BY CHANGE_ID`,
		strings.Join(columns, ", "),
		QuoteIdentifier(schema),
		QuoteIdentifier(changeLogTableName))

	rows, err = db.QueryContext(ctx, selectSQL, lastChangeID)
	if err != nil {
		return nil, lastChangeID, err
	}
	defer rows.Close()

	var changes []Db2ReplicationChange
	var maxChangeID int64 = lastChangeID

	for rows.Next() {
		values := make([]interface{}, len(columns)+3) // +3 for operation, timestamp, change_id
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
		changeID := values[len(columns)+2].(int64)

		change := Db2ReplicationChange{
			Operation: operation,
			Data:      data,
		}

		changes = append(changes, change)

		if changeID > maxChangeID {
			maxChangeID = changeID
		}
	}

	return changes, maxChangeID, nil
}

// Helper functions
func newRowReferences(columns []string) string {
	refs := make([]string, len(columns))
	for i, col := range columns {
		refs[i] = fmt.Sprintf("N.%s", QuoteIdentifier(col))
	}
	return strings.Join(refs, ", ")
}

func oldRowReferences(columns []string) string {
	refs := make([]string, len(columns))
	for i, col := range columns {
		refs[i] = fmt.Sprintf("O.%s", QuoteIdentifier(col))
	}
	return strings.Join(refs, ", ")
}
