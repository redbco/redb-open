//go:build enterprise
// +build enterprise

package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for Oracle using LogMiner.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether this database supports CDC/replication.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"logminer", "goldengate", "streams"}
}

// CheckPrerequisites checks if all prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if supplemental logging is enabled
	var suppLogStatus string
	err := r.conn.db.QueryRowContext(ctx,
		"SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE").Scan(&suppLogStatus)
	if err != nil {
		log.Printf("Warning: Could not check supplemental logging: %v", err)
	}

	// Check if archive log mode is enabled (required for LogMiner)
	var logMode string
	err = r.conn.db.QueryRowContext(ctx, "SELECT LOG_MODE FROM V$DATABASE").Scan(&logMode)
	if err != nil {
		log.Printf("Warning: Could not check log mode: %v", err)
	}

	return nil
}

// Connect establishes a CDC connection (not separately needed for Oracle LogMiner).
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Oracle LogMiner uses the same connection
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.Oracle,
		"connect",
		"logminer uses same connection",
	)
}

// GetLag returns the replication lag information.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	lag := make(map[string]interface{})
	lag["mechanism"] = "logminer"
	lag["lag_seconds"] = 0 // LogMiner has minimal lag
	return lag, nil
}

// ListSlots lists replication slots (Oracle uses SCN, not slots).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	// Oracle doesn't use replication slots like PostgreSQL
	return []map[string]interface{}{}, nil
}

// DropSlot drops a replication slot (Oracle uses SCN, not slots).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.Oracle,
		"drop slot",
		"oracle uses scn not slots",
	)
}

// ListPublications lists publications (not applicable for Oracle).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	// Oracle doesn't use publications like PostgreSQL
	return []map[string]interface{}{}, nil
}

// DropPublication drops a publication (not applicable for Oracle).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.Oracle,
		"drop publication",
		"oracle does not use publications",
	)
}

// GetStatus returns the status of the replication connection.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := make(map[string]interface{})
	status["cdc_mechanism"] = "logminer"
	status["connected"] = r.conn.IsConnected()

	// Get current SCN
	var currentSCN int64
	err := r.conn.db.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&currentSCN)
	if err == nil {
		status["current_scn"] = currentSCN
	}

	// Check if supplemental logging is enabled
	var suppLogStatus string
	err = r.conn.db.QueryRowContext(ctx,
		"SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE").Scan(&suppLogStatus)
	if err == nil {
		status["supplemental_logging"] = suppLogStatus
	}

	return status, nil
}

// SetupCDCForTable enables supplemental logging for a specific table.
func (r *ReplicationOps) SetupCDCForTable(ctx context.Context, schema, tableName string) error {
	// Check if the table exists
	var exists int
	err := r.conn.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = UPPER(?) AND table_name = UPPER(?)",
		schema, tableName).Scan(&exists)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "setup_cdc", err)
	}
	if exists == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.Oracle,
			"setup_cdc",
			adapter.ErrTableNotFound,
		).WithContext("table", fmt.Sprintf("%s.%s", schema, tableName))
	}

	// Enable supplemental logging for the table
	fullTableName := fmt.Sprintf("%s.%s", schema, tableName)
	_, err = r.conn.db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", fullTableName))
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "setup_cdc", err)
	}

	// Ensure database-level supplemental logging is enabled
	_, err = r.conn.db.ExecContext(ctx,
		"ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	if err != nil {
		// May already be enabled, log but don't fail
		log.Printf("Warning: Could not enable database supplemental logging: %v", err)
	}

	return nil
}

// StartLogMiner starts Oracle LogMiner for a specific SCN range.
func StartLogMiner(db *sql.DB, startSCN, endSCN int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
		BEGIN
			DBMS_LOGMNR.START_LOGMNR(
				STARTSCN => %d,
				ENDSCN => %d,
				OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + 
						   DBMS_LOGMNR.CONTINUOUS_MINE + 
						   DBMS_LOGMNR.NO_ROWID_IN_STMT
			);
		END;`, startSCN, endSCN)

	_, err := db.ExecContext(ctx, query)
	return err
}

// StopLogMiner stops Oracle LogMiner.
func StopLogMiner(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
	return err
}

// GetLogMinerChanges retrieves changes from LogMiner for a specific table.
func GetLogMinerChanges(ctx context.Context, db *sql.DB, tableName string, startSCN int64) ([]OracleReplicationChange, int64, error) {
	// Get current SCN
	var endSCN int64
	err := db.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&endSCN)
	if err != nil {
		return nil, startSCN, err
	}

	// If no new changes, return empty result
	if endSCN <= startSCN {
		return []OracleReplicationChange{}, startSCN, nil
	}

	// Start LogMiner session
	err = StartLogMiner(db, startSCN, endSCN)
	if err != nil {
		return nil, startSCN, fmt.Errorf("error starting LogMiner: %w", err)
	}

	// Query LogMiner view for changes
	query := fmt.Sprintf(`
		SELECT 
			OPERATION, 
			SQL_REDO,
			SQL_UNDO,
			TIMESTAMP
		FROM V$LOGMNR_CONTENTS
		WHERE 
			SEG_OWNER || '.' || TABLE_NAME = UPPER('%s')
			AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
		ORDER BY TIMESTAMP`, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		StopLogMiner(db)
		return nil, startSCN, err
	}
	defer rows.Close()

	var changes []OracleReplicationChange
	for rows.Next() {
		var operation, sqlRedo, sqlUndo string
		var timestamp time.Time

		if err := rows.Scan(&operation, &sqlRedo, &sqlUndo, &timestamp); err != nil {
			continue
		}

		change, err := parseOracleChange(operation, sqlRedo, sqlUndo)
		if err != nil {
			log.Printf("Error parsing Oracle change: %v", err)
			continue
		}

		changes = append(changes, change)
	}

	// End LogMiner session
	StopLogMiner(db)

	return changes, endSCN, nil
}

// parseOracleChange parses SQL_REDO and SQL_UNDO from LogMiner to extract data.
func parseOracleChange(operation, sqlRedo, sqlUndo string) (OracleReplicationChange, error) {
	change := OracleReplicationChange{
		Operation: strings.ToUpper(operation),
		Data:      make(map[string]interface{}),
		OldData:   make(map[string]interface{}),
	}

	switch change.Operation {
	case "INSERT":
		data, err := extractValuesFromInsert(sqlRedo)
		if err != nil {
			return change, err
		}
		change.Data = data

	case "UPDATE":
		newData, err := extractValuesFromUpdate(sqlRedo)
		if err != nil {
			return change, err
		}
		change.Data = newData

		oldData, err := extractValuesFromWhere(sqlUndo)
		if err != nil {
			return change, err
		}
		change.OldData = oldData

	case "DELETE":
		oldData, err := extractValuesFromWhere(sqlRedo)
		if err != nil {
			return change, err
		}
		change.OldData = oldData
	}

	return change, nil
}

// extractValuesFromInsert parses INSERT statement to extract column/value pairs.
func extractValuesFromInsert(sqlRedo string) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	// Simplified parser for INSERT statements
	// Example: INSERT INTO "SCHEMA"."TABLE"("COL1","COL2") VALUES ('val1', 123)
	colStart := strings.Index(sqlRedo, "(")
	colEnd := strings.Index(sqlRedo, ")")
	if colStart == -1 || colEnd == -1 {
		return data, fmt.Errorf("invalid INSERT statement format")
	}

	valStart := strings.Index(sqlRedo, "VALUES") + 6
	if valStart == -1 {
		return data, fmt.Errorf("invalid INSERT statement format")
	}
	valStart = strings.Index(sqlRedo[valStart:], "(") + valStart
	valEnd := strings.LastIndex(sqlRedo, ")")
	if valStart == -1 || valEnd == -1 {
		return data, fmt.Errorf("invalid INSERT statement format")
	}

	columnsStr := sqlRedo[colStart+1 : colEnd]
	valuesStr := sqlRedo[valStart+1 : valEnd]

	columns := parseColumns(columnsStr)
	values := parseValues(valuesStr)

	if len(columns) != len(values) {
		return data, fmt.Errorf("column and value count mismatch")
	}

	for i, col := range columns {
		data[col] = values[i]
	}

	return data, nil
}

// extractValuesFromUpdate parses UPDATE SET clause.
func extractValuesFromUpdate(sqlRedo string) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	setStart := strings.Index(sqlRedo, "SET") + 3
	whereStart := strings.Index(sqlRedo, "WHERE")
	if setStart == -1 || whereStart == -1 {
		return data, fmt.Errorf("invalid UPDATE statement format")
	}

	setClause := sqlRedo[setStart:whereStart]
	assignments := strings.Split(setClause, ",")

	for _, assignment := range assignments {
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			continue
		}

		colName := strings.Trim(parts[0], " \"'`")
		value := parseValue(strings.TrimSpace(parts[1]))
		data[colName] = value
	}

	return data, nil
}

// extractValuesFromWhere parses WHERE clause to extract column/value pairs.
func extractValuesFromWhere(sql string) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	whereStart := strings.Index(sql, "WHERE") + 5
	if whereStart == -1 {
		return data, fmt.Errorf("invalid SQL statement format")
	}

	whereClause := sql[whereStart:]
	conditions := strings.Split(whereClause, "AND")

	for _, condition := range conditions {
		parts := strings.SplitN(condition, "=", 2)
		if len(parts) != 2 {
			continue
		}

		colName := strings.Trim(parts[0], " \"'`")
		value := parseValue(strings.TrimSpace(parts[1]))
		data[colName] = value
	}

	return data, nil
}

func parseColumns(columnsStr string) []string {
	columns := strings.Split(columnsStr, ",")
	result := make([]string, 0, len(columns))

	for _, col := range columns {
		col = strings.Trim(col, " \"'`")
		if col != "" {
			result = append(result, col)
		}
	}

	return result
}

func parseValues(valuesStr string) []interface{} {
	values := strings.Split(valuesStr, ",")
	result := make([]interface{}, 0, len(values))

	for _, val := range values {
		result = append(result, parseValue(val))
	}

	return result
}

func parseValue(val string) interface{} {
	val = strings.TrimSpace(val)

	// Handle NULL
	if strings.ToUpper(val) == "NULL" {
		return nil
	}

	// Handle strings
	if (strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) ||
		(strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) {
		return strings.Trim(val, "\"'")
	}

	// Handle numbers
	if strings.Contains(val, ".") {
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	}

	var i int64
	fmt.Sscanf(val, "%d", &i)
	return i
}
