package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/godror/godror" // Oracle driver
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// CreateReplicationSource sets up a replication source
func CreateReplicationSource(db *sql.DB, tableName string, databaseID string, eventHandler func(map[string]interface{})) (*OracleReplicationSourceDetails, error) {
	// Generate unique session ID for LogMiner
	logMinerSessionID := fmt.Sprintf("logminer_%s_%s", databaseID, dbclient.GenerateUniqueID())

	// Ensure supplemental logging is enabled for the table
	_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableName))
	if err != nil {
		return nil, fmt.Errorf("error enabling supplemental logging: %v", err)
	}

	// Get current SCN (System Change Number)
	var currentSCN int64
	err = db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&currentSCN)
	if err != nil {
		return nil, fmt.Errorf("error getting current SCN: %v", err)
	}

	details := &OracleReplicationSourceDetails{
		LogMinerSessionID: logMinerSessionID,
		TableName:         tableName,
		DatabaseID:        databaseID,
		LastSCN:           currentSCN,
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return details, nil
}

// ReconnectToReplicationSource reconnects to an existing replication source
func ReconnectToReplicationSource(db *sql.DB, details *OracleReplicationSourceDetails, eventHandler func(map[string]interface{})) error {
	// Get current SCN if LastSCN is not set
	if details.LastSCN == 0 {
		err := db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&details.LastSCN)
		if err != nil {
			return fmt.Errorf("error getting current SCN: %v", err)
		}
	}

	// Start listening for replication events
	go listenForReplicationEvents(db, details, eventHandler)

	return nil
}

func listenForReplicationEvents(db *sql.DB, details *OracleReplicationSourceDetails, eventHandler func(map[string]interface{})) {
	for {
		changes, newSCN, err := getReplicationChanges(db, details.TableName, details.LastSCN)
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

		// Update the last SCN
		details.LastSCN = newSCN
		time.Sleep(100 * time.Millisecond)
	}
}

func getReplicationChanges(db *sql.DB, tableName string, startSCN int64) ([]OracleReplicationChange, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		BEGIN
			DBMS_LOGMNR.START_LOGMNR(
				STARTSCN => %d,
				ENDSCN => %d,
				OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + 
						   DBMS_LOGMNR.CONTINUOUS_MINE + 
						   DBMS_LOGMNR.NO_ROWID_IN_STMT
			);
		END;
	`, startSCN, endSCN))
	if err != nil {
		return nil, startSCN, fmt.Errorf("error starting LogMiner: %v", err)
	}

	// Query LogMiner view for changes
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT 
			OPERATION, 
			SQL_REDO,
			SQL_UNDO,
			TIMESTAMP
		FROM V$LOGMNR_CONTENTS
		WHERE 
			SEG_OWNER || '.' || TABLE_NAME = UPPER('%s')
			AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
		ORDER BY TIMESTAMP
	`, tableName))
	if err != nil {
		// End LogMiner session
		_, endErr := db.ExecContext(ctx, "BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
		if endErr != nil {
			log.Printf("Error ending LogMiner session: %v", endErr)
		}
		return nil, startSCN, err
	}
	defer rows.Close()

	var changes []OracleReplicationChange
	for rows.Next() {
		var (
			operation string
			sqlRedo   string
			sqlUndo   string
			timestamp time.Time
		)
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
	_, err = db.ExecContext(ctx, "BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
	if err != nil {
		log.Printf("Error ending LogMiner session: %v", err)
	}

	return changes, endSCN, nil
}

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

func extractValuesFromInsert(sqlRedo string) (map[string]interface{}, error) {
	// This is a simplified parser for INSERT statements
	// Example: INSERT INTO "SCHEMA"."TABLE"("COL1","COL2") VALUES ('val1', 123)
	data := make(map[string]interface{})

	// Extract column names and values
	colStart := strings.Index(sqlRedo, "(")
	colEnd := strings.Index(sqlRedo, ")")
	if colStart == -1 || colEnd == -1 || colEnd <= colStart {
		return data, fmt.Errorf("invalid INSERT statement format")
	}

	valStart := strings.Index(sqlRedo, "VALUES") + 6
	if valStart == -1 {
		return data, fmt.Errorf("invalid INSERT statement format")
	}
	valStart = strings.Index(sqlRedo[valStart:], "(") + valStart
	valEnd := strings.LastIndex(sqlRedo, ")")
	if valStart == -1 || valEnd == -1 || valEnd <= valStart {
		return data, fmt.Errorf("invalid INSERT statement format")
	}

	columns := parseColumns(sqlRedo[colStart+1 : colEnd])
	values := parseValues(sqlRedo[valStart+1 : valEnd])

	if len(columns) != len(values) {
		return data, fmt.Errorf("column and value count mismatch")
	}

	for i, col := range columns {
		data[col] = values[i]
	}

	return data, nil
}

func extractValuesFromUpdate(sqlRedo string) (map[string]interface{}, error) {
	// This is a simplified parser for UPDATE statements
	// Example: UPDATE "SCHEMA"."TABLE" SET "COL1" = 'val1', "COL2" = 123 WHERE ...
	data := make(map[string]interface{})

	// Extract SET clause
	setStart := strings.Index(sqlRedo, "SET") + 3
	whereStart := strings.Index(sqlRedo, "WHERE")
	if setStart == -1 || whereStart == -1 || whereStart <= setStart {
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

func extractValuesFromWhere(sql string) (map[string]interface{}, error) {
	// This is a simplified parser for WHERE clauses
	// Example: ... WHERE "COL1" = 'val1' AND "COL2" = 123
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
	// Parse column names from a comma-separated list
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
	// Parse values from a comma-separated list
	// This is a simplified implementation and may need to be expanded
	values := strings.Split(valuesStr, ",")
	result := make([]interface{}, 0, len(values))

	for _, val := range values {
		result = append(result, parseValue(val))
	}

	return result
}

func parseValue(val string) interface{} {
	// Parse a single value
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
		// Float
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	} else {
		// Integer
		var i int64
		fmt.Sscanf(val, "%d", &i)
		return i
	}
}
