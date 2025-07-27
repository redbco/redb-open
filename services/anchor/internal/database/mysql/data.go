package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/logger"
)

// isUUIDBytes checks if a byte slice represents a UUID (16 bytes)
func isUUIDBytes(data []byte) bool {
	return len(data) == 16
}

// bytesToUUIDString converts a 16-byte UUID to the standard UUID string format
func bytesToUUIDString(data []byte) string {
	if len(data) != 16 {
		return string(data) // fallback to string conversion if not 16 bytes
	}

	// Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		data[0], data[1], data[2], data[3],
		data[4], data[5],
		data[6], data[7],
		data[8], data[9],
		data[10], data[11], data[12], data[13], data[14], data[15])
}

// sanitizeValue converts complex types to MySQL-compatible formats
func sanitizeValue(value interface{}, logger *logger.Logger) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []interface{}:
		// Convert slice to JSON string
		if logger != nil {
			logger.Debug("Converting []interface{} to JSON string, length: %d", len(v))
		}
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			if logger != nil {
				logger.Warn("Failed to marshal slice to JSON, using empty array: %v", err)
			}
			return "[]"
		}
		return string(jsonBytes)
	case map[string]interface{}:
		// Convert map to JSON string
		if logger != nil {
			logger.Debug("Converting map[string]interface{} to JSON string, keys: %v", getMapKeys(v))
		}
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			if logger != nil {
				logger.Warn("Failed to marshal map to JSON, using empty object: %v", err)
			}
			return "{}"
		}
		return string(jsonBytes)
	case []byte:
		// Check if this is a UUID (16 bytes)
		if isUUIDBytes(v) {
			if logger != nil {
				logger.Debug("Converting UUID bytes to UUID string format")
			}
			return bytesToUUIDString(v)
		}
		// Convert other bytes to string
		if logger != nil {
			logger.Debug("Converting []byte to string, length: %d", len(v))
		}
		return string(v)
	default:
		// For other types, return as-is
		return v
	}
}

// getMapKeys returns the keys of a map for logging purposes
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// FetchData retrieves data from a specified table
func FetchData(db *sql.DB, tableName string, limit int, logger *logger.Logger) ([]map[string]interface{}, error) {
	if tableName == "" {
		if logger != nil {
			logger.Error("FetchData called with empty table name")
		}
		return nil, fmt.Errorf("table name cannot be empty")
	}

	if logger != nil {
		logger.Info("Fetching data from table: %s (limit: %d)", tableName, limit)
	}

	// Get columns for the table
	columns, err := getColumns(db, tableName)
	if err != nil {
		if logger != nil {
			logger.Error("Failed to get columns for table %s: %v", tableName, err)
		}
		return nil, err
	}

	if logger != nil {
		logger.Debug("Retrieved %d columns for table %s", len(columns), tableName)
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columns, ", "),
		QuoteIdentifier(tableName))
	if limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	if logger != nil {
		logger.Debug("Executing query: %s", query)
	}

	rows, err := db.Query(query)
	if err != nil {
		if logger != nil {
			logger.Error("Error executing query for table %s: %v", tableName, err)
		}
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Process rows
	var result []map[string]interface{}
	rowCount := 0
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the slice of interface{}
		if err := rows.Scan(valuePtrs...); err != nil {
			if logger != nil {
				logger.Error("Error scanning row %d for table %s: %v", rowCount+1, tableName, err)
			}
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
		result = append(result, rowMap)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		if logger != nil {
			logger.Error("Error iterating rows for table %s: %v", tableName, err)
		}
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if logger != nil {
		logger.Info("Successfully fetched %d rows from table %s", rowCount, tableName)
	}

	return result, nil
}

// InsertData inserts data into a specified table
func InsertData(db *sql.DB, tableName string, data []map[string]interface{}, logger *logger.Logger) (int64, error) {
	if len(data) == 0 {
		if logger != nil {
			logger.Info("InsertData called with empty data array for table %s", tableName)
		}
		return 0, nil
	}

	if logger != nil {
		logger.Info("Inserting %d rows into table: %s", len(data), tableName)
	}

	// Get all column names from the first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	if logger != nil {
		logger.Debug("Inserting data with columns: %v", columns)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		if logger != nil {
			logger.Error("Error starting transaction for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if logger != nil {
				logger.Error("Rolling back transaction for table %s due to error: %v", tableName, err)
			}
			tx.Rollback()
		}
	}()

	// Prepare placeholders for the query
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// Quote column names
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = QuoteIdentifier(col)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdentifier(tableName),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))

	if logger != nil {
		logger.Debug("Prepared insert query: %s", query)
	}

	// Prepare the statement
	stmt, err := tx.Prepare(query)
	if err != nil {
		if logger != nil {
			logger.Error("Error preparing statement for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row
	var totalRowsAffected int64
	for i, row := range data {
		// Extract values in the same order as columns
		values := make([]interface{}, len(columns))
		for j, col := range columns {
			values[j] = sanitizeValue(row[col], logger)
		}

		// Execute the statement
		result, err := stmt.Exec(values...)
		if err != nil {
			if logger != nil {
				logger.Error("Error executing insert for row %d in table %s: %v", i+1, tableName, err)
			}
			return 0, fmt.Errorf("error executing insert: %w", err)
		}

		// Get rows affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			if logger != nil {
				logger.Error("Error getting rows affected for row %d in table %s: %v", i+1, tableName, err)
			}
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		if logger != nil {
			logger.Error("Error committing transaction for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	if logger != nil {
		logger.Info("Successfully inserted %d rows into table %s", totalRowsAffected, tableName)
	}

	return totalRowsAffected, nil
}

// UpsertData inserts or updates data in a specified table based on unique constraints
func UpsertData(db *sql.DB, tableName string, data []map[string]interface{}, uniqueColumns []string, logger *logger.Logger) (int64, error) {
	if len(data) == 0 {
		if logger != nil {
			logger.Info("UpsertData called with empty data array for table %s", tableName)
		}
		return 0, nil
	}

	if logger != nil {
		logger.Info("Upserting %d rows into table: %s (unique columns: %v)", len(data), tableName, uniqueColumns)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		if logger != nil {
			logger.Error("Error starting transaction for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if logger != nil {
				logger.Error("Rolling back transaction for table %s due to error: %v", tableName, err)
			}
			tx.Rollback()
		}
	}()

	var totalRowsAffected int64

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	if logger != nil {
		logger.Debug("Upserting data with columns: %v", columns)
	}

	// Create placeholders for the prepared statement
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	// Build the ON DUPLICATE KEY UPDATE clause
	updateSet := make([]string, 0, len(columns)-len(uniqueColumns))
	for _, col := range columns {
		isUnique := false
		for _, uniqueCol := range uniqueColumns {
			if col == uniqueCol {
				isUnique = true
				break
			}
		}
		if !isUnique {
			updateSet = append(updateSet, fmt.Sprintf("%s = VALUES(%s)", QuoteIdentifier(col), QuoteIdentifier(col)))
		}
	}

	// Quote column names
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = QuoteIdentifier(col)
	}

	// Prepare the query
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		QuoteIdentifier(tableName),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateSet, ", "),
	)

	if logger != nil {
		logger.Debug("Prepared upsert query: %s", query)
	}

	// Prepare the statement
	stmt, err := tx.Prepare(query)
	if err != nil {
		if logger != nil {
			logger.Error("Error preparing statement for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row
	for i, row := range data {
		values := make([]interface{}, len(columns))
		for j, col := range columns {
			values[j] = sanitizeValue(row[col], logger)
		}

		result, err := stmt.Exec(values...)
		if err != nil {
			if logger != nil {
				logger.Error("Error executing upsert for row %d in table %s: %v", i+1, tableName, err)
			}
			return 0, fmt.Errorf("error executing upsert: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			if logger != nil {
				logger.Error("Error getting rows affected for row %d in table %s: %v", i+1, tableName, err)
			}
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		if logger != nil {
			logger.Error("Error committing transaction for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	if logger != nil {
		logger.Info("Successfully upserted %d rows in table %s", totalRowsAffected, tableName)
	}

	return totalRowsAffected, nil
}

// UpdateData updates existing data in a specified table based on a condition
func UpdateData(db *sql.DB, tableName string, data []map[string]interface{}, whereColumns []string, logger *logger.Logger) (int64, error) {
	if len(data) == 0 {
		if logger != nil {
			logger.Info("UpdateData called with empty data array for table %s", tableName)
		}
		return 0, nil
	}

	if logger != nil {
		logger.Info("Updating %d rows in table: %s (where columns: %v)", len(data), tableName, whereColumns)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		if logger != nil {
			logger.Error("Error starting transaction for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if logger != nil {
				logger.Error("Rolling back transaction for table %s due to error: %v", tableName, err)
			}
			tx.Rollback()
		}
	}()

	var totalRowsAffected int64

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	if logger != nil {
		logger.Debug("Updating data with columns: %v", columns)
	}

	// Build the WHERE clause
	whereConditions := make([]string, len(whereColumns))
	for i, col := range whereColumns {
		whereConditions[i] = fmt.Sprintf("%s = ?", QuoteIdentifier(col))
	}

	// Build the SET clause
	setClause := make([]string, 0, len(columns)-len(whereColumns))
	for _, col := range columns {
		isWhereColumn := false
		for _, whereCol := range whereColumns {
			if col == whereCol {
				isWhereColumn = true
				break
			}
		}
		if !isWhereColumn {
			setClause = append(setClause, fmt.Sprintf("%s = ?", QuoteIdentifier(col)))
		}
	}

	// Prepare the query
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		QuoteIdentifier(tableName),
		strings.Join(setClause, ", "),
		strings.Join(whereConditions, " AND "),
	)

	if logger != nil {
		logger.Debug("Prepared update query: %s", query)
	}

	// Prepare the statement
	stmt, err := tx.Prepare(query)
	if err != nil {
		if logger != nil {
			logger.Error("Error preparing statement for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	// Update each row
	for i, row := range data {
		values := make([]interface{}, 0, len(columns))

		// Add SET values first
		for _, col := range columns {
			isWhereColumn := false
			for _, whereCol := range whereColumns {
				if col == whereCol {
					isWhereColumn = true
					break
				}
			}
			if !isWhereColumn {
				values = append(values, sanitizeValue(row[col], logger))
			}
		}

		// Add WHERE values
		for _, whereCol := range whereColumns {
			values = append(values, sanitizeValue(row[whereCol], logger))
		}

		result, err := stmt.Exec(values...)
		if err != nil {
			if logger != nil {
				logger.Error("Error executing update for row %d in table %s: %v", i+1, tableName, err)
			}
			return 0, fmt.Errorf("error executing update: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			if logger != nil {
				logger.Error("Error getting rows affected for row %d in table %s: %v", i+1, tableName, err)
			}
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		if logger != nil {
			logger.Error("Error committing transaction for table %s: %v", tableName, err)
		}
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	if logger != nil {
		logger.Info("Successfully updated %d rows in table %s", totalRowsAffected, tableName)
	}

	return totalRowsAffected, nil
}

// WipeDatabase truncates all tables in the database
func WipeDatabase(db *sql.DB, logger *logger.Logger) error {
	if logger != nil {
		logger.Warn("Starting database wipe operation")
	}

	// Get all tables
	rows, err := db.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()`)
	if err != nil {
		if logger != nil {
			logger.Error("Error querying tables for wipe operation: %v", err)
		}
		return err
	}
	defer rows.Close()

	// Disable foreign key checks temporarily
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		if logger != nil {
			logger.Error("Error disabling foreign key checks: %v", err)
		}
		return err
	}
	defer db.Exec("SET FOREIGN_KEY_CHECKS = 1")

	// Truncate all tables
	tableCount := 0
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			if logger != nil {
				logger.Error("Error scanning table name: %v", err)
			}
			return err
		}

		if logger != nil {
			logger.Debug("Truncating table: %s", tableName)
		}

		_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", QuoteIdentifier(tableName)))
		if err != nil {
			if logger != nil {
				logger.Error("Error truncating table %s: %v", tableName, err)
			}
			return fmt.Errorf("error truncating table %s: %v", tableName, err)
		}
		tableCount++
	}

	if logger != nil {
		logger.Info("Successfully wiped database: truncated %d tables", tableCount)
	}

	return nil
}

// getColumns retrieves column names for a table
func getColumns(db *sql.DB, tableName string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = DATABASE() 
		AND TABLE_NAME = '%s' 
		ORDER BY ORDINAL_POSITION`, tableName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}

	return columns, nil
}
