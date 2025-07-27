package oracle

import (
	"database/sql"
	"fmt"
	"strings"
)

// FetchData retrieves data from a specified table
func FetchData(db *sql.DB, tableName string, limit int) ([]map[string]interface{}, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Get columns for the table
	columns, err := getColumns(db, tableName)
	if err != nil {
		return nil, err
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columns, ", "),
		tableName)
	if limit > 0 {
		// Oracle uses ROWNUM for limiting rows
		query = fmt.Sprintf("SELECT * FROM (%s) WHERE ROWNUM <= %d", query, limit)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying table %s: %v", tableName, err)
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			// Handle Oracle-specific NULL values and data type conversions
			if values[i] == nil {
				entry[col] = nil
			} else {
				// Oracle might return some types differently than PostgreSQL
				// For example, CLOB/BLOB types need special handling
				entry[col] = values[i]
			}
		}
		result = append(result, entry)
	}

	return result, nil
}

// InsertData inserts data into a specified table
func InsertData(db *sql.DB, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var totalRowsAffected int64

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Create placeholders for the prepared statement
	// Oracle uses :1, :2, etc. for bind variables
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	// Prepare the statement
	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	))
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Insert each row
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			// Handle special data types for Oracle
			values[i] = convertToOracleType(row[col])
		}

		result, err := stmt.Exec(values...)
		if err != nil {
			return 0, err
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(db *sql.DB) error {
	// Get all tables owned by the current user
	query := `
		SELECT table_name 
		FROM user_tables
		ORDER BY table_name`

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Truncate all tables
	for _, table := range tables {
		_, err = tx.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table))
		if err != nil {
			return err
		}
	}

	// Commit the transaction
	return tx.Commit()
}

func getColumns(db *sql.DB, tableName string) ([]string, error) {
	query := `
		SELECT column_name 
		FROM user_tab_columns 
		WHERE table_name = UPPER(?)
		ORDER BY column_id`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("error querying columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("error scanning column: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// convertToOracleType handles conversion of Go types to Oracle-compatible types
func convertToOracleType(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case bool:
		// Oracle doesn't have a boolean type, so convert to 1/0
		if v {
			return 1
		}
		return 0
	case []byte:
		// For binary data, Oracle uses RAW or BLOB
		return v
	case map[string]interface{}, []interface{}:
		// For JSON data, convert to string
		// In a real implementation, you might want to use a JSON library
		return fmt.Sprintf("%v", v)
	default:
		return v
	}
}
