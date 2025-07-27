package db2

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
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
		common.QuoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", limit)
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
			entry[col] = values[i]
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
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}

	// Prepare the statement
	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		common.QuoteIdentifier(tableName),
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
			values[i] = row[col]
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
	// Get all tables in the user schemas
	query := `
		SELECT TABSCHEMA, TABNAME 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA NOT LIKE 'SYS%' 
		AND TYPE = 'T'`

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schema, tableName string
		if err := rows.Scan(&schema, &tableName); err != nil {
			return err
		}
		tables = append(tables, fmt.Sprintf("%s.%s", schema, tableName))
	}

	// Truncate all tables
	if len(tables) > 0 {
		// Db2 doesn't support truncating multiple tables in one statement
		// so we need to do it one by one
		for _, table := range tables {
			_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s IMMEDIATE", table))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getColumns(db *sql.DB, tableName string) ([]string, error) {
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

	query := `
		SELECT COLNAME 
		FROM SYSCAT.COLUMNS 
		WHERE TABSCHEMA = ? AND TABNAME = ? 
		ORDER BY COLNO`

	rows, err := db.Query(query, schema, table)
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

	return columns, rows.Err()
}
