package mssql

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
		query += fmt.Sprintf(" TOP %d", limit)
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
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
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
	// Get all user tables
	query := `
		SELECT schema_name(schema_id) + '.' + name AS table_name
		FROM sys.tables
		WHERE type = 'U'
	`

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

	// Disable all constraints
	_, err = db.Exec("EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'")
	if err != nil {
		return err
	}

	// Truncate all tables
	for _, table := range tables {
		_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table))
		if err != nil {
			return err
		}
	}

	// Re-enable all constraints
	_, err = db.Exec("EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'")
	if err != nil {
		return err
	}

	return nil
}

func getColumns(db *sql.DB, tableName string) ([]string, error) {
	// Split schema and table name if provided
	parts := strings.Split(tableName, ".")
	var schema, table string
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	} else {
		schema = "dbo"
		table = tableName
	}

	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = @p1 AND table_name = @p2
		ORDER BY ordinal_position
	`
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
