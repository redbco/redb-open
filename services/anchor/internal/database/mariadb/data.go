package mariadb

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
		QuoteIdentifier(tableName))
	if limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	// Process rows
	var result []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the slice of interface{}
		if err := rows.Scan(valuePtrs...); err != nil {
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
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

// InsertData inserts data into a specified table
func InsertData(db *sql.DB, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get all column names from the first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Prepare placeholders for the query
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		QuoteIdentifier(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// Prepare the statement
	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row
	var totalRowsAffected int64
	for _, row := range data {
		// Extract values in the same order as columns
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		// Execute the statement
		result, err := stmt.Exec(values...)
		if err != nil {
			return 0, fmt.Errorf("error executing insert: %w", err)
		}

		// Get rows affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	return totalRowsAffected, nil
}

// WipeDatabase truncates all tables in the database
func WipeDatabase(db *sql.DB) error {
	// Get all tables
	rows, err := db.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Disable foreign key checks temporarily
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		return err
	}
	defer db.Exec("SET FOREIGN_KEY_CHECKS = 1")

	// Truncate all tables
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}

		_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", QuoteIdentifier(tableName)))
		if err != nil {
			return fmt.Errorf("error truncating table %s: %v", tableName, err)
		}
	}

	return nil
}

// BatchInsertData inserts data in batches for better performance
func BatchInsertData(db *sql.DB, tableName string, data []map[string]interface{}, batchSize int) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	// Get all column names from the first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	var totalRowsAffected int64
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]

		// Build the multi-value insert query
		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s (%s) VALUES ",
			QuoteIdentifier(tableName),
			strings.Join(columns, ", "))

		// Create placeholders for each row
		placeholderGroups := make([]string, len(batch))
		for j := range batch {
			placeholders := make([]string, len(columns))
			for k := range placeholders {
				placeholders[k] = "?"
			}
			placeholderGroups[j] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
		}

		query.WriteString(strings.Join(placeholderGroups, ", "))

		// Prepare values for all rows
		values := make([]interface{}, 0, len(batch)*len(columns))
		for _, row := range batch {
			for _, col := range columns {
				values = append(values, row[col])
			}
		}

		// Execute the batch insert
		result, err := tx.Exec(query.String(), values...)
		if err != nil {
			return 0, fmt.Errorf("error executing batch insert: %w", err)
		}

		// Get rows affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	return totalRowsAffected, nil
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

// UpsertData inserts or updates data in a specified table based on unique constraints
func UpsertData(db *sql.DB, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

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

	// Prepare the query
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		QuoteIdentifier(tableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateSet, ", "),
	)

	// Prepare the statement
	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("error preparing statement: %w", err)
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
			return 0, fmt.Errorf("error executing upsert: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	return totalRowsAffected, nil
}

// UpdateData updates existing data in a specified table based on a condition
func UpdateData(db *sql.DB, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	var totalRowsAffected int64

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
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

	// Prepare the statement
	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	// Update each row
	for _, row := range data {
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
				values = append(values, row[col])
			}
		}

		// Add WHERE values
		for _, whereCol := range whereColumns {
			values = append(values, row[whereCol])
		}

		result, err := stmt.Exec(values...)
		if err != nil {
			return 0, fmt.Errorf("error executing update: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		totalRowsAffected += rowsAffected
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	return totalRowsAffected, nil
}

// DeleteData deletes data from a specified table
func DeleteData(db *sql.DB, tableName string, whereClause string, whereArgs ...interface{}) (int64, error) {
	if tableName == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	// Build the query
	query := fmt.Sprintf("DELETE FROM %s", QuoteIdentifier(tableName))

	// Add WHERE clause if provided
	if whereClause != "" {
		query = fmt.Sprintf("%s WHERE %s", query, whereClause)
	}

	// Execute the delete
	result, err := db.Exec(query, whereArgs...)
	if err != nil {
		return 0, fmt.Errorf("error executing delete: %w", err)
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return rowsAffected, nil
}
