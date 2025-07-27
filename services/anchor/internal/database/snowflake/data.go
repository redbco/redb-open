package snowflake

import (
	"context"
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
		quoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.QueryContext(context.Background(), query)
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
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
		quoteIdentifier(tableName),
		strings.Join(quoteIdentifiers(columns), ", "),
		strings.Join(placeholders, ", "),
	))
	if err != nil {
		return 0, fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Insert each row
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			return 0, fmt.Errorf("error executing insert: %v", err)
		}

		// Snowflake doesn't support RowsAffected, so we'll just increment by 1 for each successful insert
		totalRowsAffected++
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %v", err)
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(db *sql.DB) error {
	// Get all tables in the current schema
	query := `
		SELECT TABLE_NAME 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = CURRENT_SCHEMA() 
		AND TABLE_TYPE = 'BASE TABLE';`

	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("error scanning table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating tables: %v", err)
	}

	// Truncate all tables
	if len(tables) > 0 {
		// Snowflake doesn't support truncating multiple tables in one statement
		// So we'll execute separate statements for each table
		for _, table := range tables {
			_, err = db.ExecContext(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s;", quoteIdentifier(table)))
			if err != nil {
				return fmt.Errorf("error truncating table %s: %v", table, err)
			}
		}
	}

	return nil
}

// BulkInsertData performs a bulk insert operation using Snowflake's COPY command
func BulkInsertData(db *sql.DB, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Create a temporary stage for the data
	stageName := fmt.Sprintf("%s_temp_stage", tableName)
	_, err := db.Exec(fmt.Sprintf("CREATE OR REPLACE TEMPORARY STAGE %s", stageName))
	if err != nil {
		return 0, fmt.Errorf("error creating temporary stage: %v", err)
	}

	// Put the data into the stage (this would typically use Snowflake's PUT command)
	// For simplicity, we'll use a direct INSERT instead of the staging process

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare the statement
	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(tableName),
		strings.Join(quoteIdentifiers(columns), ", "),
		strings.Join(generatePlaceholders(len(columns)), ", "),
	))
	if err != nil {
		return 0, fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	// Insert each row
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		_, err := stmt.Exec(values...)
		if err != nil {
			return 0, fmt.Errorf("error executing insert: %v", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %v", err)
	}

	return int64(len(data)), nil
}

// ExportData exports data from a table to a specified format
func ExportData(db *sql.DB, tableName string, format string, limit int) ([]byte, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", quoteIdentifier(tableName))
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	// For Snowflake, we can use the TO_<format> functions to convert data
	switch strings.ToUpper(format) {
	case "JSON":
		query = fmt.Sprintf("SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM (%s)", query)
	case "CSV":
		// We'll handle CSV formatting in Go
	case "PARQUET", "AVRO", "ORC":
		return nil, fmt.Errorf("export to %s format not supported directly, use Snowflake's COPY command", format)
	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}

	// Execute query
	var result []byte
	if strings.ToUpper(format) == "JSON" {
		var jsonData string
		err := db.QueryRow(query).Scan(&jsonData)
		if err != nil {
			return nil, fmt.Errorf("error executing export query: %v", err)
		}
		result = []byte(jsonData)
	} else {
		// Fetch data and format as CSV
		data, err := FetchData(db, tableName, limit)
		if err != nil {
			return nil, err
		}

		if len(data) == 0 {
			return []byte{}, nil
		}

		// Get columns
		columns := make([]string, 0, len(data[0]))
		for col := range data[0] {
			columns = append(columns, col)
		}

		// Format as CSV
		result = []byte(formatDataAsCSV(data, columns))
	}

	return result, nil
}

// Helper functions

func getColumns(db *sql.DB, tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ? 
		AND TABLE_SCHEMA = CURRENT_SCHEMA()
		ORDER BY ORDINAL_POSITION`

	rows, err := db.QueryContext(context.Background(), query, tableName)
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
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	return columns, nil
}

func quoteIdentifier(identifier string) string {
	// In Snowflake, identifiers are quoted with double quotes
	return fmt.Sprintf(`"%s"`, identifier)
}

func quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = quoteIdentifier(id)
	}
	return quoted
}

func generatePlaceholders(count int) []string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return placeholders
}

func formatDataAsCSV(data []map[string]interface{}, columns []string) string {
	var sb strings.Builder

	// Write header
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(escapeCSV(col))
	}
	sb.WriteString("\n")

	// Write data
	for _, row := range data {
		for i, col := range columns {
			if i > 0 {
				sb.WriteString(",")
			}
			value := fmt.Sprintf("%v", row[col])
			sb.WriteString(escapeCSV(value))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func escapeCSV(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(s, "\"", "\"\""))
	}
	return s
}
