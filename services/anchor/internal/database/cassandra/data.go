package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// FetchData retrieves data from a specified table
func FetchData(session *gocql.Session, tableName string, limit int) ([]map[string]interface{}, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Extract keyspace and table name
	parts := strings.Split(tableName, ".")
	var keyspace, table string
	if len(parts) == 2 {
		keyspace = parts[0]
		table = parts[1]
	} else {
		keyspace = GetKeyspace(session)
		table = tableName
	}

	if keyspace == "" {
		return nil, fmt.Errorf("keyspace not specified and no default keyspace in session")
	}

	// Get columns for the table
	columns, err := getTableColumns(session, keyspace, table)
	if err != nil {
		return nil, err
	}

	// Build column names list
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
	}

	// Build and execute query
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(columnNames, ", "),
		common.QuoteIdentifier(keyspace),
		common.QuoteIdentifier(table))

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	iter := session.Query(query).Iter()

	// Create a result slice
	var result []map[string]interface{}

	// Create a map for each row
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		// Create a copy of the row to add to results
		rowCopy := make(map[string]interface{})
		for k, v := range row {
			rowCopy[k] = v
		}
		result = append(result, rowCopy)

		// Clear the map for the next iteration
		for k := range row {
			delete(row, k)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error fetching data from table %s: %v", tableName, err)
	}

	return result, nil
}

// InsertData inserts data into a specified table
func InsertData(session *gocql.Session, tableName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Extract keyspace and table name
	parts := strings.Split(tableName, ".")
	var keyspace, table string
	if len(parts) == 2 {
		keyspace = parts[0]
		table = parts[1]
	} else {
		keyspace = GetKeyspace(session)
		table = tableName
	}

	if keyspace == "" {
		return 0, fmt.Errorf("keyspace not specified and no default keyspace in session")
	}

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
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		common.QuoteIdentifier(keyspace),
		common.QuoteIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt := session.Query(query)

	// Insert each row
	var totalRowsAffected int64
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		// Execute the statement
		if err := stmt.Bind(values...).Exec(); err != nil {
			return totalRowsAffected, fmt.Errorf("error inserting data: %v", err)
		}

		totalRowsAffected++
	}

	return totalRowsAffected, nil
}

// UpsertData inserts or updates data in a specified table based on unique constraints
// Note: Cassandra doesn't support traditional upserts like SQL databases.
// This implementation uses INSERT which is inherently upsert-like in Cassandra.
func UpsertData(session *gocql.Session, tableName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Extract keyspace and table name
	parts := strings.Split(tableName, ".")
	var keyspace, table string
	if len(parts) == 2 {
		keyspace = parts[0]
		table = parts[1]
	} else {
		keyspace = GetKeyspace(session)
		table = tableName
	}

	if keyspace == "" {
		return 0, fmt.Errorf("keyspace not specified and no default keyspace in session")
	}

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

	// In Cassandra, INSERT is inherently upsert-like
	// We can optionally use IF NOT EXISTS for conditional insertion
	// but for general upsert behavior, we use regular INSERT
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		common.QuoteIdentifier(keyspace),
		common.QuoteIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Prepare the statement
	stmt := session.Query(query)

	// Insert each row
	var totalRowsAffected int64
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		// Execute the statement
		if err := stmt.Bind(values...).Exec(); err != nil {
			return totalRowsAffected, fmt.Errorf("error upserting data: %v", err)
		}

		totalRowsAffected++
	}

	return totalRowsAffected, nil
}

// UpdateData updates existing data in a specified table based on a condition
// Note: Cassandra UPDATE requires that all partition key columns be specified in the WHERE clause
func UpdateData(session *gocql.Session, tableName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Extract keyspace and table name
	parts := strings.Split(tableName, ".")
	var keyspace, table string
	if len(parts) == 2 {
		keyspace = parts[0]
		table = parts[1]
	} else {
		keyspace = GetKeyspace(session)
		table = tableName
	}

	if keyspace == "" {
		return 0, fmt.Errorf("keyspace not specified and no default keyspace in session")
	}

	// Get columns from the first row
	columns := make([]string, 0, len(data[0]))
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Build the SET clause (columns to update)
	setColumns := make([]string, 0, len(columns)-len(whereColumns))
	for _, col := range columns {
		isWhereColumn := false
		for _, whereCol := range whereColumns {
			if col == whereCol {
				isWhereColumn = true
				break
			}
		}
		if !isWhereColumn {
			setColumns = append(setColumns, col)
		}
	}

	// Create placeholders for SET values
	setPlaceholders := make([]string, len(setColumns))
	for i := range setColumns {
		setPlaceholders[i] = "?"
	}

	// Build the WHERE clause
	whereConditions := make([]string, len(whereColumns))
	for i := range whereColumns {
		whereConditions[i] = "?"
	}

	// Prepare the query
	query := fmt.Sprintf(
		"UPDATE %s.%s SET %s WHERE %s",
		common.QuoteIdentifier(keyspace),
		common.QuoteIdentifier(table),
		strings.Join(setColumns, " = ?, ")+" = ?",
		strings.Join(whereColumns, " = ? AND ")+" = ?",
	)

	// Prepare the statement
	stmt := session.Query(query)

	// Update each row
	var totalRowsAffected int64
	for _, row := range data {
		values := make([]interface{}, 0, len(columns))

		// Add SET values first
		for _, col := range setColumns {
			values = append(values, row[col])
		}

		// Add WHERE values
		for _, whereCol := range whereColumns {
			values = append(values, row[whereCol])
		}

		// Execute the statement
		if err := stmt.Bind(values...).Exec(); err != nil {
			return totalRowsAffected, fmt.Errorf("error updating data: %v", err)
		}

		totalRowsAffected++
	}

	return totalRowsAffected, nil
}

// WipeDatabase removes all data from the database
func WipeDatabase(session *gocql.Session) error {
	keyspace := GetKeyspace(session)
	if keyspace == "" {
		return fmt.Errorf("no keyspace specified in session")
	}

	// Get all tables in the keyspace
	iter := session.Query(`
		SELECT table_name 
		FROM system_schema.tables 
		WHERE keyspace_name = ?
	`, keyspace).Iter()

	var tableName string
	var tables []string
	for iter.Scan(&tableName) {
		tables = append(tables, tableName)
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("error fetching tables: %v", err)
	}

	// Truncate all tables
	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE TABLE %s.%s",
			common.QuoteIdentifier(keyspace),
			common.QuoteIdentifier(table))

		if err := session.Query(query).Exec(); err != nil {
			return fmt.Errorf("error truncating table %s: %v", table, err)
		}
	}

	return nil
}

// ConvertCassandraValueToGo converts Cassandra-specific types to Go types
func ConvertCassandraValueToGo(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	// Handle specific Cassandra types
	switch v := value.(type) {
	case gocql.UUID:
		return v.String()
	case []byte:
		return v
	default:
		// For other types, return as is
		return v
	}
}
