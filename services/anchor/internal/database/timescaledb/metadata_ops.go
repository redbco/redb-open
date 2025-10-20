package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
)

// MetadataOps implements metadata operations for TimescaleDB.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about the TimescaleDB database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "timescaledb"

	// Get database name
	var dbName string
	err := m.conn.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName)
	if err == nil {
		metadata["database_name"] = dbName
	}

	// Get TimescaleDB version
	var tsVersion string
	err = m.conn.db.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&tsVersion)
	if err == nil {
		metadata["timescaledb_version"] = tsVersion
	}

	// Get PostgreSQL version
	var pgVersion string
	err = m.conn.db.QueryRowContext(ctx, "SHOW server_version").Scan(&pgVersion)
	if err == nil {
		metadata["postgresql_version"] = pgVersion
	}

	// Count hypertables
	var hypertableCount int
	err = m.conn.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = 'public'").Scan(&hypertableCount)
	if err == nil {
		metadata["hypertable_count"] = hypertableCount
	}

	// Get database size
	size, err := m.GetDatabaseSize(ctx)
	if err == nil {
		metadata["database_size_bytes"] = size
	}

	// Get table count
	tableCount, err := m.GetTableCount(ctx)
	if err == nil {
		metadata["table_count"] = tableCount
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the TimescaleDB instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var db *sql.DB

	if m.conn != nil {
		db = m.conn.db
	} else if m.instanceConn != nil {
		db = m.instanceConn.db
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "timescaledb"

	// Get PostgreSQL version
	var pgVersion string
	err := db.QueryRowContext(ctx, "SHOW server_version").Scan(&pgVersion)
	if err == nil {
		metadata["postgresql_version"] = pgVersion
	}

	// Get TimescaleDB version
	var tsVersion string
	err = db.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb' LIMIT 1").Scan(&tsVersion)
	if err == nil {
		metadata["timescaledb_version"] = tsVersion
	}

	// List all databases
	if m.instanceConn != nil {
		databases, err := m.instanceConn.ListDatabases(ctx)
		if err == nil {
			metadata["database_count"] = len(databases)
			metadata["databases"] = databases
		}
	}

	return metadata, nil
}

// GetVersion returns the TimescaleDB and PostgreSQL versions.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var db *sql.DB

	if m.conn != nil {
		db = m.conn.db
	} else if m.instanceConn != nil {
		db = m.instanceConn.db
	} else {
		return "", fmt.Errorf("no connection available")
	}

	var tsVersion, pgVersion string
	err := db.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&tsVersion)
	if err != nil {
		return "", fmt.Errorf("failed to get TimescaleDB version: %w", err)
	}

	err = db.QueryRowContext(ctx, "SHOW server_version").Scan(&pgVersion)
	if err != nil {
		return fmt.Sprintf("TimescaleDB %s", tsVersion), nil
	}

	return fmt.Sprintf("TimescaleDB %s (PostgreSQL %s)", tsVersion, pgVersion), nil
}

// GetUniqueIdentifier returns a unique identifier for the database.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		var dbName string
		err := m.conn.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&dbName)
		if err != nil {
			return "", fmt.Errorf("failed to get database name: %w", err)
		}
		return fmt.Sprintf("timescaledb::%s", dbName), nil
	}

	if m.instanceConn != nil {
		return "timescaledb::instance", nil
	}

	return "timescaledb::unknown", nil
}

// GetDatabaseSize returns the total size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	var size int64
	query := "SELECT pg_database_size(current_database())"
	err := m.conn.db.QueryRowContext(ctx, query).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to get database size: %w", err)
	}

	return size, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	var count int
	query := "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'"
	err := m.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get table count: %w", err)
	}

	return count, nil
}

// ExecuteCommand executes a SQL command and returns the result.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	var db *sql.DB

	if m.conn != nil {
		db = m.conn.db
	} else if m.instanceConn != nil {
		db = m.instanceConn.db
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	rows, err := db.QueryContext(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}
	defer rows.Close()

	// Simple text output
	var result string
	columns, err := rows.Columns()
	if err == nil {
		result += fmt.Sprintf("%v\n", columns)
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err == nil {
			result += fmt.Sprintf("%v\n", values)
		}
	}

	return []byte(result), nil
}

// GetHypertableInfo returns information about all hypertables.
func (m *MetadataOps) GetHypertableInfo(ctx context.Context) ([]map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	query := `
		SELECT 
			h.table_name,
			h.num_dimensions,
			d.column_name as time_column,
			c.compression_state
		FROM _timescaledb_catalog.hypertable h
		JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
		LEFT JOIN _timescaledb_catalog.continuous_agg c ON h.id = c.mat_hypertable_id
		WHERE h.schema_name = 'public' AND d.column_type = 'time'
		ORDER BY h.table_name
	`

	rows, err := m.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get hypertable info: %w", err)
	}
	defer rows.Close()

	var hypertables []map[string]interface{}

	for rows.Next() {
		var tableName, timeColumn string
		var numDimensions int
		var compressionState sql.NullString

		if err := rows.Scan(&tableName, &numDimensions, &timeColumn, &compressionState); err != nil {
			continue
		}

		info := map[string]interface{}{
			"table_name":     tableName,
			"num_dimensions": numDimensions,
			"time_column":    timeColumn,
		}

		if compressionState.Valid {
			info["compression_state"] = compressionState.String
		}

		hypertables = append(hypertables, info)
	}

	return hypertables, rows.Err()
}
