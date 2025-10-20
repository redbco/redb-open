package tidb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// MetadataOps implements adapter.MetadataOperator for TiDB.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata retrieves metadata about the TiDB database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	var conn *sql.DB
	var dbName string

	if m.conn != nil {
		if !m.conn.IsConnected() {
			return nil, adapter.ErrConnectionClosed
		}
		conn = m.conn.db
		dbName = m.conn.config.DatabaseName
	} else if m.instanceConn != nil {
		if !m.instanceConn.IsConnected() {
			return nil, adapter.ErrConnectionClosed
		}
		conn = m.instanceConn.db
		dbName = ""
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})

	// Get TiDB version
	var version string
	err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get server uptime
	var uptime int64
	err = conn.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME='Uptime'").Scan(&uptime)
	if err == nil {
		metadata["uptime_seconds"] = uptime
	}

	// Get database size if we have a specific database
	if dbName != "" {
		query := `
			SELECT 
				ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as size_mb
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ?
		`
		var sizeMB sql.NullFloat64
		err = conn.QueryRowContext(ctx, query, dbName).Scan(&sizeMB)
		if err == nil && sizeMB.Valid {
			metadata["size_mb"] = sizeMB.Float64
		}

		// Get table count
		var tableCount int
		err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", dbName).Scan(&tableCount)
		if err == nil {
			metadata["table_count"] = tableCount
		}
	}

	// Get PD servers
	var pdAddrs string
	err = conn.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME='pd_servers'").Scan(&pdAddrs)
	if err == nil && pdAddrs != "" {
		metadata["pd_servers"] = pdAddrs
	}

	metadata["storage_engine"] = "TiKV"

	return metadata, nil
}

// CollectInstanceMetadata retrieves metadata about the TiDB instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return m.CollectDatabaseMetadata(ctx)
}

// GetVersion returns the TiDB version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var conn *sql.DB

	if m.conn != nil {
		if !m.conn.IsConnected() {
			return "", adapter.ErrConnectionClosed
		}
		conn = m.conn.db
	} else if m.instanceConn != nil {
		if !m.instanceConn.IsConnected() {
			return "", adapter.ErrConnectionClosed
		}
		conn = m.instanceConn.db
	} else {
		return "", fmt.Errorf("no connection available")
	}

	var version string
	err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	return version, err
}

// GetUniqueIdentifier returns a unique identifier for the TiDB instance.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var conn *sql.DB

	if m.conn != nil {
		if !m.conn.IsConnected() {
			return "", adapter.ErrConnectionClosed
		}
		conn = m.conn.db
	} else if m.instanceConn != nil {
		if !m.instanceConn.IsConnected() {
			return "", adapter.ErrConnectionClosed
		}
		conn = m.instanceConn.db
	} else {
		return "", fmt.Errorf("no connection available")
	}

	// Get server UUID
	var uuid string
	err := conn.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME='server_uuid'").Scan(&uuid)
	return uuid, err
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	if m.conn == nil || !m.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	query := `
		SELECT COALESCE(SUM(data_length + index_length), 0)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
	`

	var size int64
	err := m.conn.db.QueryRowContext(ctx, query, m.conn.config.DatabaseName).Scan(&size)
	return size, err
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil || !m.conn.IsConnected() {
		return 0, adapter.ErrConnectionClosed
	}

	var count int
	err := m.conn.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
		m.conn.config.DatabaseName,
	).Scan(&count)
	return count, err
}

// ExecuteCommand executes a TiDB admin command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	if m.conn == nil || !m.conn.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	// Execute command and return result as JSON
	rows, err := m.conn.db.QueryContext(ctx, command)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// For simplicity, just return success message
	return []byte(fmt.Sprintf(`{"status":"executed","command":"%s"}`, command)), nil
}
