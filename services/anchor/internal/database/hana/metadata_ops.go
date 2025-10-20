//go:build enterprise
// +build enterprise

package hana

import (
	"context"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata from a SAP HANA database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := m.conn.db.QueryRowContext(ctx, "SELECT VERSION FROM SYS.M_DATABASE").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get database size (data and log volumes)
	var sizeBytes int64
	err = m.conn.db.QueryRowContext(ctx, `
		SELECT SUM(USED_SIZE) 
		FROM SYS.M_VOLUME_FILES
	`).Scan(&sizeBytes)
	if err == nil {
		metadata["size_bytes"] = sizeBytes
	}

	// Get tables count
	var tablesCount int
	err = m.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYS.TABLES 
		WHERE SCHEMA_NAME = CURRENT_SCHEMA
	`).Scan(&tablesCount)
	if err == nil {
		metadata["tables_count"] = tablesCount
	}

	// Get unique identifier (host and port from M_DATABASE)
	var host string
	var port int
	err = m.conn.db.QueryRowContext(ctx,
		"SELECT HOST, SQL_PORT FROM SYS.M_SERVICES WHERE SERVICE_NAME = 'indexserver'").Scan(&host, &port)
	if err == nil {
		metadata["unique_identifier"] = fmt.Sprintf("%s:%d", host, port)
	}

	return metadata, nil
}

// GetVersion returns the database version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.db.QueryRowContext(ctx, "SELECT VERSION FROM SYS.M_DATABASE").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.HANA, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns a unique identifier for this database.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var host string
	var port int
	err := m.conn.db.QueryRowContext(ctx,
		"SELECT HOST, SQL_PORT FROM SYS.M_SERVICES WHERE SERVICE_NAME = 'indexserver'").Scan(&host, &port)
	if err != nil {
		return m.conn.config.Host + ":" + fmt.Sprint(m.conn.config.Port), nil
	}
	return fmt.Sprintf("%s:%d", host, port), nil
}

// CollectInstanceMetadata collects metadata from instance (same as database for single connection).
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return m.CollectDatabaseMetadata(ctx)
}

// GetDatabaseSize returns the database size in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeBytes int64
	err := m.conn.db.QueryRowContext(ctx, `
		SELECT SUM(USED_SIZE) 
		FROM SYS.M_VOLUME_FILES
	`).Scan(&sizeBytes)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.HANA, "get_database_size", err)
	}
	return sizeBytes, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := m.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYS.TABLES 
		WHERE SCHEMA_NAME = CURRENT_SCHEMA
	`).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.HANA, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// HANA admin commands would require special handling
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.HANA, "execute command", "not yet implemented")
}

// InstanceMetadataOps implements adapter.MetadataOperator for instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectInstanceMetadata collects metadata from a SAP HANA instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := i.conn.db.QueryRowContext(ctx, "SELECT VERSION FROM SYS.M_DATABASE").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get uptime
	var startTime time.Time
	err = i.conn.db.QueryRowContext(ctx, "SELECT START_TIME FROM SYS.M_DATABASE").Scan(&startTime)
	if err == nil {
		uptimeSeconds := int64(time.Since(startTime).Seconds())
		metadata["uptime_seconds"] = uptimeSeconds
	}

	// Get total databases (tenant databases)
	var totalDatabases int
	err = i.conn.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM SYS.M_DATABASES").Scan(&totalDatabases)
	if err == nil {
		metadata["total_databases"] = totalDatabases
	}

	// Get current connections
	var totalConnections int
	err = i.conn.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM SYS.M_CONNECTIONS").Scan(&totalConnections)
	if err == nil {
		metadata["total_connections"] = totalConnections
	}

	// Get max connections (sessions)
	var maxConnections int
	err = i.conn.db.QueryRowContext(ctx,
		"SELECT TO_INT(VALUE) FROM SYS.M_INIFILE_CONTENTS WHERE FILE_NAME = 'indexserver.ini' AND SECTION = 'session' AND KEY = 'max_connections'").Scan(&maxConnections)
	if err == nil {
		metadata["max_connections"] = maxConnections
	} else {
		// Default value
		metadata["max_connections"] = 1000
	}

	return metadata, nil
}

// GetVersion returns the database version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.db.QueryRowContext(ctx, "SELECT VERSION FROM SYS.M_DATABASE").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.HANA, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns a unique identifier for this instance.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var host string
	var port int
	err := i.conn.db.QueryRowContext(ctx,
		"SELECT HOST, SQL_PORT FROM SYS.M_SERVICES WHERE SERVICE_NAME = 'indexserver'").Scan(&host, &port)
	if err != nil {
		return i.conn.config.Host + ":" + fmt.Sprint(i.conn.config.Port), nil
	}
	return fmt.Sprintf("%s:%d", host, port), nil
}

// CollectDatabaseMetadata collects database metadata (same as instance for instance connection).
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return i.CollectInstanceMetadata(ctx)
}

// GetDatabaseSize returns the database size in bytes.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeBytes int64
	err := i.conn.db.QueryRowContext(ctx, `
		SELECT SUM(USED_SIZE) 
		FROM SYS.M_VOLUME_FILES
	`).Scan(&sizeBytes)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.HANA, "get_database_size", err)
	}
	return sizeBytes, nil
}

// GetTableCount returns the number of tables in the database.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := i.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYS.TABLES
	`).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.HANA, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// HANA admin commands would require special handling
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.HANA, "execute command", "not yet implemented")
}
