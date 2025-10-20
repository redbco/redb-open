//go:build enterprise
// +build enterprise

package oracle

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

// CollectDatabaseMetadata collects metadata from an Oracle database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := m.conn.db.QueryRowContext(ctx,
		"SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get database size
	var sizeBytes int64
	err = m.conn.db.QueryRowContext(ctx, `
		SELECT SUM(bytes) 
		FROM dba_data_files
	`).Scan(&sizeBytes)
	if err != nil {
		// Fallback to user_data_files
		err = m.conn.db.QueryRowContext(ctx, `
			SELECT SUM(bytes) 
			FROM user_data_files
		`).Scan(&sizeBytes)
	}
	if err == nil {
		metadata["size_bytes"] = sizeBytes
	}

	// Get tables count
	var tablesCount int
	err = m.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM user_tables
	`).Scan(&tablesCount)
	if err == nil {
		metadata["tables_count"] = tablesCount
	}

	// Get unique identifier (DBID)
	var dbid string
	err = m.conn.db.QueryRowContext(ctx, "SELECT dbid FROM v$database").Scan(&dbid)
	if err == nil {
		metadata["unique_identifier"] = dbid
		metadata["dbid"] = dbid
	}

	return metadata, nil
}

// GetVersion returns the database version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.db.QueryRowContext(ctx, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Oracle, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns a unique identifier for this database.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var dbid string
	err := m.conn.db.QueryRowContext(ctx, "SELECT dbid FROM v$database").Scan(&dbid)
	if err != nil {
		return m.conn.config.Host + ":" + fmt.Sprint(m.conn.config.Port), nil
	}
	return dbid, nil
}

// CollectInstanceMetadata collects metadata from instance (same as database for single connection).
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return m.CollectDatabaseMetadata(ctx)
}

// GetDatabaseSize returns the database size in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeBytes int64
	err := m.conn.db.QueryRowContext(ctx, `
		SELECT SUM(bytes) 
		FROM dba_data_files
	`).Scan(&sizeBytes)
	if err != nil {
		// Fallback to user_data_files
		err = m.conn.db.QueryRowContext(ctx, `
			SELECT SUM(bytes) 
			FROM user_data_files
		`).Scan(&sizeBytes)
	}
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "get_database_size", err)
	}
	return sizeBytes, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := m.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM user_tables
	`).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// Oracle admin commands would require special handling
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.Oracle,
		"execute command",
		"not yet implemented",
	)
}

// InstanceMetadataOps implements adapter.MetadataOperator for instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectInstanceMetadata collects metadata from an Oracle instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := i.conn.db.QueryRowContext(ctx,
		"SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get uptime
	var startupTime time.Time
	err = i.conn.db.QueryRowContext(ctx, "SELECT startup_time FROM v$instance").Scan(&startupTime)
	if err == nil {
		uptimeSeconds := int64(time.Since(startupTime).Seconds())
		metadata["uptime_seconds"] = uptimeSeconds
	}

	// Get total databases (in Oracle, typically just 1 per instance, but count PDBs)
	var totalDatabases int
	err = i.conn.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM v$pdbs").Scan(&totalDatabases)
	if err == nil {
		metadata["total_databases"] = totalDatabases
	} else {
		metadata["total_databases"] = 1
	}

	// Get current connections
	var totalConnections int
	err = i.conn.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM v$session WHERE type = 'USER'").Scan(&totalConnections)
	if err == nil {
		metadata["total_connections"] = totalConnections
	}

	// Get max connections
	var maxConnections int
	err = i.conn.db.QueryRowContext(ctx,
		"SELECT value FROM v$parameter WHERE name = 'processes'").Scan(&maxConnections)
	if err == nil {
		metadata["max_connections"] = maxConnections
	}

	return metadata, nil
}

// GetVersion returns the database version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.db.QueryRowContext(ctx, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Oracle, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns a unique identifier for this instance.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var dbid string
	err := i.conn.db.QueryRowContext(ctx, "SELECT dbid FROM v$database").Scan(&dbid)
	if err != nil {
		return i.conn.config.Host + ":" + fmt.Sprint(i.conn.config.Port), nil
	}
	return dbid, nil
}

// CollectDatabaseMetadata collects database metadata (same as instance for instance connection).
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return i.CollectInstanceMetadata(ctx)
}

// GetDatabaseSize returns the database size in bytes.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeBytes int64
	err := i.conn.db.QueryRowContext(ctx, `
		SELECT SUM(bytes) 
		FROM dba_data_files
	`).Scan(&sizeBytes)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "get_database_size", err)
	}
	return sizeBytes, nil
}

// GetTableCount returns the number of tables in the database.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := i.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM dba_tables
	`).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Oracle, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// Oracle admin commands would require special handling
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.Oracle,
		"execute command",
		"not yet implemented",
	)
}
