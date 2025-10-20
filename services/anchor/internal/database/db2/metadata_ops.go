//go:build enterprise
// +build enterprise

package db2

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata from an IBM DB2 database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := m.conn.db.QueryRowContext(ctx,
		"SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get database size (in bytes)
	var sizeBytes int64
	err = m.conn.db.QueryRowContext(ctx, `
		SELECT SUM(TBSP_USED_SIZE_KB) * 1024 
		FROM TABLE(MON_GET_TABLESPACE('', -1)) AS T`).Scan(&sizeBytes)
	if err == nil {
		metadata["size_bytes"] = sizeBytes
	}

	// Get tables count
	var tablesCount int
	err = m.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA NOT LIKE 'SYS%'`).Scan(&tablesCount)
	if err == nil {
		metadata["tables_count"] = tablesCount
	}

	// Get unique identifier (database name + instance name)
	var dbName, instanceName string
	err = m.conn.db.QueryRowContext(ctx,
		"SELECT current database, current server FROM SYSIBMADM.ENV_SYS_INFO").Scan(&dbName, &instanceName)
	if err == nil {
		metadata["unique_identifier"] = fmt.Sprintf("%s_%s", instanceName, dbName)
		metadata["database_name"] = dbName
		metadata["instance_name"] = instanceName
	}

	return metadata, nil
}

// GetVersion returns the database version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.db.QueryRowContext(ctx,
		"SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.DB2, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns a unique identifier for this database.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var dbName, instanceName string
	err := m.conn.db.QueryRowContext(ctx,
		"SELECT current database, current server FROM SYSIBMADM.ENV_SYS_INFO").Scan(&dbName, &instanceName)
	if err != nil {
		return m.conn.config.Host + ":" + fmt.Sprint(m.conn.config.Port), nil
	}
	return fmt.Sprintf("%s_%s", instanceName, dbName), nil
}

// CollectInstanceMetadata collects metadata from instance (same as database for single connection).
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return m.CollectDatabaseMetadata(ctx)
}

// GetDatabaseSize returns the database size in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeBytes int64
	err := m.conn.db.QueryRowContext(ctx, `
		SELECT SUM(TBSP_USED_SIZE_KB) * 1024 
		FROM TABLE(MON_GET_TABLESPACE('', -1)) AS T`).Scan(&sizeBytes)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "get_database_size", err)
	}
	return sizeBytes, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := m.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA NOT LIKE 'SYS%'`).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// DB2 admin commands would require special handling
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.DB2, "execute command", "not yet implemented")
}

// InstanceMetadataOps implements adapter.MetadataOperator for instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectInstanceMetadata collects metadata from an IBM DB2 instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := i.conn.db.QueryRowContext(ctx,
		"SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err == nil {
		metadata["version"] = version
	}

	// Get uptime (in seconds)
	var uptimeSeconds int64
	err = i.conn.db.QueryRowContext(ctx, `
		SELECT (CURRENT TIMESTAMP - DB_CONN_TIME) SECONDS 
		FROM TABLE(MON_GET_DATABASE(-1)) AS T`).Scan(&uptimeSeconds)
	if err == nil {
		metadata["uptime_seconds"] = uptimeSeconds
	} else {
		// If we can't get uptime, set it to 0
		metadata["uptime_seconds"] = 0
	}

	// Get total databases in the instance
	var totalDatabases int
	err = i.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM TABLE(ADMIN_LIST_DB_PARTITION_GROUPS()) AS T`).Scan(&totalDatabases)
	if err == nil {
		metadata["total_databases"] = totalDatabases
	}

	// Get current connections
	var totalConnections int
	err = i.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM TABLE(MON_GET_CONNECTION(NULL, -1)) AS T`).Scan(&totalConnections)
	if err == nil {
		metadata["total_connections"] = totalConnections
	}

	// Get max connections
	var maxConnectionsStr string
	err = i.conn.db.QueryRowContext(ctx,
		"SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'max_connections'").Scan(&maxConnectionsStr)
	if err == nil {
		if maxConnections, err := strconv.Atoi(maxConnectionsStr); err == nil {
			metadata["max_connections"] = maxConnections
		}
	}

	return metadata, nil
}

// GetVersion returns the database version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.db.QueryRowContext(ctx,
		"SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.DB2, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns a unique identifier for this instance.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var dbName, instanceName string
	err := i.conn.db.QueryRowContext(ctx,
		"SELECT current database, current server FROM SYSIBMADM.ENV_SYS_INFO").Scan(&dbName, &instanceName)
	if err != nil {
		return i.conn.config.Host + ":" + fmt.Sprint(i.conn.config.Port), nil
	}
	return fmt.Sprintf("%s_%s", instanceName, dbName), nil
}

// CollectDatabaseMetadata collects database metadata (same as instance for instance connection).
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return i.CollectInstanceMetadata(ctx)
}

// GetDatabaseSize returns the database size in bytes.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeBytes int64
	err := i.conn.db.QueryRowContext(ctx, `
		SELECT SUM(TBSP_USED_SIZE_KB) * 1024 
		FROM TABLE(MON_GET_TABLESPACE('', -1)) AS T`).Scan(&sizeBytes)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "get_database_size", err)
	}
	return sizeBytes, nil
}

// GetTableCount returns the number of tables in the database.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := i.conn.db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA NOT LIKE 'SYS%'`).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DB2, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// DB2 admin commands would require special handling
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.DB2, "execute command", "not yet implemented")
}
