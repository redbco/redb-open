package mariadb

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for MariaDB database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata about the MariaDB database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectDatabaseMetadata function
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "collect_database_metadata", err)
	}
	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the MariaDB instance.
// Note: This is called on a database connection, not an instance connection.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectInstanceMetadata function
	metadata, err := CollectInstanceMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the MariaDB version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.MariaDB, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns the unique identifier for the MariaDB instance.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var serverUUID string
	err := m.conn.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&serverUUID)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.MariaDB, "get_unique_identifier", err)
	}
	return serverUUID, nil
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	query := `
		SELECT IFNULL(SUM(data_length + index_length), 0) 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()
	`
	var size int64
	err := m.conn.db.QueryRowContext(ctx, query).Scan(&size)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MariaDB, "get_database_size", err)
	}
	return size, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()"
	err := m.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.MariaDB, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// MariaDB doesn't have a direct ExecuteCommand function
	// Execute as a query and return results as JSON-like format
	rows, err := m.conn.db.QueryContext(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "execute_command", err)
	}
	defer rows.Close()

	// Simple implementation: return success message
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

// CollectInstanceMetrics collects performance metrics (not available on database connection)
func (m *MetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.MariaDB,
		"collect instance metrics",
		"not available on database connections",
	)
}

// ListLogicalDatabases lists logical databases (not available on database connection)
func (m *MetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.MariaDB,
		"list logical databases",
		"not available on database connections",
	)
}

// InstanceMetadataOps implements adapter.MetadataOperator for MariaDB instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectDatabaseMetadata is not applicable for instance connections.
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.MariaDB,
		"collect database metadata",
		"not available on instance connections",
	)
}

// CollectInstanceMetadata collects metadata about the MariaDB instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectInstanceMetadata function
	metadata, err := CollectInstanceMetadata(ctx, i.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the MariaDB version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.MariaDB, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns the unique identifier for the MariaDB instance.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var serverUUID string
	err := i.conn.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&serverUUID)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.MariaDB, "get_unique_identifier", err)
	}
	return serverUUID, nil
}

// GetDatabaseSize is not applicable for instance connections.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.MariaDB,
		"get database size",
		"not available on instance connections",
	)
}

// GetTableCount is not applicable for instance connections.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.MariaDB,
		"get table count",
		"not available on instance connections",
	)
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// Execute as a query and return results as JSON-like format
	rows, err := i.conn.db.QueryContext(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "execute_command", err)
	}
	defer rows.Close()

	// Simple implementation: return success message
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

// CollectInstanceMetrics collects performance metrics from the MariaDB instance
func (i *InstanceMetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// Connection metrics - similar to MySQL
	var threadsConnected, threadsRunning int32
	err := i.conn.db.QueryRowContext(ctx,
		"SELECT VARIABLE_VALUE FROM information_schema.global_status WHERE VARIABLE_NAME = 'Threads_connected'").Scan(&threadsConnected)
	if err == nil {
		i.conn.db.QueryRowContext(ctx,
			"SELECT VARIABLE_VALUE FROM information_schema.global_status WHERE VARIABLE_NAME = 'Threads_running'").Scan(&threadsRunning)
		metrics["active_connections"] = threadsRunning
		metrics["idle_connections"] = threadsConnected - threadsRunning
	}

	// Cache hit ratio from InnoDB buffer pool
	var bufferPoolReads, bufferPoolReadRequests int64
	i.conn.db.QueryRowContext(ctx,
		"SELECT VARIABLE_VALUE FROM information_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads'").Scan(&bufferPoolReads)
	i.conn.db.QueryRowContext(ctx,
		"SELECT VARIABLE_VALUE FROM information_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests'").Scan(&bufferPoolReadRequests)
	if bufferPoolReadRequests > 0 {
		hitRatio := (1.0 - float64(bufferPoolReads)/float64(bufferPoolReadRequests)) * 100.0
		metrics["cache_hit_ratio"] = hitRatio
	}

	// Replication status
	rows, err := i.conn.db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			metrics["is_replica"] = true
			// Note: Full parsing would require column mapping, simplified here
		} else {
			metrics["is_replica"] = false
		}
	}

	return metrics, nil
}

// ListLogicalDatabases lists all logical databases in the MariaDB instance
func (i *InstanceMetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	query := `
		SELECT 
			schema_name,
			COALESCE(SUM(data_length + index_length), 0) as size_bytes,
			DEFAULT_CHARACTER_SET_NAME as encoding,
			DEFAULT_COLLATION_NAME as collation
		FROM information_schema.schemata s
		LEFT JOIN information_schema.tables t ON s.schema_name = t.table_schema
		WHERE s.schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		GROUP BY schema_name, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
		ORDER BY schema_name
	`

	rows, err := i.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "list_databases", err)
	}
	defer rows.Close()

	var databases []adapter.LogicalDatabaseInfo
	for rows.Next() {
		var db adapter.LogicalDatabaseInfo
		err := rows.Scan(&db.Name, &db.SizeBytes, &db.Encoding, &db.Collation)
		if err != nil {
			continue
		}
		db.Owner = "root" // MariaDB doesn't have database owners like PostgreSQL
		databases = append(databases, db)
	}

	if err := rows.Err(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.MariaDB, "list_databases", err)
	}

	return databases, nil
}
