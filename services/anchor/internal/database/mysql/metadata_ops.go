package mysql

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for MySQL database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata about the database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "collect_database_metadata", err)
	}
	return metadata, nil
}

// CollectInstanceMetadata is not applicable for database connections.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"metadata",
		"instance metadata collection not supported on database connection",
	)
}

// GetVersion returns the MySQL version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	metadata, err := m.CollectDatabaseMetadata(ctx)
	if err != nil {
		return "", err
	}

	if version, ok := metadata["version"].(string); ok {
		return version, nil
	}

	return "", adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"version",
		"version not found in metadata",
	)
}

// GetUniqueIdentifier returns a unique identifier for the database.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	metadata, err := m.CollectDatabaseMetadata(ctx)
	if err != nil {
		return "", err
	}

	if id, ok := metadata["unique_identifier"].(string); ok {
		return id, nil
	}

	return "", nil // Not all databases have unique identifiers
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	metadata, err := m.CollectDatabaseMetadata(ctx)
	if err != nil {
		return 0, err
	}

	if size, ok := metadata["size_bytes"].(int64); ok {
		return size, nil
	}
	if size, ok := metadata["size_bytes"].(float64); ok {
		return int64(size), nil
	}

	return 0, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	metadata, err := m.CollectDatabaseMetadata(ctx)
	if err != nil {
		return 0, err
	}

	if count, ok := metadata["tables_count"].(int); ok {
		return count, nil
	}
	if count, ok := metadata["tables_count"].(float64); ok {
		return int(count), nil
	}

	return 0, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, m.conn.db, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "execute_command", err)
	}
	return result, nil
}

// CollectInstanceMetrics collects performance metrics (not available on database connection)
func (m *MetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"metadata",
		"instance metrics collection not supported on database connection",
	)
}

// ListLogicalDatabases lists logical databases (not available on database connection)
func (m *MetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"metadata",
		"list databases not supported on database connection",
	)
}

// InstanceMetadataOps implements adapter.MetadataOperator for MySQL instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectDatabaseMetadata is not applicable for instance connections.
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"metadata",
		"database metadata collection not supported on instance connection",
	)
}

// CollectInstanceMetadata collects metadata about the database instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the MySQL version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	metadata, err := i.CollectInstanceMetadata(ctx)
	if err != nil {
		return "", err
	}

	if version, ok := metadata["version"].(string); ok {
		return version, nil
	}

	return "", adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"version",
		"version not found in metadata",
	)
}

// GetUniqueIdentifier returns a unique identifier for the instance.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	metadata, err := i.CollectInstanceMetadata(ctx)
	if err != nil {
		return "", err
	}

	if id, ok := metadata["unique_identifier"].(string); ok {
		return id, nil
	}

	return "", nil
}

// GetDatabaseSize is not applicable for instance connections.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"metadata",
		"database size not applicable for instance connection",
	)
}

// GetTableCount is not applicable for instance connections.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewConfigurationError(
		dbcapabilities.MySQL,
		"metadata",
		"table count not applicable for instance connection",
	)
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, i.conn.db, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "execute_command", err)
	}
	return result, nil
}

// CollectInstanceMetrics collects performance metrics from the MySQL instance
func (i *InstanceMetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// Connection metrics
	connQuery := `
		SELECT 
			VARIABLE_VALUE 
		FROM performance_schema.global_status 
		WHERE VARIABLE_NAME IN ('Threads_connected', 'Threads_running', 'Max_used_connections')
	`
	rows, err := i.conn.db.QueryContext(ctx, connQuery)
	if err == nil {
		defer rows.Close()
		var connected, running int32
		idx := 0
		for rows.Next() {
			var val int32
			if rows.Scan(&val) == nil {
				switch idx {
				case 0:
					connected = val
				case 1:
					running = val
				}
				idx++
			}
		}
		metrics["active_connections"] = running
		metrics["idle_connections"] = connected - running
	}

	// InnoDB buffer pool hit ratio
	bufferQuery := `
		SELECT 
			(1 - (Innodb_buffer_pool_reads / Innodb_buffer_pool_read_requests)) * 100 as hit_ratio
		FROM 
			(SELECT VARIABLE_VALUE as Innodb_buffer_pool_reads FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads') r,
			(SELECT VARIABLE_VALUE as Innodb_buffer_pool_read_requests FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests') rr
	`
	var hitRatio float64
	if err := i.conn.db.QueryRowContext(ctx, bufferQuery).Scan(&hitRatio); err == nil {
		metrics["cache_hit_ratio"] = hitRatio
	}

	// Replication status
	var slaveStatus map[string]interface{}
	rows, err = i.conn.db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err == nil {
		defer rows.Close()
		if rows.Next() {
			cols, _ := rows.Columns()
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if rows.Scan(valPtrs...) == nil {
				slaveStatus = make(map[string]interface{})
				for i, col := range cols {
					slaveStatus[col] = vals[i]
				}
				metrics["is_replica"] = true
				if secBehind, ok := slaveStatus["Seconds_Behind_Master"]; ok {
					if seconds, ok := secBehind.(int64); ok {
						metrics["replication_lag_seconds"] = float64(seconds)
					}
				}
			}
		} else {
			metrics["is_replica"] = false
		}
	}

	return metrics, nil
}

// ListLogicalDatabases lists all logical databases in the MySQL instance
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
		return nil, adapter.WrapError(dbcapabilities.MySQL, "list_databases", err)
	}
	defer rows.Close()

	var databases []adapter.LogicalDatabaseInfo
	for rows.Next() {
		var db adapter.LogicalDatabaseInfo
		err := rows.Scan(&db.Name, &db.SizeBytes, &db.Encoding, &db.Collation)
		if err != nil {
			continue
		}
		db.Owner = "mysql" // MySQL doesn't have database owners like PostgreSQL
		databases = append(databases, db)
	}

	if err := rows.Err(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "list_databases", err)
	}

	return databases, nil
}
