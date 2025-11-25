package postgres

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for PostgreSQL database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata about the database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectDatabaseMetadata function
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "collect_database_metadata", err)
	}
	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the database instance.
// Note: This is called on a database connection, not an instance connection.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectInstanceMetadata function
	metadata, err := CollectInstanceMetadata(ctx, m.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the PostgreSQL version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.PostgreSQL, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns the unique identifier for the PostgreSQL instance.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var identifier string
	err := m.conn.pool.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&identifier)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.PostgreSQL, "get_unique_identifier", err)
	}
	return identifier, nil
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var size int64
	err := m.conn.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&size)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.PostgreSQL, "get_database_size", err)
	}
	return size, nil
}

// GetTableCount returns the number of tables in the database.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	query := `
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
	`
	err := m.conn.pool.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.PostgreSQL, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// Use existing ExecuteCommand function
	result, err := ExecuteCommand(ctx, m.conn.pool, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "execute_command", err)
	}
	return result, nil
}

// CollectInstanceMetrics collects performance metrics (not available on database connection)
func (m *MetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.PostgreSQL,
		"collect instance metrics",
		"not available on database connections - use instance connection",
	)
}

// ListLogicalDatabases lists logical databases (not available on database connection)
func (m *MetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.PostgreSQL,
		"list logical databases",
		"not available on database connections - use instance connection",
	)
}

// InstanceMetadataOps implements adapter.MetadataOperator for PostgreSQL instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectDatabaseMetadata is not applicable for instance connections.
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.PostgreSQL,
		"collect database metadata",
		"not available on instance connections",
	)
}

// CollectInstanceMetadata collects metadata about the PostgreSQL instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectInstanceMetadata function
	metadata, err := CollectInstanceMetadata(ctx, i.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the PostgreSQL version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.PostgreSQL, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns the unique identifier for the PostgreSQL instance.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var identifier string
	err := i.conn.pool.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&identifier)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.PostgreSQL, "get_unique_identifier", err)
	}
	return identifier, nil
}

// GetDatabaseSize is not applicable for instance connections.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.PostgreSQL,
		"get database size",
		"not available on instance connections",
	)
}

// GetTableCount is not applicable for instance connections.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.PostgreSQL,
		"get table count",
		"not available on instance connections",
	)
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// Use existing ExecuteCommand function
	result, err := ExecuteCommand(ctx, i.conn.pool, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "execute_command", err)
	}
	return result, nil
}

// CollectInstanceMetrics collects performance metrics from the PostgreSQL instance
func (i *InstanceMetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// Connection metrics
	var activeConn, idleConn, totalConn int32
	query := `
		SELECT 
			COUNT(*) FILTER (WHERE state = 'active') as active,
			COUNT(*) FILTER (WHERE state = 'idle') as idle,
			COUNT(*) as total
		FROM pg_stat_activity
		WHERE datname IS NOT NULL
	`
	err := i.conn.pool.QueryRow(ctx, query).Scan(&activeConn, &idleConn, &totalConn)
	if err == nil {
		metrics["active_connections"] = activeConn
		metrics["idle_connections"] = idleConn

		// Get max connections for utilization
		var maxConn int32
		if err := i.conn.pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConn); err == nil && maxConn > 0 {
			metrics["connection_utilization"] = float64(totalConn) / float64(maxConn) * 100.0
		}
	}

	// Cache hit ratio
	cacheQuery := `
		SELECT 
			CASE 
				WHEN (SUM(heap_blks_hit) + SUM(heap_blks_read)) > 0 
				THEN ROUND(100.0 * SUM(heap_blks_hit) / NULLIF(SUM(heap_blks_hit) + SUM(heap_blks_read), 0), 2)
				ELSE 0 
			END as cache_hit_ratio
		FROM pg_statio_user_tables
	`
	var cacheHitRatio float64
	if err := i.conn.pool.QueryRow(ctx, cacheQuery).Scan(&cacheHitRatio); err == nil {
		metrics["cache_hit_ratio"] = cacheHitRatio
	}

	// Transaction metrics
	txQuery := `
		SELECT 
			COALESCE(SUM(xact_commit + xact_rollback), 0) as total_transactions,
			EXTRACT(EPOCH FROM (now() - MIN(stats_reset)))::float8 as stats_age_seconds
		FROM pg_stat_database
		WHERE datname NOT IN ('template0', 'template1')
	`
	var totalTx int64
	var statsAge float64
	if err := i.conn.pool.QueryRow(ctx, txQuery).Scan(&totalTx, &statsAge); err == nil && statsAge > 0 {
		metrics["transactions_per_second"] = float64(totalTx) / statsAge
	}

	// Replication metrics
	var isReplica bool
	if err := i.conn.pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&isReplica); err == nil {
		metrics["is_replica"] = isReplica
		if isReplica {
			var lagSeconds float64
			lagQuery := `SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float8`
			if err := i.conn.pool.QueryRow(ctx, lagQuery).Scan(&lagSeconds); err == nil {
				metrics["replication_lag_seconds"] = lagSeconds
			}
		}
	}

	return metrics, nil
}

// ListLogicalDatabases lists all logical databases in the PostgreSQL instance
func (i *InstanceMetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	query := `
		SELECT 
			datname,
			pg_database_size(datname) as size_bytes,
			pg_catalog.pg_get_userbyid(datdba) as owner,
			pg_encoding_to_char(encoding) as encoding,
			datcollate as collation
		FROM pg_database
		WHERE datistemplate = false
		ORDER BY datname
	`

	rows, err := i.conn.pool.Query(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_databases", err)
	}
	defer rows.Close()

	var databases []adapter.LogicalDatabaseInfo
	for rows.Next() {
		var db adapter.LogicalDatabaseInfo
		var collation string
		err := rows.Scan(&db.Name, &db.SizeBytes, &db.Owner, &db.Encoding, &collation)
		if err != nil {
			continue
		}
		db.Collation = collation
		databases = append(databases, db)
	}

	if err := rows.Err(); err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_databases", err)
	}

	return databases, nil
}
