package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// PostgresCollector collects metrics from PostgreSQL instances
type PostgresCollector struct {
	conn adapter.InstanceConnection
}

// NewPostgresCollector creates a new PostgreSQL metrics collector
func NewPostgresCollector(conn adapter.InstanceConnection) *PostgresCollector {
	return &PostgresCollector{
		conn: conn,
	}
}

// CollectMetrics collects current performance metrics from PostgreSQL
func (c *PostgresCollector) CollectMetrics() (*InstanceMetrics, error) {
	pool, ok := c.conn.Raw().(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid connection type for PostgreSQL metrics collector")
	}

	ctx := context.Background()
	metrics := &InstanceMetrics{
		CollectedAt:     time.Now(),
		ExtendedMetrics: make(map[string]interface{}),
	}

	// Collect connection metrics
	if err := c.collectConnectionMetrics(ctx, pool, metrics); err != nil {
		return nil, fmt.Errorf("failed to collect connection metrics: %w", err)
	}

	// Collect performance metrics
	if err := c.collectPerformanceMetrics(ctx, pool, metrics); err != nil {
		return nil, fmt.Errorf("failed to collect performance metrics: %w", err)
	}

	// Collect cache metrics
	if err := c.collectCacheMetrics(ctx, pool, metrics); err != nil {
		return nil, fmt.Errorf("failed to collect cache metrics: %w", err)
	}

	// Collect replication metrics (if applicable)
	if err := c.collectReplicationMetrics(ctx, pool, metrics); err != nil {
		// Replication metrics are optional, just log and continue
		metrics.ExtendedMetrics["replication_error"] = err.Error()
	}

	// Collect transaction metrics
	if err := c.collectTransactionMetrics(ctx, pool, metrics); err != nil {
		return nil, fmt.Errorf("failed to collect transaction metrics: %w", err)
	}

	return metrics, nil
}

// collectConnectionMetrics collects connection-related metrics
func (c *PostgresCollector) collectConnectionMetrics(ctx context.Context, pool *pgxpool.Pool, metrics *InstanceMetrics) error {
	query := `
		SELECT 
			COUNT(*) FILTER (WHERE state = 'active') as active,
			COUNT(*) FILTER (WHERE state = 'idle') as idle,
			COUNT(*) as total
		FROM pg_stat_activity
		WHERE datname IS NOT NULL
	`

	var active, idle, total int32
	err := pool.QueryRow(ctx, query).Scan(&active, &idle, &total)
	if err != nil {
		return fmt.Errorf("failed to query connection stats: %w", err)
	}

	metrics.ActiveConnections = &active
	metrics.IdleConnections = &idle

	// Get max connections
	var maxConn int32
	err = pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConn)
	if err != nil {
		return fmt.Errorf("failed to get max connections: %w", err)
	}

	// Calculate connection utilization
	if maxConn > 0 {
		utilization := float64(total) / float64(maxConn) * 100.0
		metrics.ConnectionUtilization = &utilization
	}

	metrics.ExtendedMetrics["max_connections"] = maxConn
	metrics.ExtendedMetrics["total_connections"] = total

	return nil
}

// collectPerformanceMetrics collects performance-related metrics
func (c *PostgresCollector) collectPerformanceMetrics(ctx context.Context, pool *pgxpool.Pool, metrics *InstanceMetrics) error {
	// Get database statistics
	query := `
		SELECT 
			COALESCE(SUM(xact_commit), 0) as commits,
			COALESCE(SUM(xact_rollback), 0) as rollbacks,
			COALESCE(SUM(blks_read), 0) as blocks_read,
			COALESCE(SUM(blks_hit), 0) as blocks_hit,
			COALESCE(SUM(tup_returned), 0) as tuples_returned,
			COALESCE(SUM(tup_fetched), 0) as tuples_fetched,
			COALESCE(SUM(tup_inserted), 0) as tuples_inserted,
			COALESCE(SUM(tup_updated), 0) as tuples_updated,
			COALESCE(SUM(tup_deleted), 0) as tuples_deleted
		FROM pg_stat_database
		WHERE datname NOT IN ('template0', 'template1')
	`

	var commits, rollbacks, blocksRead, blocksHit int64
	var tuplesReturned, tuplesFetched, tuplesInserted, tuplesUpdated, tuplesDeleted int64

	err := pool.QueryRow(ctx, query).Scan(
		&commits, &rollbacks, &blocksRead, &blocksHit,
		&tuplesReturned, &tuplesFetched, &tuplesInserted, &tuplesUpdated, &tuplesDeleted,
	)
	if err != nil {
		return fmt.Errorf("failed to query database stats: %w", err)
	}

	// Store in extended metrics
	metrics.ExtendedMetrics["commits"] = commits
	metrics.ExtendedMetrics["rollbacks"] = rollbacks
	metrics.ExtendedMetrics["blocks_read"] = blocksRead
	metrics.ExtendedMetrics["blocks_hit"] = blocksHit
	metrics.ExtendedMetrics["tuples_returned"] = tuplesReturned
	metrics.ExtendedMetrics["tuples_fetched"] = tuplesFetched
	metrics.ExtendedMetrics["tuples_inserted"] = tuplesInserted
	metrics.ExtendedMetrics["tuples_updated"] = tuplesUpdated
	metrics.ExtendedMetrics["tuples_deleted"] = tuplesDeleted

	// Get slow query count if pg_stat_statements is available
	slowQueryCount, err := c.getSlowQueryCount(ctx, pool)
	if err == nil {
		metrics.SlowQueryCount = Int32Ptr(int32(slowQueryCount))
	}

	return nil
}

// collectCacheMetrics collects cache-related metrics
func (c *PostgresCollector) collectCacheMetrics(ctx context.Context, pool *pgxpool.Pool, metrics *InstanceMetrics) error {
	// Calculate buffer cache hit ratio
	query := `
		SELECT 
			CASE 
				WHEN (SUM(heap_blks_hit) + SUM(heap_blks_read)) > 0 
				THEN ROUND(100.0 * SUM(heap_blks_hit) / NULLIF(SUM(heap_blks_hit) + SUM(heap_blks_read), 0), 2)
				ELSE 0 
			END as cache_hit_ratio
		FROM pg_statio_user_tables
	`

	var cacheHitRatio float64
	err := pool.QueryRow(ctx, query).Scan(&cacheHitRatio)
	if err != nil {
		return fmt.Errorf("failed to query cache hit ratio: %w", err)
	}

	metrics.CacheHitRatio = &cacheHitRatio

	return nil
}

// collectReplicationMetrics collects replication-related metrics
func (c *PostgresCollector) collectReplicationMetrics(ctx context.Context, pool *pgxpool.Pool, metrics *InstanceMetrics) error {
	// Check if this is a replica
	var isReplica bool
	err := pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&isReplica)
	if err != nil {
		return fmt.Errorf("failed to check replica status: %w", err)
	}

	metrics.IsReplica = &isReplica

	if isReplica {
		// Get replication lag for replica
		query := `
			SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float8 as lag_seconds
		`
		var lagSeconds float64
		err = pool.QueryRow(ctx, query).Scan(&lagSeconds)
		if err == nil {
			metrics.ReplicationLagSeconds = &lagSeconds
		}
	} else {
		// Check if this is a primary with replicas
		var replicaCount int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM pg_stat_replication").Scan(&replicaCount)
		if err == nil {
			metrics.ExtendedMetrics["replica_count"] = replicaCount
		}
	}

	return nil
}

// collectTransactionMetrics collects transaction-related metrics
func (c *PostgresCollector) collectTransactionMetrics(ctx context.Context, pool *pgxpool.Pool, metrics *InstanceMetrics) error {
	// Get transaction statistics over the last stats reset period
	query := `
		SELECT 
			COALESCE(SUM(xact_commit + xact_rollback), 0) as total_transactions,
			EXTRACT(EPOCH FROM (now() - stats_reset))::float8 as stats_age_seconds
		FROM pg_stat_database
		WHERE datname NOT IN ('template0', 'template1')
		GROUP BY stats_reset
	`

	var totalTransactions int64
	var statsAgeSeconds float64
	err := pool.QueryRow(ctx, query).Scan(&totalTransactions, &statsAgeSeconds)
	if err == nil && statsAgeSeconds > 0 {
		tps := float64(totalTransactions) / statsAgeSeconds
		metrics.TransactionsPerSecond = &tps
		metrics.ExtendedMetrics["total_transactions"] = totalTransactions
		metrics.ExtendedMetrics["stats_age_seconds"] = statsAgeSeconds
	}

	return nil
}

// getSlowQueryCount gets the count of slow queries from pg_stat_statements if available
func (c *PostgresCollector) getSlowQueryCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	// Check if pg_stat_statements is available
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
		)
	`).Scan(&exists)
	if err != nil || !exists {
		return 0, fmt.Errorf("pg_stat_statements not available")
	}

	// Count queries with mean_exec_time > 1000ms (1 second)
	var count int64
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) 
		FROM pg_stat_statements 
		WHERE mean_exec_time > 1000
	`).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// CollectEnhancedMetadata collects extended instance metadata from PostgreSQL
func (c *PostgresCollector) CollectEnhancedMetadata() (*EnhancedInstanceMetadata, error) {
	pool, ok := c.conn.Raw().(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid connection type for PostgreSQL metadata collector")
	}

	ctx := context.Background()
	metadata := &EnhancedInstanceMetadata{
		ServerSettings:  make(map[string]string),
		FeaturesEnabled: []string{},
	}

	// Get version
	var version string
	err := pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}
	metadata.Version = version

	// Detect edition (PostgreSQL, EnterpriseDB, Citus, etc.)
	metadata.Edition = c.detectEdition(version)

	// Get platform and architecture
	var sysInfo string
	err = pool.QueryRow(ctx, "SELECT version()").Scan(&sysInfo)
	if err == nil {
		metadata.Platform, metadata.Architecture = c.parsePlatformInfo(sysInfo)
	}

	// Get important server settings
	c.collectServerSettings(ctx, pool, metadata)

	// Check for installed extensions
	c.collectExtensions(ctx, pool, metadata)

	// Get cluster info
	c.collectClusterInfo(ctx, pool, metadata)

	// Check SSL status
	c.collectSSLInfo(ctx, pool, metadata)

	// Get logical databases list
	databases, err := c.listLogicalDatabases(ctx, pool)
	if err == nil {
		metadata.LogicalDatabases = databases
		metadata.TotalDatabases = int32(len(databases))
	}

	// Get basic stats
	var uptime int64
	err = pool.QueryRow(ctx, "SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint").Scan(&uptime)
	if err == nil {
		metadata.UptimeSeconds = uptime
	}

	var maxConn, totalConn int32
	pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConn)
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM pg_stat_activity").Scan(&totalConn)
	metadata.MaxConnections = maxConn
	metadata.TotalConnections = totalConn

	return metadata, nil
}

// detectEdition detects the PostgreSQL edition from version string
func (c *PostgresCollector) detectEdition(version string) string {
	// Check for common editions
	if contains(version, "EnterpriseDB") {
		return "EnterpriseDB"
	} else if contains(version, "Citus") {
		return "Citus"
	} else if contains(version, "TimescaleDB") {
		return "TimescaleDB"
	} else if contains(version, "Amazon Aurora") {
		return "Amazon Aurora PostgreSQL"
	} else if contains(version, "Google Cloud SQL") {
		return "Google Cloud SQL"
	}
	return "Community"
}

// parsePlatformInfo extracts platform and architecture from version string
func (c *PostgresCollector) parsePlatformInfo(version string) (platform, architecture string) {
	// Parse from version string like "PostgreSQL 16.3 on aarch64-unknown-linux-gnu"
	if contains(version, "linux") {
		platform = "linux"
	} else if contains(version, "darwin") {
		platform = "darwin"
	} else if contains(version, "win") {
		platform = "windows"
	} else {
		platform = "unknown"
	}

	if contains(version, "aarch64") || contains(version, "arm64") {
		architecture = "arm64"
	} else if contains(version, "x86_64") || contains(version, "amd64") {
		architecture = "x86_64"
	} else {
		architecture = "unknown"
	}

	return platform, architecture
}

// collectServerSettings collects important server configuration settings
func (c *PostgresCollector) collectServerSettings(ctx context.Context, pool *pgxpool.Pool, metadata *EnhancedInstanceMetadata) {
	settings := []string{
		"shared_buffers",
		"work_mem",
		"maintenance_work_mem",
		"effective_cache_size",
		"wal_buffers",
		"max_wal_size",
		"checkpoint_timeout",
		"random_page_cost",
	}

	for _, setting := range settings {
		var value string
		err := pool.QueryRow(ctx, fmt.Sprintf("SHOW %s", setting)).Scan(&value)
		if err == nil {
			metadata.ServerSettings[setting] = value
		}
	}
}

// collectExtensions collects installed PostgreSQL extensions
func (c *PostgresCollector) collectExtensions(ctx context.Context, pool *pgxpool.Pool, metadata *EnhancedInstanceMetadata) {
	query := `SELECT extname FROM pg_extension WHERE extname NOT IN ('plpgsql')`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()

	extensions := []string{}
	for rows.Next() {
		var extName string
		if err := rows.Scan(&extName); err == nil {
			extensions = append(extensions, extName)
		}
	}

	if len(extensions) > 0 {
		metadata.FeaturesEnabled = extensions
	}
}

// collectClusterInfo collects cluster/replication information
func (c *PostgresCollector) collectClusterInfo(ctx context.Context, pool *pgxpool.Pool, metadata *EnhancedInstanceMetadata) {
	var isReplica bool
	err := pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&isReplica)
	if err != nil {
		return
	}

	clusterInfo := &ClusterInfo{
		IsClustered: false,
		NodeRole:    "standalone",
	}

	if isReplica {
		clusterInfo.NodeRole = "replica"
		clusterInfo.IsClustered = true
	} else {
		// Check if this is a primary with replicas
		var replicaCount int
		err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM pg_stat_replication").Scan(&replicaCount)
		if err == nil && replicaCount > 0 {
			clusterInfo.NodeRole = "primary"
			clusterInfo.IsClustered = true
		}
	}

	metadata.ClusterInfo = clusterInfo
}

// collectSSLInfo collects SSL configuration information
func (c *PostgresCollector) collectSSLInfo(ctx context.Context, pool *pgxpool.Pool, metadata *EnhancedInstanceMetadata) {
	var sslEnabled string
	err := pool.QueryRow(ctx, "SHOW ssl").Scan(&sslEnabled)
	if err == nil {
		metadata.SSLEnabled = (sslEnabled == "on")
	}
}

// listLogicalDatabases lists all logical databases in the instance
func (c *PostgresCollector) listLogicalDatabases(ctx context.Context, pool *pgxpool.Pool) ([]LogicalDatabaseInfo, error) {
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

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databases []LogicalDatabaseInfo
	for rows.Next() {
		var db LogicalDatabaseInfo
		var collation string
		err := rows.Scan(&db.Name, &db.SizeBytes, &db.Owner, &db.Encoding, &collation)
		if err != nil {
			continue
		}
		db.Collation = collation
		databases = append(databases, db)
	}

	return databases, nil
}

// GetDatabaseType returns the database type
func (c *PostgresCollector) GetDatabaseType() string {
	return string(dbcapabilities.PostgreSQL)
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
