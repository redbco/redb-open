package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/syslog"
)

// Repository handles database operations for metrics
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new metrics repository
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		pool: pool,
	}
}

// StoreInstanceMetrics stores instance metrics in the database
func (r *Repository) StoreInstanceMetrics(ctx context.Context, metrics *InstanceMetrics) error {
	if metrics == nil {
		return fmt.Errorf("metrics cannot be nil")
	}

	// Marshal extended metrics to JSON
	extendedMetricsJSON, err := json.Marshal(metrics.ExtendedMetrics)
	if err != nil {
		return fmt.Errorf("failed to marshal extended metrics: %w", err)
	}

	query := `
		INSERT INTO instance_metrics (
			instance_id,
			collected_at,
			active_connections,
			idle_connections,
			connection_utilization,
			queries_per_second,
			transactions_per_second,
			cache_hit_ratio,
			cpu_usage,
			memory_usage_bytes,
			memory_total_bytes,
			disk_usage_bytes,
			disk_total_bytes,
			avg_query_time_ms,
			slow_query_count,
			replication_lag_seconds,
			is_replica,
			extended_metrics
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18
		)
	`

	_, err = r.pool.Exec(ctx, query,
		metrics.InstanceID,
		metrics.CollectedAt,
		metrics.ActiveConnections,
		metrics.IdleConnections,
		metrics.ConnectionUtilization,
		metrics.QueriesPerSecond,
		metrics.TransactionsPerSecond,
		metrics.CacheHitRatio,
		metrics.CPUUsage,
		metrics.MemoryUsageBytes,
		metrics.MemoryTotalBytes,
		metrics.DiskUsageBytes,
		metrics.DiskTotalBytes,
		metrics.AvgQueryTimeMs,
		metrics.SlowQueryCount,
		metrics.ReplicationLagSeconds,
		metrics.IsReplica,
		extendedMetricsJSON,
	)

	if err != nil {
		return fmt.Errorf("failed to insert instance metrics: %w", err)
	}

	syslog.Debug("metrics", "Stored metrics for instance %s at %v", metrics.InstanceID, metrics.CollectedAt)
	return nil
}

// GetInstanceMetrics retrieves metrics for an instance within a time range
func (r *Repository) GetInstanceMetrics(ctx context.Context, instanceID string, startTime, endTime *time.Time) ([]*InstanceMetrics, error) {
	query := `
		SELECT 
			instance_id,
			collected_at,
			active_connections,
			idle_connections,
			connection_utilization,
			queries_per_second,
			transactions_per_second,
			cache_hit_ratio,
			cpu_usage,
			memory_usage_bytes,
			memory_total_bytes,
			disk_usage_bytes,
			disk_total_bytes,
			avg_query_time_ms,
			slow_query_count,
			replication_lag_seconds,
			is_replica,
			extended_metrics
		FROM instance_metrics
		WHERE instance_id = $1
			AND collected_at >= $2
			AND collected_at <= $3
		ORDER BY collected_at DESC
	`

	// Default to last 24 hours if not specified
	if startTime == nil {
		defaultStart := time.Now().Add(-24 * time.Hour)
		startTime = &defaultStart
	}
	if endTime == nil {
		now := time.Now()
		endTime = &now
	}

	rows, err := r.pool.Query(ctx, query, instanceID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query instance metrics: %w", err)
	}
	defer rows.Close()

	var metrics []*InstanceMetrics
	for rows.Next() {
		var m InstanceMetrics
		var extendedMetricsJSON []byte

		err := rows.Scan(
			&m.InstanceID,
			&m.CollectedAt,
			&m.ActiveConnections,
			&m.IdleConnections,
			&m.ConnectionUtilization,
			&m.QueriesPerSecond,
			&m.TransactionsPerSecond,
			&m.CacheHitRatio,
			&m.CPUUsage,
			&m.MemoryUsageBytes,
			&m.MemoryTotalBytes,
			&m.DiskUsageBytes,
			&m.DiskTotalBytes,
			&m.AvgQueryTimeMs,
			&m.SlowQueryCount,
			&m.ReplicationLagSeconds,
			&m.IsReplica,
			&extendedMetricsJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan metric row: %w", err)
		}

		// Unmarshal extended metrics
		if len(extendedMetricsJSON) > 0 {
			if err := json.Unmarshal(extendedMetricsJSON, &m.ExtendedMetrics); err != nil {
				syslog.Warn("metrics", "Failed to unmarshal extended metrics for instance %s: %v", instanceID, err)
				m.ExtendedMetrics = make(map[string]interface{})
			}
		} else {
			m.ExtendedMetrics = make(map[string]interface{})
		}

		metrics = append(metrics, &m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating metrics rows: %w", err)
	}

	return metrics, nil
}

// GetLatestInstanceMetrics retrieves the most recent metrics for an instance
func (r *Repository) GetLatestInstanceMetrics(ctx context.Context, instanceID string) (*InstanceMetrics, error) {
	query := `
		SELECT 
			instance_id,
			collected_at,
			active_connections,
			idle_connections,
			connection_utilization,
			queries_per_second,
			transactions_per_second,
			cache_hit_ratio,
			cpu_usage,
			memory_usage_bytes,
			memory_total_bytes,
			disk_usage_bytes,
			disk_total_bytes,
			avg_query_time_ms,
			slow_query_count,
			replication_lag_seconds,
			is_replica,
			extended_metrics
		FROM instance_metrics
		WHERE instance_id = $1
		ORDER BY collected_at DESC
		LIMIT 1
	`

	var m InstanceMetrics
	var extendedMetricsJSON []byte

	err := r.pool.QueryRow(ctx, query, instanceID).Scan(
		&m.InstanceID,
		&m.CollectedAt,
		&m.ActiveConnections,
		&m.IdleConnections,
		&m.ConnectionUtilization,
		&m.QueriesPerSecond,
		&m.TransactionsPerSecond,
		&m.CacheHitRatio,
		&m.CPUUsage,
		&m.MemoryUsageBytes,
		&m.MemoryTotalBytes,
		&m.DiskUsageBytes,
		&m.DiskTotalBytes,
		&m.AvgQueryTimeMs,
		&m.SlowQueryCount,
		&m.ReplicationLagSeconds,
		&m.IsReplica,
		&extendedMetricsJSON,
	)

	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil // No metrics found, not an error
		}
		return nil, fmt.Errorf("failed to query latest instance metrics: %w", err)
	}

	// Unmarshal extended metrics
	if len(extendedMetricsJSON) > 0 {
		if err := json.Unmarshal(extendedMetricsJSON, &m.ExtendedMetrics); err != nil {
			syslog.Warn("metrics", "Failed to unmarshal extended metrics for instance %s: %v", instanceID, err)
			m.ExtendedMetrics = make(map[string]interface{})
		}
	} else {
		m.ExtendedMetrics = make(map[string]interface{})
	}

	return &m, nil
}

// DeleteOldMetrics deletes metrics older than the specified duration
func (r *Repository) DeleteOldMetrics(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoffTime := time.Now().Add(-olderThan)

	query := `
		DELETE FROM instance_metrics
		WHERE collected_at < $1
	`

	result, err := r.pool.Exec(ctx, query, cutoffTime)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old metrics: %w", err)
	}

	rowsAffected := result.RowsAffected()
	syslog.Info("metrics", "Deleted %d old metric records (older than %v)", rowsAffected, cutoffTime)

	return rowsAffected, nil
}

// AnalyzeMetricsTable runs ANALYZE on the instance_metrics table to update statistics
func (r *Repository) AnalyzeMetricsTable(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, "ANALYZE instance_metrics")
	if err != nil {
		return fmt.Errorf("failed to analyze metrics table: %w", err)
	}

	syslog.Debug("metrics", "Analyzed instance_metrics table")
	return nil
}
