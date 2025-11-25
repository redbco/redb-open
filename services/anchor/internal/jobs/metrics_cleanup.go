package jobs

import (
	"context"
	"time"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/metrics"
)

// MetricsCleanupJob handles automatic cleanup of old metrics data
type MetricsCleanupJob struct {
	repository    *metrics.Repository
	logger        *logger.Logger
	retentionDays int
	cleanupTicker *time.Ticker
	stopChan      chan struct{}
}

// NewMetricsCleanupJob creates a new metrics cleanup job
func NewMetricsCleanupJob(repository *metrics.Repository, logger *logger.Logger, retentionDays int) *MetricsCleanupJob {
	if retentionDays <= 0 {
		retentionDays = 7 // Default to 7 days
	}

	return &MetricsCleanupJob{
		repository:    repository,
		logger:        logger,
		retentionDays: retentionDays,
		stopChan:      make(chan struct{}),
	}
}

// Start begins the cleanup job, running daily at midnight
func (j *MetricsCleanupJob) Start(ctx context.Context) {
	// Calculate time until next midnight
	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	durationUntilMidnight := time.Until(nextMidnight)

	j.logger.Info("Metrics cleanup job starting. Next cleanup in %v", durationUntilMidnight)

	// Wait until midnight for first cleanup
	select {
	case <-time.After(durationUntilMidnight):
		j.performCleanup(ctx)
	case <-ctx.Done():
		j.logger.Info("Metrics cleanup job stopped before first cleanup")
		return
	case <-j.stopChan:
		j.logger.Info("Metrics cleanup job stopped before first cleanup")
		return
	}

	// Run cleanup daily at midnight
	j.cleanupTicker = time.NewTicker(24 * time.Hour)
	defer j.cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			j.logger.Info("Metrics cleanup job stopped (context cancelled)")
			return
		case <-j.stopChan:
			j.logger.Info("Metrics cleanup job stopped")
			return
		case <-j.cleanupTicker.C:
			j.performCleanup(ctx)
		}
	}
}

// Stop stops the cleanup job
func (j *MetricsCleanupJob) Stop() {
	close(j.stopChan)
}

// performCleanup executes the cleanup operation
func (j *MetricsCleanupJob) performCleanup(ctx context.Context) {
	j.logger.Info("Starting metrics cleanup (retention: %d days)", j.retentionDays)

	retentionDuration := time.Duration(j.retentionDays) * 24 * time.Hour

	// Delete old metrics
	deletedCount, err := j.repository.DeleteOldMetrics(ctx, retentionDuration)
	if err != nil {
		j.logger.Error("Failed to delete old metrics: %v", err)
		return
	}

	j.logger.Info("Deleted %d old metric records", deletedCount)

	// Analyze the metrics table to update statistics
	if err := j.repository.AnalyzeMetricsTable(ctx); err != nil {
		j.logger.Error("Failed to analyze metrics table: %v", err)
		return
	}

	j.logger.Info("Metrics cleanup completed successfully")
}

// RunOnce runs the cleanup operation immediately (useful for testing or manual triggers)
func (j *MetricsCleanupJob) RunOnce(ctx context.Context) error {
	j.logger.Info("Running metrics cleanup (manual trigger)")
	j.performCleanup(ctx)
	return nil
}
