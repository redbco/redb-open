package metrics

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/logger"
)

// Manager coordinates metrics collection across all database types
type Manager struct {
	collectors map[dbcapabilities.DatabaseType]MetricsCollector
	repository *Repository
	logger     *logger.Logger
}

// NewManager creates a new metrics manager
func NewManager(repository *Repository, logger *logger.Logger) *Manager {
	return &Manager{
		collectors: make(map[dbcapabilities.DatabaseType]MetricsCollector),
		repository: repository,
		logger:     logger,
	}
}

// RegisterCollector registers a metrics collector for a specific database type
func (m *Manager) RegisterCollector(dbType dbcapabilities.DatabaseType, collector MetricsCollector) {
	m.collectors[dbType] = collector
}

// CollectAndStoreMetrics collects metrics from an instance connection and stores them
func (m *Manager) CollectAndStoreMetrics(ctx context.Context, instanceConn adapter.InstanceConnection) error {
	dbType := instanceConn.Type()
	collector, exists := m.collectors[dbType]

	if !exists {
		return fmt.Errorf("no metrics collector registered for database type: %s", dbType)
	}

	// Collect metrics
	metrics, err := collector.CollectMetrics()
	if err != nil {
		return fmt.Errorf("failed to collect metrics for %s: %w", dbType, err)
	}

	// Set instance ID from connection
	metrics.InstanceID = instanceConn.ID()

	// Store in database
	if err := m.repository.StoreInstanceMetrics(ctx, metrics); err != nil {
		return fmt.Errorf("failed to store metrics for instance %s: %w", metrics.InstanceID, err)
	}

	m.logger.Debug("Successfully collected and stored metrics for instance %s", metrics.InstanceID)
	return nil
}

// GetCollector returns the metrics collector for a specific database type
func (m *Manager) GetCollector(dbType dbcapabilities.DatabaseType) (MetricsCollector, error) {
	collector, exists := m.collectors[dbType]
	if !exists {
		return nil, fmt.Errorf("no metrics collector registered for database type: %s", dbType)
	}
	return collector, nil
}

// Helper functions for pointer conversions

// Int32Ptr returns a pointer to an int32 value
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr returns a pointer to an int64 value
func Int64Ptr(v int64) *int64 {
	return &v
}

// Float64Ptr returns a pointer to a float64 value
func Float64Ptr(v float64) *float64 {
	return &v
}

// BoolPtr returns a pointer to a bool value
func BoolPtr(v bool) *bool {
	return &v
}
