package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// MetadataOps implements metadata operations for Prometheus.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about Prometheus.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "prometheus"

	// Get metric count
	metrics, err := m.conn.client.GetSeriesNames(ctx)
	if err == nil {
		metadata["metric_count"] = len(metrics)
		metadata["metrics"] = metrics
	}

	// Get label count
	labels, err := m.conn.client.GetLabels(ctx)
	if err == nil {
		metadata["label_count"] = len(labels)
		metadata["labels"] = labels
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the Prometheus instance.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *PrometheusClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "prometheus"

	// Get metric count
	metrics, err := client.GetSeriesNames(ctx)
	if err == nil {
		metadata["metric_count"] = len(metrics)
	}

	// Get label count
	labels, err := client.GetLabels(ctx)
	if err == nil {
		metadata["label_count"] = len(labels)
	}

	return metadata, nil
}

// GetVersion returns the Prometheus version (via build info).
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	// Prometheus exposes version info via the prometheus_build_info metric
	var client *PrometheusClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return "", fmt.Errorf("no connection available")
	}

	// Query build info
	result, err := client.Query(ctx, "prometheus_build_info", time.Time{})
	if err != nil {
		return "Prometheus (version unknown)", nil
	}

	// Try to extract version from metric labels
	if len(result.Data.Result) > 0 {
		if item, ok := result.Data.Result[0].(map[string]interface{}); ok {
			if metric, ok := item["metric"].(map[string]interface{}); ok {
				if version, ok := metric["version"].(string); ok {
					return fmt.Sprintf("Prometheus %s", version), nil
				}
			}
		}
	}

	return "Prometheus", nil
}

// GetUniqueIdentifier returns a unique identifier for Prometheus.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		return "prometheus::metrics", nil
	}

	if m.instanceConn != nil {
		return "prometheus::instance", nil
	}

	return "prometheus::unknown", nil
}

// GetDatabaseSize returns the storage size (not directly available).
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	// Prometheus doesn't expose storage size directly via API
	// Would need to check TSDB stats or disk usage
	return 0, fmt.Errorf("database size not available via Prometheus API")
}

// GetTableCount returns the number of metrics (tables).
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	metrics, err := m.conn.client.GetSeriesNames(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get metric count: %w", err)
	}

	return len(metrics), nil
}

// ExecuteCommand executes a PromQL query and returns results.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	var client *PrometheusClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	result, err := client.Query(ctx, command, time.Time{})
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	// Convert result to JSON bytes
	data, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	return data, nil
}

// GetTargetInfo returns information about scrape targets.
func (m *MetadataOps) GetTargetInfo(ctx context.Context) (map[string]interface{}, error) {
	// This would require accessing the Prometheus targets API
	// For now, return placeholder
	return map[string]interface{}{
		"message": "Target information requires Prometheus targets API",
	}, nil
}
