package druid

import (
	"context"
	"encoding/json"
	"fmt"
)

// MetadataOps implements metadata operations for Druid.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about a Druid datasource.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "druid"

	// List datasources
	datasources, err := m.conn.client.ListDatasources(ctx)
	if err == nil {
		metadata["datasource_count"] = len(datasources)
		metadata["datasources"] = datasources
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the Druid cluster.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *DruidClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "druid"

	// List datasources
	datasources, err := client.ListDatasources(ctx)
	if err == nil {
		metadata["datasource_count"] = len(datasources)
		metadata["datasources"] = datasources
	}

	return metadata, nil
}

// GetVersion returns the Druid version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	// Druid version would typically be retrieved from /status endpoint
	return "Apache Druid", nil
}

// GetUniqueIdentifier returns a unique identifier for the Druid cluster.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		return "druid::cluster", nil
	}

	if m.instanceConn != nil {
		return "druid::instance", nil
	}

	return "druid::unknown", nil
}

// GetDatabaseSize returns the total size of datasources.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	// Druid size would require querying segment metadata
	return 0, fmt.Errorf("database size calculation requires segment metadata API")
}

// GetTableCount returns the number of datasources.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	datasources, err := m.conn.client.ListDatasources(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get datasource count: %w", err)
	}

	return len(datasources), nil
}

// ExecuteCommand executes a SQL query and returns results.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	var client *DruidClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	result, err := client.QuerySQL(ctx, command)
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
