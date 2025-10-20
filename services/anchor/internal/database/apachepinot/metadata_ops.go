package apachepinot

import (
	"context"
	"encoding/json"
	"fmt"
)

// MetadataOps implements metadata operations for Pinot.
type MetadataOps struct {
	conn         *Connection
	instanceConn *InstanceConnection
}

// CollectDatabaseMetadata collects metadata about Pinot tables.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "apachepinot"

	// List tables
	tables, err := m.conn.client.ListTables(ctx)
	if err == nil {
		metadata["table_count"] = len(tables)
		metadata["tables"] = tables
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the Pinot cluster.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	var client *PinotClient

	if m.conn != nil {
		client = m.conn.client
	} else if m.instanceConn != nil {
		client = m.instanceConn.client
	} else {
		return nil, fmt.Errorf("no connection available")
	}

	metadata := make(map[string]interface{})
	metadata["database_type"] = "apachepinot"

	// List tables
	tables, err := client.ListTables(ctx)
	if err == nil {
		metadata["table_count"] = len(tables)
		metadata["tables"] = tables
	}

	return metadata, nil
}

// GetVersion returns the Pinot version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	// Pinot version would typically be retrieved from /version endpoint
	return "Apache Pinot", nil
}

// GetUniqueIdentifier returns a unique identifier for the Pinot cluster.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	if m.conn != nil {
		return "apachepinot::cluster", nil
	}

	if m.instanceConn != nil {
		return "apachepinot::instance", nil
	}

	return "apachepinot::unknown", nil
}

// GetDatabaseSize returns the total size of tables.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	// Pinot size would require querying segment metadata
	return 0, fmt.Errorf("database size calculation requires segment metadata API")
}

// GetTableCount returns the number of tables.
func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	if m.conn == nil {
		return 0, fmt.Errorf("no connection available")
	}

	tables, err := m.conn.client.ListTables(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get table count: %w", err)
	}

	return len(tables), nil
}

// ExecuteCommand executes a SQL query and returns results.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	var client *PinotClient

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
