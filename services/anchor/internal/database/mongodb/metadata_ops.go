package mongodb

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for MongoDB database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata about the database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "collect_database_metadata", err)
	}
	return metadata, nil
}

// CollectInstanceMetadata is not applicable for database connections.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
		"metadata",
		"instance metadata collection not supported on database connection",
	)
}

// GetVersion returns the MongoDB version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	metadata, err := m.CollectDatabaseMetadata(ctx)
	if err != nil {
		return "", err
	}

	if version, ok := metadata["version"].(string); ok {
		return version, nil
	}

	return "", adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
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

// GetTableCount returns the number of collections in the database.
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
	if count, ok := metadata["collections_count"].(int); ok {
		return count, nil
	}
	if count, ok := metadata["collections_count"].(float64); ok {
		return int(count), nil
	}

	return 0, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, m.conn.db, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "execute_command", err)
	}
	return result, nil
}

// CollectInstanceMetrics collects performance metrics (not available on database connection)
func (m *MetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
		"metadata",
		"instance metrics collection not supported on database connection",
	)
}

// ListLogicalDatabases lists logical databases (not available on database connection)
func (m *MetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
		"metadata",
		"list databases not supported on database connection",
	)
}

// InstanceMetadataOps implements adapter.MetadataOperator for MongoDB instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectDatabaseMetadata is not applicable for instance connections.
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
		"metadata",
		"database metadata collection not supported on instance connection",
	)
}

// CollectInstanceMetadata collects metadata about the database instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the MongoDB version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	metadata, err := i.CollectInstanceMetadata(ctx)
	if err != nil {
		return "", err
	}

	if version, ok := metadata["version"].(string); ok {
		return version, nil
	}

	return "", adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
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
		dbcapabilities.MongoDB,
		"metadata",
		"database size not applicable for instance connection",
	)
}

// GetTableCount is not applicable for instance connections.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewConfigurationError(
		dbcapabilities.MongoDB,
		"metadata",
		"table count not applicable for instance connection",
	)
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, i.conn.client, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "execute_command", err)
	}
	return result, nil
}

// CollectInstanceMetrics collects performance metrics from the MongoDB instance
func (i *InstanceMetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// Get server status for metrics
	serverStatus := i.conn.client.Database("admin").RunCommand(ctx, map[string]interface{}{"serverStatus": 1})
	var status map[string]interface{}
	if err := serverStatus.Decode(&status); err == nil {
		// Extract connection metrics
		if conns, ok := status["connections"].(map[string]interface{}); ok {
			if current, ok := conns["current"].(int32); ok {
				metrics["active_connections"] = current
			}
			if available, ok := conns["available"].(int32); ok {
				metrics["idle_connections"] = available
			}
		}

		// Extract cache metrics (WiredTiger)
		if wiredTiger, ok := status["wiredTiger"].(map[string]interface{}); ok {
			if cache, ok := wiredTiger["cache"].(map[string]interface{}); ok {
				// Calculate cache hit ratio if available
				metrics["extended_wiredtiger"] = cache
			}
		}

		// Check if replica set
		if replStatus, ok := status["repl"].(map[string]interface{}); ok {
			if setName, ok := replStatus["setName"].(string); ok && setName != "" {
				metrics["is_replica"] = true
				// Get replication lag if available
				if rbid, ok := replStatus["rbid"].(int32); ok {
					metrics["replica_set_id"] = rbid
				}
			} else {
				metrics["is_replica"] = false
			}
		}
	}

	return metrics, nil
}

// ListLogicalDatabases lists all logical databases in the MongoDB instance
func (i *InstanceMetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	// List all databases
	dbs, err := i.conn.client.ListDatabaseNames(ctx, map[string]interface{}{})
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "list_databases", err)
	}

	var databases []adapter.LogicalDatabaseInfo
	for _, dbName := range dbs {
		// Get database stats
		db := i.conn.client.Database(dbName)
		statsCmd := map[string]interface{}{"dbStats": 1}
		statsResult := db.RunCommand(ctx, statsCmd)

		var stats map[string]interface{}
		if err := statsResult.Decode(&stats); err == nil {
			dbInfo := adapter.LogicalDatabaseInfo{
				Name:  dbName,
				Owner: "admin", // MongoDB doesn't have per-database owners
			}

			// Get size if available
			if dataSize, ok := stats["dataSize"].(int64); ok {
				dbInfo.SizeBytes = dataSize
			} else if dataSize, ok := stats["dataSize"].(float64); ok {
				dbInfo.SizeBytes = int64(dataSize)
			}

			databases = append(databases, dbInfo)
		}
	}

	return databases, nil
}
