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
