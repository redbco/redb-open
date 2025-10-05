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
