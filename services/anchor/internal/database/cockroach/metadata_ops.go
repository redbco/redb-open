package cockroach

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for CockroachDB database connections.
type MetadataOps struct {
	conn *Connection
}

// CollectDatabaseMetadata collects metadata about the CockroachDB database.
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectDatabaseMetadata function
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "collect_database_metadata", err)
	}
	return metadata, nil
}

// CollectInstanceMetadata collects metadata about the CockroachDB instance.
// Note: This is called on a database connection, not an instance connection.
func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectInstanceMetadata function
	metadata, err := CollectInstanceMetadata(ctx, m.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the CockroachDB version.
func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.CockroachDB, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns the unique identifier for the CockroachDB cluster.
func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var clusterID string
	err := m.conn.pool.QueryRow(ctx, "SELECT crdb_internal.cluster_id()::STRING").Scan(&clusterID)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.CockroachDB, "get_unique_identifier", err)
	}
	return clusterID, nil
}

// GetDatabaseSize returns the size of the database in bytes.
func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var size int64
	query := `
		SELECT COALESCE(SUM(range_size), 0) 
		FROM crdb_internal.ranges 
		WHERE database_name = current_database()
	`
	err := m.conn.pool.QueryRow(ctx, query).Scan(&size)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.CockroachDB, "get_database_size", err)
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
		return 0, adapter.WrapError(dbcapabilities.CockroachDB, "get_table_count", err)
	}
	return count, nil
}

// ExecuteCommand executes an administrative command.
func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// CockroachDB supports admin commands through SQL
	rows, err := m.conn.pool.Query(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "execute_command", err)
	}
	defer rows.Close()

	// Simple implementation: return success message
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

// InstanceMetadataOps implements adapter.MetadataOperator for CockroachDB instance connections.
type InstanceMetadataOps struct {
	conn *InstanceConnection
}

// CollectDatabaseMetadata is not applicable for instance connections.
func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.CockroachDB,
		"collect database metadata",
		"not available on instance connections",
	)
}

// CollectInstanceMetadata collects metadata about the CockroachDB instance.
func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	// Use existing CollectInstanceMetadata function
	metadata, err := CollectInstanceMetadata(ctx, i.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

// GetVersion returns the CockroachDB version.
func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.CockroachDB, "get_version", err)
	}
	return version, nil
}

// GetUniqueIdentifier returns the unique identifier for the CockroachDB cluster.
func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var clusterID string
	err := i.conn.pool.QueryRow(ctx, "SELECT crdb_internal.cluster_id()::STRING").Scan(&clusterID)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.CockroachDB, "get_unique_identifier", err)
	}
	return clusterID, nil
}

// GetDatabaseSize is not applicable for instance connections.
func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.CockroachDB,
		"get database size",
		"not available on instance connections",
	)
}

// GetTableCount is not applicable for instance connections.
func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(
		dbcapabilities.CockroachDB,
		"get table count",
		"not available on instance connections",
	)
}

// ExecuteCommand executes an administrative command.
func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	// Execute as a query and return results
	rows, err := i.conn.pool.Query(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "execute_command", err)
	}
	defer rows.Close()

	// Simple implementation: return success message
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}
