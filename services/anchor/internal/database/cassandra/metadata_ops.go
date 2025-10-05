package cassandra

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type MetadataOps struct {
	conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.session)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.session)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.session.Query("SELECT release_version FROM system.local").WithContext(ctx).Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Cassandra, "get_version", err)
	}
	return version, nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var clusterName string
	err := m.conn.session.Query("SELECT cluster_name FROM system.local").WithContext(ctx).Scan(&clusterName)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Cassandra, "get_unique_identifier", err)
	}
	return clusterName, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	// Cassandra doesn't provide direct keyspace size query
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "get database size", "requires nodetool or custom calculation")
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	iter := m.conn.session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ?", m.conn.config.DatabaseName).WithContext(ctx).Iter()
	iter.Scan(&count)

	if err := iter.Close(); err != nil {
		return 0, adapter.WrapError(dbcapabilities.Cassandra, "get_table_count", err)
	}
	return count, nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	err := m.conn.session.Query(command).WithContext(ctx).Exec()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "execute_command", err)
	}
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.session)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.session.Query("SELECT release_version FROM system.local").WithContext(ctx).Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Cassandra, "get_version", err)
	}
	return version, nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var clusterName string
	err := i.conn.session.Query("SELECT cluster_name FROM system.local").WithContext(ctx).Scan(&clusterName)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Cassandra, "get_unique_identifier", err)
	}
	return clusterName, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	err := i.conn.session.Query(command).WithContext(ctx).Exec()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Cassandra, "execute_command", err)
	}
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}
