package clickhouse

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type MetadataOps struct {
	conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.conn)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.conn)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	row := m.conn.conn.QueryRow(ctx, "SELECT version()")
	if err := row.Scan(&version); err != nil {
		return "", adapter.WrapError(dbcapabilities.ClickHouse, "get_version", err)
	}
	return version, nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var serverID string
	row := m.conn.conn.QueryRow(ctx, "SELECT getMacro('cluster') as id")
	if err := row.Scan(&serverID); err != nil {
		return m.conn.config.DatabaseID, nil
	}
	return serverID, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var size int64
	row := m.conn.conn.QueryRow(ctx, "SELECT sum(bytes) FROM system.parts WHERE database = currentDatabase()")
	if err := row.Scan(&size); err != nil {
		return 0, adapter.WrapError(dbcapabilities.ClickHouse, "get_database_size", err)
	}
	return size, nil
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	row := m.conn.conn.QueryRow(ctx, "SELECT count(*) FROM system.tables WHERE database = currentDatabase()")
	if err := row.Scan(&count); err != nil {
		return 0, adapter.WrapError(dbcapabilities.ClickHouse, "get_table_count", err)
	}
	return count, nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, m.conn.conn, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "execute_command", err)
	}
	return result, nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.ClickHouse, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.conn)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	row := i.conn.conn.QueryRow(ctx, "SELECT version()")
	if err := row.Scan(&version); err != nil {
		return "", adapter.WrapError(dbcapabilities.ClickHouse, "get_version", err)
	}
	return version, nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var serverID string
	row := i.conn.conn.QueryRow(ctx, "SELECT getMacro('cluster') as id")
	if err := row.Scan(&serverID); err != nil {
		return i.conn.config.UniqueIdentifier, nil
	}
	return serverID, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.ClickHouse, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.ClickHouse, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, i.conn.conn, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.ClickHouse, "execute_command", err)
	}
	return result, nil
}
