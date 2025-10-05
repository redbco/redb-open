package edgedb

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
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.client.QuerySingle(ctx, "SELECT sys::get_version_as_str();", &version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.EdgeDB, "get_version", err)
	}
	return version, nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	// EdgeDB doesn't have a built-in unique identifier
	return m.conn.config.DatabaseID, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "get database size", "not available via EdgeQL")
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	um, err := DiscoverSchema(m.conn.client)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.EdgeDB, "get_table_count", err)
	}
	return len(um.Tables), nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	err := m.conn.client.Execute(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "execute_command", err)
	}
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.client.QuerySingle(ctx, "SELECT sys::get_version_as_str();", &version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.EdgeDB, "get_version", err)
	}
	return version, nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	return i.conn.config.UniqueIdentifier, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.EdgeDB, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	err := i.conn.client.Execute(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.EdgeDB, "execute_command", err)
	}
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}
