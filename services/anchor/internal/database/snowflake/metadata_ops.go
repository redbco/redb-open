package snowflake

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type MetadataOps struct {
	conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.db.QueryRowContext(ctx, "SELECT CURRENT_VERSION()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Snowflake, "get_version", err)
	}
	return version, nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var account string
	err := m.conn.db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&account)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Snowflake, "get_unique_identifier", err)
	}
	return account, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var size int64
	err := m.conn.db.QueryRowContext(ctx, "SELECT SUM(BYTES) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = CURRENT_SCHEMA()").Scan(&size)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Snowflake, "get_database_size", err)
	}
	return size, nil
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	err := m.conn.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = CURRENT_SCHEMA()").Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Snowflake, "get_table_count", err)
	}
	return count, nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, m.conn.db, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "execute_command", err)
	}
	return result, nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.db.QueryRowContext(ctx, "SELECT CURRENT_VERSION()").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Snowflake, "get_version", err)
	}
	return version, nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var account string
	err := i.conn.db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&account)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Snowflake, "get_unique_identifier", err)
	}
	return account, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := ExecuteCommand(ctx, i.conn.db, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "execute_command", err)
	}
	return result, nil
}
