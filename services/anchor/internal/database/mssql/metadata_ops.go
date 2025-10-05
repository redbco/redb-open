package mssql

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
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := m.conn.db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.SQLServer, "get_version", err)
	}
	return version, nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var serverName string
	err := m.conn.db.QueryRowContext(ctx, "SELECT @@SERVERNAME").Scan(&serverName)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.SQLServer, "get_unique_identifier", err)
	}
	return serverName, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	var sizeMB float64
	query := `
		SELECT SUM(CAST(size AS BIGINT) * 8 * 1024) 
		FROM sys.database_files
	`
	err := m.conn.db.QueryRowContext(ctx, query).Scan(&sizeMB)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.SQLServer, "get_database_size", err)
	}
	return int64(sizeMB), nil
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	var count int
	query := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'"
	err := m.conn.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.SQLServer, "get_table_count", err)
	}
	return count, nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	rows, err := m.conn.db.QueryContext(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "execute_command", err)
	}
	defer rows.Close()
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.db)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	var version string
	err := i.conn.db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.SQLServer, "get_version", err)
	}
	return version, nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	var serverName string
	err := i.conn.db.QueryRowContext(ctx, "SELECT @@SERVERNAME").Scan(&serverName)
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.SQLServer, "get_unique_identifier", err)
	}
	return serverName, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.SQLServer, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	rows, err := i.conn.db.QueryContext(ctx, command)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "execute_command", err)
	}
	defer rows.Close()
	result := fmt.Sprintf(`{"success": true, "command": "%s"}`, command)
	return []byte(result), nil
}
