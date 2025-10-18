package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

type MetadataOps struct {
	conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DynamoDB, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DynamoDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	// DynamoDB is a managed service, version is not exposed
	return "AWS Managed", nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	return m.conn.config.DatabaseID, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	result, err := m.conn.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DynamoDB, "get_database_size", err)
	}

	var totalSize int64
	for _, tableName := range result.TableNames {
		desc, err := m.conn.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: &tableName,
		})
		if err == nil && desc.Table != nil && desc.Table.TableSizeBytes != nil {
			totalSize += *desc.Table.TableSizeBytes
		}
	}
	return totalSize, nil
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	result, err := m.conn.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.DynamoDB, "get_table_count", err)
	}
	return len(result.TableNames), nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.DynamoDB, "execute_command", "DynamoDB uses AWS SDK, not SQL commands")
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.DynamoDB, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DynamoDB, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "AWS Managed", nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	return i.conn.config.UniqueIdentifier, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.DynamoDB, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.DynamoDB, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.DynamoDB, "execute_command", "DynamoDB uses AWS SDK, not SQL commands")
}
