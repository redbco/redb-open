package pinecone

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
		return nil, adapter.WrapError(dbcapabilities.Pinecone, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Pinecone, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "cloud", nil // Pinecone is a managed cloud service
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	return fmt.Sprintf("%s:%s", m.conn.client.Environment, m.conn.client.ProjectID), nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "get database size", "not available via API")
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	um, err := DiscoverSchema(m.conn.client)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Pinecone, "get_table_count", err)
	}
	return len(um.Tables), nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result := fmt.Sprintf(`{"success": false, "error": "Pinecone uses REST API, not commands"}`)
	return []byte(result), nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Pinecone, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	return "cloud", nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	return fmt.Sprintf("%s:%s", i.conn.client.Environment, i.conn.client.ProjectID), nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Pinecone, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result := fmt.Sprintf(`{"success": false, "error": "Pinecone uses REST API, not commands"}`)
	return []byte(result), nil
}
