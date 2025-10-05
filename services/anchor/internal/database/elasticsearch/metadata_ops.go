package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// parseJSONResponse parses a JSON response from Elasticsearch
func parseJSONResponse(body io.ReadCloser, v interface{}) error {
	return json.NewDecoder(body).Decode(v)
}

type MetadataOps struct {
	conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectDatabaseMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Elasticsearch, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Elasticsearch, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	info, err := m.conn.client.Client.Info()
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Elasticsearch, "get_version", err)
	}
	defer info.Body.Close()

	// Parse response directly using Go's JSON
	var response struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := parseJSONResponse(info.Body, &response); err != nil {
		return "unknown", nil
	}
	return response.Version.Number, nil
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	info, err := m.conn.client.Client.Info()
	if err != nil {
		return m.conn.config.DatabaseID, nil
	}
	defer info.Body.Close()

	var response struct {
		ClusterName string `json:"cluster_name"`
	}
	if err := parseJSONResponse(info.Body, &response); err != nil {
		return m.conn.config.DatabaseID, nil
	}
	return response.ClusterName, nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Elasticsearch, "get database size", "requires stats API aggregation")
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	um, err := DiscoverSchema(m.conn.client)
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Elasticsearch, "get_table_count", err)
	}
	return len(um.Tables), nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result := fmt.Sprintf(`{"success": false, "error": "Elasticsearch uses REST API, not commands"}`)
	return []byte(result), nil
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Elasticsearch, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Elasticsearch, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	info, err := i.conn.client.Client.Info()
	if err != nil {
		return "unknown", nil
	}
	defer info.Body.Close()

	var response struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := parseJSONResponse(info.Body, &response); err != nil {
		return "unknown", nil
	}
	return response.Version.Number, nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	info, err := i.conn.client.Client.Info()
	if err != nil {
		return i.conn.config.UniqueIdentifier, nil
	}
	defer info.Body.Close()

	var response struct {
		ClusterName string `json:"cluster_name"`
	}
	if err := parseJSONResponse(info.Body, &response); err != nil {
		return i.conn.config.UniqueIdentifier, nil
	}
	return response.ClusterName, nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Elasticsearch, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Elasticsearch, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result := fmt.Sprintf(`{"success": false, "error": "Elasticsearch uses REST API, not commands"}`)
	return []byte(result), nil
}
