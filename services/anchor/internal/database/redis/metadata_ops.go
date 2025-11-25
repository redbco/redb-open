package redis

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
		return nil, adapter.WrapError(dbcapabilities.Redis, "collect_database_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, m.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (m *MetadataOps) GetVersion(ctx context.Context) (string, error) {
	info, err := m.conn.client.Info(ctx, "server").Result()
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Redis, "get_version", err)
	}
	// Parse version from INFO output
	for _, line := range splitLines(info) {
		if len(line) > 13 && line[:13] == "redis_version" {
			return line[14:], nil
		}
	}
	return "", adapter.NewDatabaseError(dbcapabilities.Redis, "get_version", fmt.Errorf("version not found in INFO"))
}

func (m *MetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	info, err := m.conn.client.Info(ctx, "server").Result()
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Redis, "get_unique_identifier", err)
	}
	// Parse run_id from INFO output
	for _, line := range splitLines(info) {
		if len(line) > 6 && line[:6] == "run_id" {
			return line[7:], nil
		}
	}
	return "", nil
}

func (m *MetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	size, err := m.conn.client.DBSize(ctx).Result()
	if err != nil {
		return 0, adapter.WrapError(dbcapabilities.Redis, "get_database_size", err)
	}
	return size, nil
}

func (m *MetadataOps) GetTableCount(ctx context.Context) (int, error) {
	// Redis doesn't have tables
	return 0, nil
}

func (m *MetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := m.conn.client.Do(ctx, command).Result()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "execute_command", err)
	}
	return []byte(fmt.Sprintf("%v", result)), nil
}

func (m *MetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "collect instance metrics", "not available on database connections")
}

func (m *MetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "list logical databases", "not available on database connections")
}

type InstanceMetadataOps struct {
	conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "collect database metadata", "not available on instance connections")
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
	metadata, err := CollectInstanceMetadata(ctx, i.conn.client)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "collect_instance_metadata", err)
	}
	return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion(ctx context.Context) (string, error) {
	info, err := i.conn.client.Info(ctx, "server").Result()
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Redis, "get_version", err)
	}
	for _, line := range splitLines(info) {
		if len(line) > 13 && line[:13] == "redis_version" {
			return line[14:], nil
		}
	}
	return "", nil
}

func (i *InstanceMetadataOps) GetUniqueIdentifier(ctx context.Context) (string, error) {
	info, err := i.conn.client.Info(ctx, "server").Result()
	if err != nil {
		return "", adapter.WrapError(dbcapabilities.Redis, "get_unique_identifier", err)
	}
	for _, line := range splitLines(info) {
		if len(line) > 6 && line[:6] == "run_id" {
			return line[7:], nil
		}
	}
	return "", nil
}

func (i *InstanceMetadataOps) GetDatabaseSize(ctx context.Context) (int64, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "get database size", "not available on instance connections")
}

func (i *InstanceMetadataOps) GetTableCount(ctx context.Context) (int, error) {
	return 0, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "get table count", "not available on instance connections")
}

func (i *InstanceMetadataOps) ExecuteCommand(ctx context.Context, command string) ([]byte, error) {
	result, err := i.conn.client.Do(ctx, command).Result()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "execute_command", err)
	}
	return []byte(fmt.Sprintf("%v", result)), nil
}

func (i *InstanceMetadataOps) CollectInstanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	// Redis metrics can be collected from INFO command
	info, err := i.conn.client.Info(ctx).Result()
	if err == nil {
		// Parse basic metrics from info
		_ = info // Placeholder for future metric parsing
	}
	return metrics, nil
}

func (i *InstanceMetadataOps) ListLogicalDatabases(ctx context.Context) ([]adapter.LogicalDatabaseInfo, error) {
	// Redis has numbered databases (0-15 by default)
	var databases []adapter.LogicalDatabaseInfo
	// Get the number of databases from CONFIG
	configGet := i.conn.client.ConfigGet(ctx, "databases")
	if result, err := configGet.Result(); err == nil && len(result) >= 2 {
		// Create a simple entry for Redis
		databases = append(databases, adapter.LogicalDatabaseInfo{
			Name:      "redis",
			SizeBytes: 0, // Would need to iterate through all databases to get size
			Owner:     "redis",
		})
	}
	return databases, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' || s[i] == '\r' {
			if i > start {
				lines = append(lines, s[start:i])
			}
			start = i + 1
			if i+1 < len(s) && s[i] == '\r' && s[i+1] == '\n' {
				i++
				start = i + 1
			}
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
