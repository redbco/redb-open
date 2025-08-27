package redis

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateRedisUnifiedModel creates a UnifiedModel for Redis with database details
func CreateRedisUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.Redis,
		Modules:      make(map[string]unifiedmodel.Module),
		Functions:    make(map[string]unifiedmodel.Function),
		Streams:      make(map[string]unifiedmodel.Stream),
		Namespaces:   make(map[string]unifiedmodel.Namespace),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}
	return um
}

// ConvertRedisModule converts common.ModuleInfo to unifiedmodel.Module for Redis
func ConvertRedisModule(moduleInfo common.ModuleInfo) unifiedmodel.Module {
	return unifiedmodel.Module{
		Name:    moduleInfo.Name,
		Comment: moduleInfo.Description,
	}
}

// ConvertRedisStream converts common.StreamInfo to unifiedmodel.Stream for Redis
func ConvertRedisStream(streamInfo common.StreamInfo) unifiedmodel.Stream {
	return unifiedmodel.Stream{
		Name: streamInfo.Name,
	}
}

// ConvertRedisKeySpace converts common.KeySpaceInfo to unifiedmodel.Namespace for Redis
func ConvertRedisKeySpace(keySpaceInfo common.KeySpaceInfo) unifiedmodel.Namespace {
	return unifiedmodel.Namespace{
		Name: fmt.Sprintf("db%d", keySpaceInfo.ID), // Use database ID as name
	}
}

// RedisReplicationSourceDetails contains information about a Redis replication source
type RedisReplicationSourceDetails struct {
	KeyPattern  string `json:"key_pattern"`
	ChannelName string `json:"channel_name"`
	DatabaseID  string `json:"database_id"`
}

// RedisReplicationChange represents a change in Redis data
type RedisReplicationChange struct {
	Operation string
	Key       string
	Data      interface{}
	OldData   interface{}
}
