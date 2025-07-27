package redis

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// RedisDetails contains information about a Redis database
type RedisDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	KeyCount         int64  `json:"keyCount"`
	MemoryUsage      int64  `json:"memoryUsage"`
}

// RedisSchema represents the schema of a Redis database
type RedisSchema struct {
	Keys       []common.KeyInfo       `json:"keys"`
	Modules    []common.ModuleInfo    `json:"modules"`
	Functions  []common.FunctionInfo  `json:"functions"`
	Streams    []common.StreamInfo    `json:"streams"`
	KeySpaces  []common.KeySpaceInfo  `json:"keySpaces"`
	Extensions []common.ExtensionInfo `json:"extensions"`
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
