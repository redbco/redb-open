package redis

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
