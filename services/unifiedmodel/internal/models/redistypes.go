package models

// RedisSchema represents the schema of a Redis database
type RedisSchema struct {
	SchemaType string           `json:"schemaType"`
	Keys       []RedisKey       `json:"keys"`
	Modules    []RedisModule    `json:"modules"`
	Functions  []RedisFunction  `json:"functions"`
	Streams    []RedisStream    `json:"streams"`
	KeySpaces  []RedisKeySpace  `json:"keySpaces"`
	Extensions []RedisExtension `json:"extensions"`
}

// RedisKey represents a Redis key
type RedisKey struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"` // string, list, set, hash, zset, stream
	TTL         int64       `json:"ttl"`  // Time to live in seconds, -1 if no TTL
	Size        int64       `json:"size"` // Size in bytes
	SampleValue interface{} `json:"sampleValue,omitempty"`
}

// RedisModule represents a Redis module
type RedisModule struct {
	Name string `json:"name"`
}

// RedisFunction represents a Redis function (Redis 7.0+)
type RedisFunction struct {
	Name       string `json:"name"`
	Library    string `json:"library"`
	Arguments  string `json:"arguments"`
	ReturnType string `json:"returnType"`
	Body       string `json:"body"`
}

// RedisStream represents a Redis stream
type RedisStream struct {
	Name         string `json:"name"`
	Length       int64  `json:"length"`
	FirstEntryID string `json:"firstEntryID,omitempty"`
	LastEntryID  string `json:"lastEntryID,omitempty"`
	Groups       int    `json:"groups"`
}

// RedisKeySpace represents a Redis keyspace
type RedisKeySpace struct {
	ID      int   `json:"id"`
	Keys    int64 `json:"keys"`
	Expires int64 `json:"expires"`
	AvgTTL  int64 `json:"avgTTL"`
}

// RedisExtension represents a Redis extension
type RedisExtension struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Description string   `json:"description,omitempty"`
	Commands    []string `json:"commands,omitempty"`
}
