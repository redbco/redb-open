// Package adapter provides the unified interface for all streaming platform adapters.
// This package defines the contracts that platform-specific implementations must follow.
package adapter

import (
	"context"
	"time"

	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

// StreamAdapter represents a streaming platform adapter.
// Each platform type (Kafka, Kinesis, Pub/Sub, etc.) must implement this interface.
type StreamAdapter interface {
	// Type returns the canonical streaming platform identifier
	Type() streamcapabilities.StreamPlatform

	// Capabilities returns the capability metadata for this streaming platform
	Capabilities() streamcapabilities.Capability

	// Connect establishes a connection to the streaming platform
	Connect(ctx context.Context, config ConnectionConfig) (Connection, error)
}

// Connection represents an active connection to a streaming platform.
// This is the main interface for interacting with streams.
type Connection interface {
	// Identity and status
	ID() string
	Type() streamcapabilities.StreamPlatform
	IsConnected() bool

	// Lifecycle management
	Ping(ctx context.Context) error
	Close() error

	// Operation interfaces
	// Returns nil if the operation category is not supported by this platform
	ProducerOperations() ProducerOperator
	ConsumerOperations() ConsumerOperator
	AdminOperations() AdminOperator

	// Raw returns the underlying platform-specific connection object.
	// Use this only when you need to perform operations not covered by the standard interfaces.
	// Type assertion is required when using Raw().
	Raw() interface{}

	// Configuration
	Config() ConnectionConfig
	Adapter() StreamAdapter
}

// ProducerOperator handles message production operations.
type ProducerOperator interface {
	// Produce sends messages to a topic synchronously
	Produce(ctx context.Context, topic string, messages []Message) error

	// ProduceAsync sends messages to a topic asynchronously with a callback
	ProduceAsync(ctx context.Context, topic string, messages []Message, callback func(error)) error

	// Flush ensures all pending messages are sent
	Flush(ctx context.Context) error

	// Close closes the producer
	Close() error
}

// ConsumerOperator handles message consumption operations.
type ConsumerOperator interface {
	// Subscribe subscribes to one or more topics with a consumer group
	Subscribe(ctx context.Context, topics []string, groupID string) error

	// Consume starts consuming messages and calls the handler for each message
	// This is a blocking operation that runs until the context is cancelled
	Consume(ctx context.Context, handler MessageHandler) error

	// Commit commits the current offset
	Commit(ctx context.Context) error

	// Seek moves the consumer to a specific offset or timestamp
	Seek(ctx context.Context, topic string, partition int32, offset int64) error

	// Close closes the consumer
	Close() error
}

// AdminOperator handles administrative operations on the streaming platform.
type AdminOperator interface {
	// ListTopics returns a list of all topics/streams
	ListTopics(ctx context.Context) ([]TopicInfo, error)

	// CreateTopic creates a new topic with the specified configuration
	CreateTopic(ctx context.Context, name string, config TopicConfig) error

	// DeleteTopic deletes a topic
	DeleteTopic(ctx context.Context, name string) error

	// GetTopicMetadata retrieves metadata about a specific topic
	GetTopicMetadata(ctx context.Context, topic string) (TopicMetadata, error)

	// GetTopicConfig retrieves the configuration for a topic
	GetTopicConfig(ctx context.Context, topic string) (TopicConfig, error)
}

// MessageHandler is called for each message consumed from a topic.
// Return an error to stop consumption.
type MessageHandler func(ctx context.Context, msg *Message) error

// Message represents a message in a stream.
type Message struct {
	// Topic/stream the message belongs to
	Topic string

	// Partition (if the platform supports partitions)
	Partition int32

	// Offset within the partition
	Offset int64

	// Key for the message (optional, used for partitioning)
	Key []byte

	// Value is the message payload
	Value []byte

	// Headers contain metadata key-value pairs
	Headers map[string]string

	// Timestamp when the message was produced
	Timestamp time.Time

	// Platform-specific metadata
	Metadata map[string]interface{}
}

// TopicInfo contains basic information about a topic.
type TopicInfo struct {
	Name       string
	Partitions int32
	Replicas   int32
	Config     map[string]string
}

// TopicMetadata contains detailed metadata about a topic.
type TopicMetadata struct {
	Name       string
	Partitions []PartitionMetadata
	Config     map[string]string
	Metadata   map[string]interface{}
}

// PartitionMetadata contains metadata about a specific partition.
type PartitionMetadata struct {
	ID       int32
	Leader   string
	Replicas []string
	ISR      []string // In-Sync Replicas
	Offset   OffsetInfo
}

// OffsetInfo contains offset information for a partition.
type OffsetInfo struct {
	Oldest int64 // Oldest available offset
	Newest int64 // Newest available offset
}

// TopicConfig contains configuration for creating a topic.
type TopicConfig struct {
	NumPartitions     int32
	ReplicationFactor int32
	RetentionMs       int64
	Config            map[string]string
}

// ConnectionConfig contains the configuration for connecting to a streaming platform.
type ConnectionConfig struct {
	// Unique identifier for this connection
	ID string

	// Platform type
	Platform streamcapabilities.StreamPlatform

	// Connection details (platform-specific)
	Brokers       []string          // For Kafka, Redpanda
	Region        string            // For AWS Kinesis, SNS, SQS
	Project       string            // For GCP Pub/Sub
	Namespace     string            // For Azure Event Hubs
	Endpoint      string            // Generic endpoint for other platforms
	Configuration map[string]string // Additional platform-specific config

	// Authentication
	Username       string
	Password       string
	SASLMechanism  string // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, etc.
	CertFile       string // Path to TLS certificate
	KeyFile        string // Path to TLS key
	CAFile         string // Path to CA certificate
	TLSEnabled     bool
	TLSSkipVerify  bool
	Authentication map[string]string // Additional auth parameters

	// Consumer configuration
	GroupID          string
	AutoOffsetReset  string // earliest, latest
	EnableAutoCommit bool

	// Producer configuration
	Acks           string // 0, 1, all
	Compression    string // none, gzip, snappy, lz4, zstd
	MaxMessageSize int

	// Timeouts
	ConnectTimeout time.Duration
	RequestTimeout time.Duration

	// Metadata
	Metadata map[string]interface{}
}
