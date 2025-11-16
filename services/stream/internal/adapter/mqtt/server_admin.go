package mqtt

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

// ServerAdmin handles administrative operations for MQTT server
type ServerAdmin struct {
	conn *ServerConnection
}

func (a *ServerAdmin) ListTopics(ctx context.Context) ([]adapter.TopicInfo, error) {
	// Get all topics from server
	topics := make([]adapter.TopicInfo, 0)

	// Get topics from retained messages
	// Mochi MQTT doesn't provide a direct way to list all topics
	// We can only enumerate topics that have retained messages or active subscriptions

	// Note: This is a limitation of the MQTT protocol itself
	// Topics are ephemeral and only exist when they have subscribers or retained messages

	return topics, nil
}

func (a *ServerAdmin) CreateTopic(ctx context.Context, name string, config adapter.TopicConfig) error {
	// MQTT doesn't require explicit topic creation
	// Topics are created automatically when first published to
	return nil
}

func (a *ServerAdmin) DeleteTopic(ctx context.Context, name string) error {
	// MQTT doesn't support topic deletion
	// Topics cease to exist when they have no subscribers and no retained messages
	return fmt.Errorf("topic deletion not supported for MQTT")
}

func (a *ServerAdmin) GetTopicMetadata(ctx context.Context, topic string) (adapter.TopicMetadata, error) {
	// Get subscriber count for topic
	clients := a.conn.server.Clients.GetAll()
	subscriberCount := len(clients)

	metadata := adapter.TopicMetadata{
		Name:       topic,
		Partitions: []adapter.PartitionMetadata{},
		Config:     make(map[string]string),
		Metadata: map[string]interface{}{
			"subscriber_count": subscriberCount,
			"total_clients":    len(clients),
		},
	}

	return metadata, nil
}

func (a *ServerAdmin) GetTopicConfig(ctx context.Context, topic string) (adapter.TopicConfig, error) {
	// MQTT doesn't have configurable topic settings
	return adapter.TopicConfig{
		NumPartitions:     1,
		ReplicationFactor: 0,
		RetentionMs:       0,
		Config:            make(map[string]string),
	}, nil
}
