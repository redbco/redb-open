package pubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"google.golang.org/api/iterator"
)

type Admin struct {
	conn *Connection
}

func (a *Admin) ListTopics(ctx context.Context) ([]adapter.TopicInfo, error) {
	var topics []adapter.TopicInfo

	it := a.conn.client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list topics: %w", err)
		}

		topics = append(topics, adapter.TopicInfo{
			Name:       topic.ID(),
			Partitions: 1, // Pub/Sub doesn't have partitions
			Replicas:   0, // Pub/Sub handles replication internally
		})
	}

	return topics, nil
}

func (a *Admin) CreateTopic(ctx context.Context, name string, config adapter.TopicConfig) error {
	topic := a.conn.client.Topic(name)

	// Check if topic already exists
	exists, err := topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}
	if exists {
		return fmt.Errorf("topic already exists: %s", name)
	}

	// Create topic with configuration
	topicConfig := &pubsub.TopicConfig{}

	// Set retention if provided
	if config.RetentionMs > 0 {
		// Pub/Sub uses message retention duration
		// Note: Pub/Sub has different retention model than Kafka
	}

	_, err = a.conn.client.CreateTopicWithConfig(ctx, name, topicConfig)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	topic := a.conn.client.Topic(name)

	err := topic.Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	return nil
}

func (a *Admin) GetTopicMetadata(ctx context.Context, topicName string) (adapter.TopicMetadata, error) {
	topic := a.conn.client.Topic(topicName)

	// Check if topic exists
	exists, err := topic.Exists(ctx)
	if err != nil {
		return adapter.TopicMetadata{}, fmt.Errorf("failed to check topic existence: %w", err)
	}
	if !exists {
		return adapter.TopicMetadata{}, fmt.Errorf("topic does not exist: %s", topicName)
	}

	// Get topic configuration
	config, err := topic.Config(ctx)
	if err != nil {
		return adapter.TopicMetadata{}, fmt.Errorf("failed to get topic config: %w", err)
	}

	metadata := adapter.TopicMetadata{
		Name:       topicName,
		Partitions: []adapter.PartitionMetadata{}, // Pub/Sub doesn't have partitions
		Metadata: map[string]interface{}{
			"kms_key_name": config.KMSKeyName,
			"labels":       config.Labels,
		},
	}

	return metadata, nil
}

func (a *Admin) GetTopicConfig(ctx context.Context, topicName string) (adapter.TopicConfig, error) {
	topic := a.conn.client.Topic(topicName)

	// Check if topic exists
	exists, err := topic.Exists(ctx)
	if err != nil {
		return adapter.TopicConfig{}, fmt.Errorf("failed to check topic existence: %w", err)
	}
	if !exists {
		return adapter.TopicConfig{}, fmt.Errorf("topic does not exist: %s", topicName)
	}

	// Pub/Sub doesn't have the same configuration options as Kafka
	return adapter.TopicConfig{
		NumPartitions:     1,
		ReplicationFactor: 0, // Pub/Sub handles replication internally
		RetentionMs:       0, // Pub/Sub has different retention model
	}, nil
}
