package pubsub

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Producer struct {
	conn   *Connection
	topics map[string]*pubsub.Topic
	mu     sync.RWMutex
}

func (p *Producer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
	pubsubTopic, err := p.getOrCreateTopic(ctx, topic)
	if err != nil {
		return err
	}

	var results []*pubsub.PublishResult

	for _, msg := range messages {
		pubsubMsg := &pubsub.Message{
			Data: msg.Value,
		}

		// Add key as attribute
		if len(msg.Key) > 0 {
			if pubsubMsg.Attributes == nil {
				pubsubMsg.Attributes = make(map[string]string)
			}
			pubsubMsg.Attributes["key"] = string(msg.Key)
		}

		// Add custom metadata as attributes
		if msg.Metadata != nil {
			if pubsubMsg.Attributes == nil {
				pubsubMsg.Attributes = make(map[string]string)
			}
			for k, v := range msg.Metadata {
				if strVal, ok := v.(string); ok {
					pubsubMsg.Attributes[k] = strVal
				}
			}
		}

		result := pubsubTopic.Publish(ctx, pubsubMsg)
		results = append(results, result)
	}

	// Wait for all publishes to complete
	for _, result := range results {
		_, err := result.Get(ctx)
		if err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}

	return nil
}

func (p *Producer) ProduceAsync(ctx context.Context, topic string, messages []adapter.Message, callback func(error)) error {
	go func() {
		err := p.Produce(ctx, topic, messages)
		if callback != nil {
			callback(err)
		}
	}()
	return nil
}

func (p *Producer) Flush(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, topic := range p.topics {
		topic.Stop()
	}

	return nil
}

func (p *Producer) Close() error {
	return p.Flush(context.Background())
}

func (p *Producer) getOrCreateTopic(ctx context.Context, topicName string) (*pubsub.Topic, error) {
	p.mu.RLock()
	if p.topics == nil {
		p.mu.RUnlock()
		p.mu.Lock()
		p.topics = make(map[string]*pubsub.Topic)
		p.mu.Unlock()
		p.mu.RLock()
	}

	topic, exists := p.topics[topicName]
	p.mu.RUnlock()

	if exists {
		return topic, nil
	}

	// Get topic from client
	topic = p.conn.client.Topic(topicName)

	// Check if topic exists
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check topic existence: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("topic does not exist: %s", topicName)
	}

	// Configure topic settings
	topic.PublishSettings.CountThreshold = 100
	topic.PublishSettings.ByteThreshold = 1e6
	topic.PublishSettings.DelayThreshold = 100

	p.mu.Lock()
	p.topics[topicName] = topic
	p.mu.Unlock()

	return topic, nil
}
