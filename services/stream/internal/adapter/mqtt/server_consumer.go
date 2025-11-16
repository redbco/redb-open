package mqtt

import (
	"context"
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

// ServerConsumer handles message consumption from MQTT server
type ServerConsumer struct {
	conn          *ServerConnection
	subscriptions map[string]bool
	stopChan      chan struct{}
	mu            sync.RWMutex
}

func (c *ServerConsumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscriptions == nil {
		c.subscriptions = make(map[string]bool)
	}

	if c.stopChan == nil {
		c.stopChan = make(chan struct{})
	}

	// Mark topics as subscribed
	for _, topic := range topics {
		c.subscriptions[topic] = true
	}

	// Note: Message capture would require implementing a full Hook interface
	// For now, this is a simplified implementation
	// In production, would use proper pub/sub hooks to capture messages

	return nil
}

func (c *ServerConsumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
	c.mu.RLock()
	if len(c.subscriptions) == 0 {
		c.mu.RUnlock()
		return fmt.Errorf("not subscribed to any topics")
	}
	c.mu.RUnlock()

	// Process messages from buffer
	// Note: In production, this would be populated by message hooks
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopChan:
			return nil
		case msg := <-c.conn.messageBuffer:
			if err := handler(ctx, msg); err != nil {
				return err
			}
		}
	}
}

func (c *ServerConsumer) Commit(ctx context.Context) error {
	// MQTT server doesn't have commits
	return nil
}

func (c *ServerConsumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
	// MQTT doesn't support seeking
	return fmt.Errorf("seek not supported for MQTT server")
}

func (c *ServerConsumer) Close() error {
	if c.stopChan != nil {
		close(c.stopChan)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscriptions = make(map[string]bool)

	return nil
}

// Note: MessageCaptureHook implementation would go here
// For full production use, would implement complete mqtt.Hook interface
// This would require implementing all hook methods from the Mochi MQTT library

// matchTopic checks if a topic matches a subscription pattern
func matchTopic(subscription, topic string) bool {
	// Simple wildcard matching
	// # matches multiple levels
	// + matches single level
	if subscription == "#" {
		return true
	}

	if subscription == topic {
		return true
	}

	// TODO: Implement full MQTT topic matching with wildcards
	// For now, simple exact match
	return false
}
