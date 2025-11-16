package mqtt

import (
	"context"
	"fmt"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

// ClientConsumer handles MQTT message consumption from broker
type ClientConsumer struct {
	conn     *ClientConnection
	stopChan chan struct{}
}

func (c *ClientConsumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()

	if c.stopChan == nil {
		c.stopChan = make(chan struct{})
	}

	// Subscribe to each topic
	for _, topic := range topics {
		// MQTT doesn't support consumer groups natively
		// Each client gets all messages matching the subscription
		token := c.conn.client.Subscribe(topic, c.conn.qos, nil)
		token.Wait()
		if err := token.Error(); err != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
		}

		c.conn.subscriptions[topic] = token
	}

	return nil
}

func (c *ClientConsumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
	if len(c.conn.subscriptions) == 0 {
		return fmt.Errorf("not subscribed to any topics")
	}

	// Create message callback
	msgChan := make(chan *adapter.Message, 100)
	errChan := make(chan error, 1)

	// Set up message handler for all subscribed topics
	c.conn.mu.RLock()
	for topic := range c.conn.subscriptions {
		c.setupTopicHandler(topic, msgChan, errChan)
	}
	c.conn.mu.RUnlock()

	// Process messages
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopChan:
			return nil
		case err := <-errChan:
			return err
		case msg := <-msgChan:
			if err := handler(ctx, msg); err != nil {
				return err
			}
		}
	}
}

func (c *ClientConsumer) setupTopicHandler(topic string, msgChan chan<- *adapter.Message, errChan chan<- error) {
	callback := func(client pahomqtt.Client, msg pahomqtt.Message) {
		adapterMsg := &adapter.Message{
			Topic:     msg.Topic(),
			Partition: 0, // MQTT doesn't have partitions
			Offset:    int64(msg.MessageID()),
			Key:       []byte{}, // MQTT doesn't have keys
			Value:     msg.Payload(),
			Timestamp: time.Now(), // MQTT v3 doesn't have timestamp, using current time
			Headers:   make(map[string]string),
			Metadata: map[string]interface{}{
				"qos":       msg.Qos(),
				"retained":  msg.Retained(),
				"duplicate": msg.Duplicate(),
			},
		}

		select {
		case msgChan <- adapterMsg:
		default:
			// Channel full, log warning
		}
	}

	// Resubscribe with handler
	token := c.conn.client.Subscribe(topic, c.conn.qos, callback)
	token.Wait()
	if err := token.Error(); err != nil {
		select {
		case errChan <- fmt.Errorf("failed to setup handler for topic %s: %w", topic, err):
		default:
		}
	}
}

func (c *ClientConsumer) Commit(ctx context.Context) error {
	// MQTT doesn't have explicit commits
	return nil
}

func (c *ClientConsumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
	// MQTT doesn't support seeking
	return fmt.Errorf("seek not supported for MQTT")
}

func (c *ClientConsumer) Close() error {
	if c.stopChan != nil {
		close(c.stopChan)
	}

	c.conn.mu.Lock()
	defer c.conn.mu.Unlock()

	// Unsubscribe from all topics
	for topic := range c.conn.subscriptions {
		token := c.conn.client.Unsubscribe(topic)
		token.Wait()
		// Ignore errors during cleanup
	}

	c.conn.subscriptions = make(map[string]pahomqtt.Token)
	return nil
}
