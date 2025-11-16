package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Consumer struct {
	conn          *Connection
	subscriptions map[string]*pubsub.Subscription
	cancelFuncs   map[string]context.CancelFunc
	mu            sync.RWMutex
}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscriptions == nil {
		c.subscriptions = make(map[string]*pubsub.Subscription)
		c.cancelFuncs = make(map[string]context.CancelFunc)
	}

	for _, topic := range topics {
		// Create subscription name from groupID
		subscriptionName := groupID
		if subscriptionName == "" {
			subscriptionName = fmt.Sprintf("sub-%s-%d", topic, time.Now().Unix())
		}

		// Get or create subscription
		sub := c.conn.client.Subscription(subscriptionName)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check subscription existence: %w", err)
		}

		if !exists {
			// Create subscription
			topicObj := c.conn.client.Topic(topic)
			sub, err = c.conn.client.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{
				Topic:               topicObj,
				AckDeadline:         60 * time.Second,
				RetainAckedMessages: false,
			})
			if err != nil {
				return fmt.Errorf("failed to create subscription: %w", err)
			}
		}

		// Configure subscription settings
		sub.ReceiveSettings.MaxOutstandingMessages = 100
		sub.ReceiveSettings.MaxOutstandingBytes = 1e9
		sub.ReceiveSettings.NumGoroutines = 10

		c.subscriptions[topic] = sub
	}

	return nil
}

func (c *Consumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
	c.mu.RLock()
	if len(c.subscriptions) == 0 {
		c.mu.RUnlock()
		return fmt.Errorf("not subscribed to any topics")
	}

	// Start receiving from all subscriptions
	errChan := make(chan error, len(c.subscriptions))
	var wg sync.WaitGroup

	for topic, sub := range c.subscriptions {
		wg.Add(1)
		go func(t string, s *pubsub.Subscription) {
			defer wg.Done()

			receiveCtx, cancel := context.WithCancel(ctx)
			c.mu.Lock()
			c.cancelFuncs[t] = cancel
			c.mu.Unlock()

			err := s.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
				adapterMsg := &adapter.Message{
					Topic:     t,
					Partition: 0, // Pub/Sub doesn't have partitions
					Key:       []byte(msg.ID),
					Value:     msg.Data,
					Timestamp: msg.PublishTime,
					Metadata: map[string]interface{}{
						"message_id":   msg.ID,
						"attributes":   msg.Attributes,
						"ordering_key": msg.OrderingKey,
					},
				}

				if err := handler(ctx, adapterMsg); err != nil {
					msg.Nack()
					return
				}

				msg.Ack()
			})

			if err != nil && err != context.Canceled {
				errChan <- fmt.Errorf("error receiving from topic %s: %w", t, err)
			}
		}(topic, sub)
	}
	c.mu.RUnlock()

	// Wait for context cancellation or error
	go func() {
		wg.Wait()
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		c.stopAll()
		return ctx.Err()
	case err := <-errChan:
		if err != nil {
			c.stopAll()
			return err
		}
	}

	return nil
}

func (c *Consumer) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cancel := range c.cancelFuncs {
		cancel()
	}
	c.cancelFuncs = make(map[string]context.CancelFunc)
}

func (c *Consumer) Commit(ctx context.Context) error {
	// Pub/Sub handles acknowledgments automatically
	return nil
}

func (c *Consumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
	c.mu.RLock()
	sub, exists := c.subscriptions[topic]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("not subscribed to topic: %s", topic)
	}

	// Pub/Sub uses timestamps for seeking
	seekTime := time.Unix(offset, 0)
	err := sub.SeekToTime(ctx, seekTime)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	return nil
}

func (c *Consumer) Close() error {
	c.stopAll()
	return nil
}
