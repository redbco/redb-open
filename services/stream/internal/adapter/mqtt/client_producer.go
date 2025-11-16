package mqtt

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

// ClientProducer handles MQTT message production to broker
type ClientProducer struct {
	conn *ClientConnection
}

func (p *ClientProducer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
	for _, msg := range messages {
		// Determine QoS from message metadata or use default
		qos := p.conn.qos
		if msg.Metadata != nil {
			if q, ok := msg.Metadata["qos"].(byte); ok {
				qos = q
			} else if q, ok := msg.Metadata["qos"].(int); ok {
				qos = byte(q)
			}
		}

		// Determine retain flag
		retain := false
		if msg.Metadata != nil {
			if r, ok := msg.Metadata["retained"].(bool); ok {
				retain = r
			}
		}

		// Use message topic if provided, otherwise use specified topic
		publishTopic := topic
		if msg.Topic != "" {
			publishTopic = msg.Topic
		}

		// Publish message
		token := p.conn.client.Publish(publishTopic, qos, retain, msg.Value)
		token.Wait()
		if err := token.Error(); err != nil {
			return fmt.Errorf("failed to publish message to topic %s: %w", publishTopic, err)
		}
	}

	return nil
}

func (p *ClientProducer) ProduceAsync(ctx context.Context, topic string, messages []adapter.Message, callback func(error)) error {
	go func() {
		err := p.Produce(ctx, topic, messages)
		if callback != nil {
			callback(err)
		}
	}()
	return nil
}

func (p *ClientProducer) Flush(ctx context.Context) error {
	// MQTT client library handles message queuing internally
	// Wait for a short period to allow pending messages to be sent
	// This is a best-effort flush
	return nil
}

func (p *ClientProducer) Close() error {
	return nil
}
