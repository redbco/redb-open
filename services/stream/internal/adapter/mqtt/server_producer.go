package mqtt

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

// ServerProducer handles message production from MQTT server to clients
type ServerProducer struct {
	conn *ServerConnection
}

func (p *ServerProducer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
	for _, msg := range messages {
		// Determine QoS from message metadata or use default
		qos := byte(1)
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

		// Publish to all subscribed clients using inline client
		if err := p.conn.server.Publish(publishTopic, msg.Value, retain, qos); err != nil {
			return fmt.Errorf("failed to publish message to topic %s: %w", publishTopic, err)
		}
	}

	return nil
}

func (p *ServerProducer) ProduceAsync(ctx context.Context, topic string, messages []adapter.Message, callback func(error)) error {
	go func() {
		err := p.Produce(ctx, topic, messages)
		if callback != nil {
			callback(err)
		}
	}()
	return nil
}

func (p *ServerProducer) Flush(ctx context.Context) error {
	// Server publishes are synchronous
	return nil
}

func (p *ServerProducer) Close() error {
	return nil
}
