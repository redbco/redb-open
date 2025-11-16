package eventhubs

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Producer struct {
	conn *Connection
}

func (p *Producer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
	client, err := p.conn.getOrCreateProducer(ctx)
	if err != nil {
		return err
	}

	// Create event batch
	batch, err := client.NewEventDataBatch(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}

	for _, msg := range messages {
		event := &azeventhubs.EventData{
			Body: msg.Value,
		}

		// Add custom properties
		if msg.Metadata != nil {
			event.Properties = make(map[string]any)
			for k, v := range msg.Metadata {
				event.Properties[k] = v
			}
		}

		// Partition key handling - EventHubs uses session routing which is separate
		// For now, we'll use batch options if available
		var addOpts *azeventhubs.AddEventDataOptions
		if len(msg.Key) > 0 {
			// Note: Azure Event Hubs doesn't have a direct PartitionKey in AddEventDataOptions
			// Instead, partition key is set at the EventData level or batch level
			// We'll add it as metadata for now
			if event.Properties == nil {
				event.Properties = make(map[string]any)
			}
			event.Properties["partition_key"] = string(msg.Key)
		}

		// Try to add event to batch
		err := batch.AddEventData(event, addOpts)
		if err != nil {
			// Batch is full, send it and create a new one
			if err := client.SendEventDataBatch(ctx, batch, nil); err != nil {
				return fmt.Errorf("failed to send batch: %w", err)
			}

			// Create new batch
			batch, err = client.NewEventDataBatch(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to create new batch: %w", err)
			}

			// Add event to new batch
			if err := batch.AddEventData(event, addOpts); err != nil {
				return fmt.Errorf("failed to add event to new batch: %w", err)
			}
		}
	}

	// Send remaining events in batch
	if batch.NumEvents() > 0 {
		if err := client.SendEventDataBatch(ctx, batch, nil); err != nil {
			return fmt.Errorf("failed to send final batch: %w", err)
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
	// Event Hubs SendEventDataBatch is synchronous, no flush needed
	return nil
}

func (p *Producer) Close() error {
	return nil
}
