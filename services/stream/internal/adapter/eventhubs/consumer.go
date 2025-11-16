package eventhubs

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Consumer struct {
	conn             *Connection
	consumerGroup    string
	partitionClients map[string]*azeventhubs.PartitionClient
	cancelFuncs      map[string]context.CancelFunc
	mu               sync.RWMutex
}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.partitionClients == nil {
		c.partitionClients = make(map[string]*azeventhubs.PartitionClient)
		c.cancelFuncs = make(map[string]context.CancelFunc)
	}

	c.consumerGroup = groupID
	if c.consumerGroup == "" {
		c.consumerGroup = "$Default"
	}

	// Event Hubs topics are not explicitly subscribed to in the same way as Kafka
	// The event hub name is part of the connection string
	// We'll get the partition IDs when we start consuming

	return nil
}

func (c *Consumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
	// Get consumer client
	client, err := c.conn.getOrCreateConsumer(ctx, c.consumerGroup)
	if err != nil {
		return err
	}

	// Get event hub properties to find partition IDs
	props, err := client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get event hub properties: %w", err)
	}

	// Start consuming from all partitions
	errChan := make(chan error, len(props.PartitionIDs))
	var wg sync.WaitGroup

	for _, partitionID := range props.PartitionIDs {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()

			if err := c.consumePartition(ctx, client, pid, handler); err != nil && err != context.Canceled {
				errChan <- fmt.Errorf("error consuming partition %s: %w", pid, err)
			}
		}(partitionID)
	}

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

func (c *Consumer) consumePartition(ctx context.Context, client *azeventhubs.ConsumerClient, partitionID string, handler adapter.MessageHandler) error {
	// Get event hub name first
	props, err := client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get event hub properties: %w", err)
	}

	// Create partition client
	partitionClient, err := client.NewPartitionClient(partitionID, &azeventhubs.PartitionClientOptions{
		StartPosition: azeventhubs.StartPosition{
			Latest: to.Ptr(true),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create partition client: %w", err)
	}

	c.mu.Lock()
	c.partitionClients[partitionID] = partitionClient
	receiveCtx, cancel := context.WithCancel(ctx)
	c.cancelFuncs[partitionID] = cancel
	c.mu.Unlock()

	defer func() {
		partitionClient.Close(context.Background())
		c.mu.Lock()
		delete(c.partitionClients, partitionID)
		delete(c.cancelFuncs, partitionID)
		c.mu.Unlock()
	}()

	// Receive events
	for {
		select {
		case <-receiveCtx.Done():
			return receiveCtx.Err()
		default:
		}

		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return err
			}
			return fmt.Errorf("failed to receive events: %w", err)
		}

		for _, event := range events {
			// Handle partition key safely
			var key []byte
			if event.PartitionKey != nil && *event.PartitionKey != "" {
				key = []byte(*event.PartitionKey)
			}

			// Convert to adapter message
			adapterMsg := &adapter.Message{
				Topic:     props.Name, // Event Hub name
				Partition: parsePartitionID(partitionID),
				Key:       key,
				Value:     event.Body,
				Timestamp: *event.EnqueuedTime,
				Metadata: map[string]interface{}{
					"partition_id":    partitionID,
					"offset":          event.Offset,
					"sequence_number": event.SequenceNumber,
					"properties":      event.Properties,
				},
			}

			if err := handler(receiveCtx, adapterMsg); err != nil {
				return err
			}
		}
	}
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
	// Event Hubs checkpointing would require Azure Blob Storage
	// This is typically done through checkpoint store
	return nil
}

func (c *Consumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
	// Event Hubs seeking requires recreating the partition client with new start position
	// This would need to stop and restart the partition consumer
	return fmt.Errorf("seek not yet implemented for Event Hubs")
}

func (c *Consumer) Close() error {
	c.stopAll()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, client := range c.partitionClients {
		client.Close(context.Background())
	}
	c.partitionClients = make(map[string]*azeventhubs.PartitionClient)

	return nil
}

func parsePartitionID(pid string) int32 {
	var id int32
	fmt.Sscanf(pid, "%d", &id)
	return id
}
