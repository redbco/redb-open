package kinesis

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Consumer struct {
	conn           *Connection
	streamName     string
	shardIterators map[string]string // shardID -> iterator
	stopChan       chan struct{}
}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
	if len(topics) == 0 {
		return fmt.Errorf("at least one stream name required")
	}

	// Kinesis only supports consuming from one stream at a time
	c.streamName = topics[0]
	c.shardIterators = make(map[string]string)
	c.stopChan = make(chan struct{})

	// Get shard iterators for all shards
	describeOutput, err := c.conn.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(c.streamName),
	})
	if err != nil {
		return fmt.Errorf("failed to describe stream: %w", err)
	}

	// Get iterator for each shard
	for _, shard := range describeOutput.StreamDescription.Shards {
		iteratorType := types.ShardIteratorTypeLatest
		if c.conn.config.AutoOffsetReset == "earliest" {
			iteratorType = types.ShardIteratorTypeTrimHorizon
		}

		iterOutput, err := c.conn.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        aws.String(c.streamName),
			ShardId:           shard.ShardId,
			ShardIteratorType: iteratorType,
		})
		if err != nil {
			return fmt.Errorf("failed to get shard iterator for %s: %w", *shard.ShardId, err)
		}

		c.shardIterators[*shard.ShardId] = *iterOutput.ShardIterator
	}

	return nil
}

func (c *Consumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
	if c.streamName == "" {
		return fmt.Errorf("not subscribed to any stream")
	}

	// Consume from all shards concurrently
	errChan := make(chan error, len(c.shardIterators))

	for shardID, iterator := range c.shardIterators {
		go c.consumeShard(ctx, shardID, iterator, handler, errChan)
	}

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		close(c.stopChan)
		return ctx.Err()
	case err := <-errChan:
		close(c.stopChan)
		return err
	}
}

func (c *Consumer) consumeShard(ctx context.Context, shardID, initialIterator string, handler adapter.MessageHandler, errChan chan error) {
	iterator := initialIterator

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
		}

		// Get records from shard
		output, err := c.conn.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: aws.String(iterator),
			Limit:         aws.Int32(100),
		})
		if err != nil {
			errChan <- fmt.Errorf("failed to get records from shard %s: %w", shardID, err)
			return
		}

		// Process records
		for _, record := range output.Records {
			msg := &adapter.Message{
				Topic:     c.streamName,
				Partition: 0, // Kinesis uses shards, map to partition 0
				Key:       []byte(*record.PartitionKey),
				Value:     record.Data,
				Timestamp: *record.ApproximateArrivalTimestamp,
				Metadata: map[string]interface{}{
					"shard_id":        shardID,
					"sequence_number": *record.SequenceNumber,
				},
			}

			if err := handler(ctx, msg); err != nil {
				errChan <- err
				return
			}
		}

		// Update iterator for next call
		if output.NextShardIterator == nil {
			// Shard has been closed
			return
		}
		iterator = *output.NextShardIterator

		// Small delay to avoid throttling
		if len(output.Records) == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Consumer) Commit(ctx context.Context) error {
	// Kinesis doesn't have explicit commit - checkpointing would be handled by application
	// For DynamoDB-based checkpointing, implement separately
	return nil
}

func (c *Consumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
	// Kinesis uses sequence numbers instead of offsets
	return fmt.Errorf("seek not implemented for Kinesis - use sequence numbers")
}

func (c *Consumer) Close() error {
	if c.stopChan != nil {
		close(c.stopChan)
	}
	return nil
}
