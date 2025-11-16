package kinesis

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Admin struct {
	conn *Connection
}

func (a *Admin) ListTopics(ctx context.Context) ([]adapter.TopicInfo, error) {
	var topics []adapter.TopicInfo
	var nextToken *string

	for {
		input := &kinesis.ListStreamsInput{
			Limit:                    aws.Int32(100),
			ExclusiveStartStreamName: nextToken,
		}

		output, err := a.conn.client.ListStreams(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list streams: %w", err)
		}

		for _, streamName := range output.StreamNames {
			// Get stream details
			describeOutput, err := a.conn.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
				StreamName: aws.String(streamName),
			})
			if err != nil {
				continue // Skip streams we can't describe
			}

			topics = append(topics, adapter.TopicInfo{
				Name:       streamName,
				Partitions: int32(len(describeOutput.StreamDescription.Shards)),
				Replicas:   0, // Kinesis doesn't expose replica count
			})
		}

		if output.HasMoreStreams != nil && !*output.HasMoreStreams {
			break
		}

		if len(output.StreamNames) > 0 {
			lastStream := output.StreamNames[len(output.StreamNames)-1]
			nextToken = &lastStream
		} else {
			break
		}
	}

	return topics, nil
}

func (a *Admin) CreateTopic(ctx context.Context, name string, config adapter.TopicConfig) error {
	input := &kinesis.CreateStreamInput{
		StreamName: aws.String(name),
		ShardCount: aws.Int32(config.NumPartitions),
	}

	if config.NumPartitions == 0 {
		input.ShardCount = aws.Int32(1)
	}

	_, err := a.conn.client.CreateStream(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}

func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	input := &kinesis.DeleteStreamInput{
		StreamName: aws.String(name),
	}

	_, err := a.conn.client.DeleteStream(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete stream: %w", err)
	}

	return nil
}

func (a *Admin) GetTopicMetadata(ctx context.Context, topic string) (adapter.TopicMetadata, error) {
	describeOutput, err := a.conn.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(topic),
	})
	if err != nil {
		return adapter.TopicMetadata{}, fmt.Errorf("failed to describe stream: %w", err)
	}

	desc := describeOutput.StreamDescription
	metadata := adapter.TopicMetadata{
		Name:       topic,
		Partitions: make([]adapter.PartitionMetadata, 0),
		Metadata: map[string]interface{}{
			"stream_arn":      *desc.StreamARN,
			"stream_status":   string(desc.StreamStatus),
			"retention_hours": *desc.RetentionPeriodHours,
		},
	}

	// Convert shards to partition metadata
	for i, shard := range desc.Shards {
		metadata.Partitions = append(metadata.Partitions, adapter.PartitionMetadata{
			ID:       int32(i),
			Leader:   *shard.ShardId,
			Replicas: []string{},
			ISR:      []string{},
			Offset: adapter.OffsetInfo{
				Oldest: 0,
				Newest: 0,
			},
		})
	}

	return metadata, nil
}

func (a *Admin) GetTopicConfig(ctx context.Context, topic string) (adapter.TopicConfig, error) {
	describeOutput, err := a.conn.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(topic),
	})
	if err != nil {
		return adapter.TopicConfig{}, fmt.Errorf("failed to describe stream: %w", err)
	}

	return adapter.TopicConfig{
		NumPartitions:     int32(len(describeOutput.StreamDescription.Shards)),
		ReplicationFactor: 0, // Kinesis handles replication internally
		RetentionMs:       int64(*describeOutput.StreamDescription.RetentionPeriodHours) * 3600000,
	}, nil
}
