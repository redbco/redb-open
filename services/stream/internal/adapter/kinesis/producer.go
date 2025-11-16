package kinesis

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Producer struct {
	conn *Connection
}

func (p *Producer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
	for _, msg := range messages {
		partitionKey := string(msg.Key)
		if partitionKey == "" {
			partitionKey = "default"
		}

		input := &kinesis.PutRecordInput{
			StreamName:   aws.String(topic),
			Data:         msg.Value,
			PartitionKey: aws.String(partitionKey),
		}

		_, err := p.conn.client.PutRecord(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to produce message: %w", err)
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
	// Kinesis PutRecord is synchronous, no flush needed
	return nil
}

func (p *Producer) Close() error {
	return nil
}
