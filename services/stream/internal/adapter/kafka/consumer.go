package kafka

import (
	"context"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Consumer struct{}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, groupID string) error {
	return nil
}

func (c *Consumer) Consume(ctx context.Context, handler adapter.MessageHandler) error {
	return nil
}

func (c *Consumer) Commit(ctx context.Context) error {
	return nil
}

func (c *Consumer) Seek(ctx context.Context, topic string, partition int32, offset int64) error {
	return nil
}

func (c *Consumer) Close() error {
	return nil
}
