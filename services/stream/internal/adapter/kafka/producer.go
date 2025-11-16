package kafka

import (
	"context"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Producer struct{}

func (p *Producer) Produce(ctx context.Context, topic string, messages []adapter.Message) error {
	return nil
}

func (p *Producer) ProduceAsync(ctx context.Context, topic string, messages []adapter.Message, callback func(error)) error {
	return nil
}

func (p *Producer) Flush(ctx context.Context) error {
	return nil
}

func (p *Producer) Close() error {
	return nil
}
