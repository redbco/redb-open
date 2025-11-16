package kafka

import (
	"context"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Admin struct{}

func (a *Admin) ListTopics(ctx context.Context) ([]adapter.TopicInfo, error) {
	return []adapter.TopicInfo{}, nil
}

func (a *Admin) CreateTopic(ctx context.Context, name string, config adapter.TopicConfig) error {
	return nil
}

func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	return nil
}

func (a *Admin) GetTopicMetadata(ctx context.Context, topic string) (adapter.TopicMetadata, error) {
	return adapter.TopicMetadata{}, nil
}

func (a *Admin) GetTopicConfig(ctx context.Context, topic string) (adapter.TopicConfig, error) {
	return adapter.TopicConfig{}, nil
}
