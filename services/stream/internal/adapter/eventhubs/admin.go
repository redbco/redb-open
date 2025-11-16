package eventhubs

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Admin struct {
	conn *Connection
}

func (a *Admin) ListTopics(ctx context.Context) ([]adapter.TopicInfo, error) {
	// Event Hubs doesn't have a direct API to list all event hubs in a namespace from the data plane
	// This would typically require the management plane (Azure Resource Manager API)
	// For now, return the current event hub from the connection

	client, err := a.conn.getOrCreateProducer(ctx)
	if err != nil {
		return nil, err
	}

	props, err := client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get event hub properties: %w", err)
	}

	return []adapter.TopicInfo{
		{
			Name:       props.Name,
			Partitions: int32(len(props.PartitionIDs)),
			Replicas:   0, // Event Hubs handles replication internally
		},
	}, nil
}

func (a *Admin) CreateTopic(ctx context.Context, name string, config adapter.TopicConfig) error {
	// Creating Event Hubs requires Azure Resource Manager API (management plane)
	// Cannot be done from data plane SDK
	return fmt.Errorf("creating event hubs requires Azure Resource Manager API - not supported via data plane")
}

func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	// Deleting Event Hubs requires Azure Resource Manager API (management plane)
	// Cannot be done from data plane SDK
	return fmt.Errorf("deleting event hubs requires Azure Resource Manager API - not supported via data plane")
}

func (a *Admin) GetTopicMetadata(ctx context.Context, topic string) (adapter.TopicMetadata, error) {
	client, err := a.conn.getOrCreateProducer(ctx)
	if err != nil {
		return adapter.TopicMetadata{}, err
	}

	props, err := client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return adapter.TopicMetadata{}, fmt.Errorf("failed to get event hub properties: %w", err)
	}

	metadata := adapter.TopicMetadata{
		Name:       props.Name,
		Partitions: make([]adapter.PartitionMetadata, 0),
		Metadata: map[string]interface{}{
			"partition_count": len(props.PartitionIDs),
		},
	}

	// Get partition information
	for i, partitionID := range props.PartitionIDs {
		partitionProps, err := client.GetPartitionProperties(ctx, partitionID, nil)
		if err != nil {
			continue // Skip partitions we can't get info for
		}

		metadata.Partitions = append(metadata.Partitions, adapter.PartitionMetadata{
			ID:       int32(i),
			Leader:   partitionID,
			Replicas: []string{},
			ISR:      []string{},
			Offset: adapter.OffsetInfo{
				Oldest: partitionProps.BeginningSequenceNumber,
				Newest: partitionProps.LastEnqueuedSequenceNumber,
			},
		})
	}

	return metadata, nil
}

func (a *Admin) GetTopicConfig(ctx context.Context, topic string) (adapter.TopicConfig, error) {
	client, err := a.conn.getOrCreateProducer(ctx)
	if err != nil {
		return adapter.TopicConfig{}, err
	}

	props, err := client.GetEventHubProperties(ctx, nil)
	if err != nil {
		return adapter.TopicConfig{}, fmt.Errorf("failed to get event hub properties: %w", err)
	}

	return adapter.TopicConfig{
		NumPartitions:     int32(len(props.PartitionIDs)),
		ReplicationFactor: 0, // Event Hubs handles replication internally
		RetentionMs:       0, // Would need management API to get retention policy
	}, nil
}
