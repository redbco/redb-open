package druid

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// ReplicationOps implements replication operations for Druid.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether CDC/replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Druid supports streaming ingestion via Kafka
	return true
}

// GetSupportedMechanisms returns the list of supported CDC mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"kafka_ingestion", "kinesis_ingestion", "tranquility"}
}

// CheckPrerequisites checks if prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Druid streaming ingestion doesn't require specific prerequisites at DB level
	return r.conn.client.Ping(ctx)
}

// Connect establishes a CDC connection (not applicable for Druid).
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	return nil, fmt.Errorf("CDC connection not applicable for Druid - configure Kafka/Kinesis ingestion specs instead")
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := map[string]interface{}{
		"supported":  true,
		"mechanisms": []string{"kafka_ingestion", "kinesis_ingestion"},
		"message":    "Druid replication is configured via streaming ingestion specs",
	}

	return status, nil
}

// GetLag returns the replication lag (not directly available).
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"message": "Replication lag monitoring requires checking Kafka consumer lag or ingestion task metrics",
	}, nil
}

// ListSlots is not applicable for Druid.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("replication slots not applicable for Druid")
}

// DropSlot is not applicable for Druid.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return fmt.Errorf("replication slots not applicable for Druid")
}

// ListPublications is not applicable for Druid.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("publications not applicable for Druid")
}

// DropPublication is not applicable for Druid.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return fmt.Errorf("publications not applicable for Druid")
}

// ParseEvent is not implemented for Druid.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	return nil, fmt.Errorf("ParseEvent not applicable for Druid")
}

// ApplyCDCEvent is not implemented for Druid.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	return fmt.Errorf("ApplyCDCEvent not applicable for Druid")
}

// TransformData is not implemented for Druid.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("TransformData not applicable for Druid")
}
