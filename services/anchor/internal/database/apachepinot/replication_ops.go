package apachepinot

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// ReplicationOps implements replication operations for Pinot.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether CDC/replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Pinot supports realtime ingestion via Kafka
	return true
}

// GetSupportedMechanisms returns the list of supported CDC mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"kafka_ingestion", "pulsar_ingestion", "kinesis_ingestion"}
}

// CheckPrerequisites checks if prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Pinot realtime ingestion doesn't require specific prerequisites at DB level
	return r.conn.client.Ping(ctx)
}

// Connect establishes a CDC connection (not applicable for Pinot).
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	return nil, fmt.Errorf("CDC connection not applicable for Pinot - configure Kafka/Pulsar realtime ingestion instead")
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := map[string]interface{}{
		"supported":  true,
		"mechanisms": []string{"kafka_ingestion", "pulsar_ingestion", "kinesis_ingestion"},
		"message":    "Pinot replication is configured via realtime table configs",
	}

	return status, nil
}

// GetLag returns the replication lag (not directly available).
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"message": "Replication lag monitoring requires checking Kafka consumer lag or realtime segment metrics",
	}, nil
}

// ListSlots is not applicable for Pinot.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("replication slots not applicable for Pinot")
}

// DropSlot is not applicable for Pinot.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return fmt.Errorf("replication slots not applicable for Pinot")
}

// ListPublications is not applicable for Pinot.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("publications not applicable for Pinot")
}

// DropPublication is not applicable for Pinot.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return fmt.Errorf("publications not applicable for Pinot")
}

// ParseEvent is not implemented for Pinot.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	return nil, fmt.Errorf("ParseEvent not applicable for Pinot")
}

// ApplyCDCEvent is not implemented for Pinot.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	return fmt.Errorf("ApplyCDCEvent not applicable for Pinot")
}

// TransformData is not implemented for Pinot.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("TransformData not applicable for Pinot")
}
