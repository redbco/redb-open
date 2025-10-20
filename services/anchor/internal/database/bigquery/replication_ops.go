package bigquery

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// ReplicationOps implements replication operations for BigQuery.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether CDC/replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return false // BigQuery CDC would be implemented separately via change history
}

// GetSupportedMechanisms returns the list of supported CDC mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"change_history", "log_analytics"}
}

// CheckPrerequisites checks if prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	return fmt.Errorf("CDC not implemented for BigQuery")
}

// Connect establishes a CDC connection.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	return nil, fmt.Errorf("CDC not implemented for BigQuery")
}

// GetStatus returns the CDC status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"supported": false,
		"message":   "CDC not implemented for BigQuery",
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return nil, fmt.Errorf("CDC not implemented for BigQuery")
}

// ListSlots lists replication slots (not applicable for BigQuery).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("replication slots not applicable for BigQuery")
}

// DropSlot drops a replication slot (not applicable for BigQuery).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return fmt.Errorf("replication slots not applicable for BigQuery")
}

// ListPublications lists publications (not applicable for BigQuery).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("publications not applicable for BigQuery")
}

// DropPublication drops a publication (not applicable for BigQuery).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return fmt.Errorf("publications not applicable for BigQuery")
}

// ParseEvent parses a CDC event (not implemented).
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	return nil, fmt.Errorf("ParseEvent not implemented for BigQuery")
}

// ApplyCDCEvent applies a CDC event (not implemented).
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	return fmt.Errorf("ApplyCDCEvent not implemented for BigQuery")
}

// TransformData transforms data using transformation rules.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("TransformData not implemented for BigQuery")
}
