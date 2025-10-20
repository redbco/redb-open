package prometheus

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// ReplicationOps implements replication operations for Prometheus.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether CDC/replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Prometheus supports federation and remote_write for replication
	return true
}

// GetSupportedMechanisms returns the list of supported CDC mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"federation", "remote_write", "remote_read"}
}

// CheckPrerequisites checks if prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Prometheus federation and remote write don't require specific prerequisites
	// Just verify connection works
	return r.conn.client.Ping(ctx)
}

// Connect establishes a CDC connection (not applicable for Prometheus).
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	return nil, fmt.Errorf("CDC connection not applicable for Prometheus - use federation or remote_write configuration")
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := map[string]interface{}{
		"supported":  true,
		"mechanisms": []string{"federation", "remote_write", "remote_read"},
		"message":    "Prometheus replication is configured via remote_write/remote_read endpoints and federation",
	}

	return status, nil
}

// GetLag returns the replication lag (not directly available).
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"message": "Replication lag monitoring requires checking remote_write queue metrics",
	}, nil
}

// ListSlots is not applicable for Prometheus.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("replication slots not applicable for Prometheus")
}

// DropSlot is not applicable for Prometheus.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return fmt.Errorf("replication slots not applicable for Prometheus")
}

// ListPublications is not applicable for Prometheus.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("publications not applicable for Prometheus")
}

// DropPublication is not applicable for Prometheus.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return fmt.Errorf("publications not applicable for Prometheus")
}

// ParseEvent is not implemented for Prometheus.
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	return nil, fmt.Errorf("ParseEvent not applicable for Prometheus")
}

// ApplyCDCEvent is not implemented for Prometheus.
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	return fmt.Errorf("ApplyCDCEvent not applicable for Prometheus")
}

// TransformData is not implemented for Prometheus.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("TransformData not applicable for Prometheus")
}
