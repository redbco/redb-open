package databricks

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// ReplicationOps implements replication operations for Databricks.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether CDC/replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return false // Databricks CDC via Delta Lake CDF would be implemented separately
}

// GetSupportedMechanisms returns the list of supported CDC mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"delta_cdf"}
}

// CheckPrerequisites checks if prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	return fmt.Errorf("CDC not implemented for Databricks")
}

// Connect establishes a CDC connection.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	return nil, fmt.Errorf("CDC not implemented for Databricks")
}

// GetStatus returns the CDC status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"supported": false,
		"message":   "CDC not implemented for Databricks",
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return nil, fmt.Errorf("CDC not implemented for Databricks")
}

// ListSlots lists replication slots (not applicable for Databricks).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("replication slots not applicable for Databricks")
}

// DropSlot drops a replication slot (not applicable for Databricks).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return fmt.Errorf("replication slots not applicable for Databricks")
}

// ListPublications lists publications (not applicable for Databricks).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("publications not applicable for Databricks")
}

// DropPublication drops a publication (not applicable for Databricks).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return fmt.Errorf("publications not applicable for Databricks")
}

// ParseEvent parses a CDC event (not implemented).
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	return nil, fmt.Errorf("ParseEvent not implemented for Databricks")
}

// ApplyCDCEvent applies a CDC event (not implemented).
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	return fmt.Errorf("ApplyCDCEvent not implemented for Databricks")
}

// TransformData transforms data using transformation rules.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("TransformData not implemented for Databricks")
}
