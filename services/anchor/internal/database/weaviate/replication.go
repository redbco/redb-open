package weaviate

import (
	"fmt"

	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// ConnectReplication - placeholder for databases without CDC
func ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, common.ReplicationSourceInterface, error) {
	return nil, nil, fmt.Errorf("change data capture is not supported for %s databases", config.ConnectionType)
}

// CreateReplicationSource - placeholder for databases without CDC
func CreateReplicationSource(client *WeaviateClient, classNames []string, databaseID string, databaseName string, eventHandler func(map[string]interface{})) (common.ReplicationSourceInterface, error) {
	return nil, fmt.Errorf("change data capture is not supported for Weaviate databases")
}

// CheckLogicalReplicationPrerequisites - placeholder for databases without CDC
func CheckLogicalReplicationPrerequisites(client *WeaviateClient) error {
	return fmt.Errorf("change data capture is not supported for Weaviate databases")
}

// GetReplicationStatus - placeholder for databases without CDC
func GetReplicationStatus(client *WeaviateClient, databaseID string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for Weaviate databases")
}

// ListReplicationSlots - placeholder for databases without CDC
func ListReplicationSlots(client *WeaviateClient, databaseID string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for Weaviate databases")
}

// DropReplicationSlot - placeholder for databases without CDC
func DropReplicationSlot(client *WeaviateClient, slotName string) error {
	return fmt.Errorf("change data capture is not supported for Weaviate databases")
}
