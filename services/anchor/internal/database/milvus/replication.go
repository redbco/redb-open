package milvus

import (
	"fmt"

	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// ConnectReplication - placeholder for databases without CDC
func ConnectReplication(config dbclient.ReplicationConfig) (*dbclient.ReplicationClient, dbclient.ReplicationSourceInterface, error) {
	return nil, nil, fmt.Errorf("change data capture is not supported for %s databases", config.ConnectionType)
}

// CreateReplicationSource - placeholder for databases without CDC
func CreateReplicationSource(client *MilvusClient, collectionNames []string, databaseID string, databaseName string, eventHandler func(map[string]interface{})) (dbclient.ReplicationSourceInterface, error) {
	return nil, fmt.Errorf("change data capture is not supported for Milvus databases")
}

// CheckLogicalReplicationPrerequisites - placeholder for databases without CDC
func CheckLogicalReplicationPrerequisites(client *MilvusClient) error {
	return fmt.Errorf("change data capture is not supported for Milvus databases")
}

// GetReplicationStatus - placeholder for databases without CDC
func GetReplicationStatus(client *MilvusClient, databaseID string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for Milvus databases")
}

// ListReplicationSlots - placeholder for databases without CDC
func ListReplicationSlots(client *MilvusClient, databaseID string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for Milvus databases")
}

// DropReplicationSlot - placeholder for databases without CDC
func DropReplicationSlot(client *MilvusClient, slotName string) error {
	return fmt.Errorf("change data capture is not supported for Milvus databases")
}
