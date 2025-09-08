package chroma

import (
	"fmt"

	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// ConnectReplication - placeholder for databases without CDC
func ConnectReplication(config dbclient.ReplicationConfig) (*dbclient.ReplicationClient, dbclient.ReplicationSourceInterface, error) {
	return nil, nil, fmt.Errorf("change data capture is not supported for %s databases", config.ConnectionType)
}

// CreateReplicationSource - placeholder for databases without CDC
func CreateReplicationSource(client *ChromaClient, collectionNames []string, databaseID string, databaseName string, eventHandler func(map[string]interface{})) (dbclient.ReplicationSourceInterface, error) {
	return nil, fmt.Errorf("change data capture is not supported for Chroma databases")
}

// CheckLogicalReplicationPrerequisites - placeholder for databases without CDC
func CheckLogicalReplicationPrerequisites(client *ChromaClient) error {
	return fmt.Errorf("change data capture is not supported for Chroma databases")
}

// GetReplicationStatus - placeholder for databases without CDC
func GetReplicationStatus(client *ChromaClient, databaseID string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for Chroma databases")
}

// ListReplicationSlots - placeholder for databases without CDC
func ListReplicationSlots(client *ChromaClient, databaseID string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for Chroma databases")
}

// DropReplicationSlot - placeholder for databases without CDC
func DropReplicationSlot(client *ChromaClient, slotName string) error {
	return fmt.Errorf("change data capture is not supported for Chroma databases")
}
