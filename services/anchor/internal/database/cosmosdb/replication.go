package cosmosdb

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// ConnectReplication - placeholder for databases with limited CDC
func ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, common.ReplicationSourceInterface, error) {
	return nil, nil, fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// CreateReplicationSource - placeholder for databases with limited CDC
func CreateReplicationSource(db interface{}, tableNames []string, databaseID string, databaseName string, eventHandler func(map[string]interface{}), logger *logger.Logger) (interface{}, error) {
	return nil, fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// CheckLogicalReplicationPrerequisites - placeholder for databases with limited CDC
func CheckLogicalReplicationPrerequisites(db interface{}, logger *logger.Logger) error {
	return fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// GetReplicationStatus - placeholder for databases with limited CDC
func GetReplicationStatus(db interface{}, databaseID string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// ListReplicationSlots - placeholder for databases with limited CDC
func ListReplicationSlots(db interface{}, databaseID string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// DropReplicationSlot - placeholder for databases with limited CDC
func DropReplicationSlot(db interface{}, slotName string) error {
	return fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// StartReplication - placeholder for databases with limited CDC
func StartReplication(db interface{}, slotName string) error {
	return fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// StopReplication - placeholder for databases with limited CDC
func StopReplication(db interface{}, slotName string) error {
	return fmt.Errorf("traditional change data capture is not supported for CosmosDB. Use Change Feed instead")
}

// GetReplicationMetrics - placeholder for databases with limited CDC
func GetReplicationMetrics(db interface{}, databaseID string) (map[string]interface{}, error) {
	return map[string]interface{}{
		"supported": false,
		"reason":    "CosmosDB does not support traditional change data capture",
		"alternatives": []string{
			"CosmosDB Change Feed (built-in feature)",
			"Azure Functions with Cosmos DB trigger",
			"Azure Stream Analytics",
			"Azure Data Factory",
			"Azure Logic Apps",
		},
		"change_feed_info": map[string]interface{}{
			"description": "CosmosDB provides Change Feed for real-time data processing",
			"features": []string{
				"Real-time change notifications",
				"Guaranteed order within partition",
				"Incremental processing",
				"Serverless consumption",
			},
		},
	}, nil
}
