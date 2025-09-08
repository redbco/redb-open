package dynamodb

import (
	"fmt"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// ConnectReplication - placeholder for databases without CDC
func ConnectReplication(config dbclient.ReplicationConfig) (*dbclient.ReplicationClient, dbclient.ReplicationSourceInterface, error) {
	return nil, nil, fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// CreateReplicationSource - placeholder for databases without CDC
func CreateReplicationSource(db interface{}, tableNames []string, databaseID string, databaseName string, eventHandler func(map[string]interface{}), logger *logger.Logger) (interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// CheckLogicalReplicationPrerequisites - placeholder for databases without CDC
func CheckLogicalReplicationPrerequisites(db interface{}, logger *logger.Logger) error {
	return fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// GetReplicationStatus - placeholder for databases without CDC
func GetReplicationStatus(db interface{}, databaseID string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// ListReplicationSlots - placeholder for databases without CDC
func ListReplicationSlots(db interface{}, databaseID string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// DropReplicationSlot - placeholder for databases without CDC
func DropReplicationSlot(db interface{}, slotName string) error {
	return fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// StartReplication - placeholder for databases without CDC
func StartReplication(db interface{}, slotName string) error {
	return fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// StopReplication - placeholder for databases without CDC
func StopReplication(db interface{}, slotName string) error {
	return fmt.Errorf("change data capture is not supported for DynamoDB databases")
}

// GetReplicationMetrics - placeholder for databases without CDC
func GetReplicationMetrics(db interface{}, databaseID string) (map[string]interface{}, error) {
	return map[string]interface{}{
		"supported": false,
		"reason":    "DynamoDB does not support traditional change data capture",
		"alternatives": []string{
			"DynamoDB Streams (limited to 24 hours)",
			"AWS Data Pipeline",
			"AWS Database Migration Service (DMS)",
			"Amazon Kinesis Data Streams",
		},
	}, nil
}
