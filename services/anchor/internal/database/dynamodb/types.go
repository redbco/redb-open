package dynamodb

import (
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DynamoDBDetails contains information about a DynamoDB database
type DynamoDBDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	Region           string `json:"region"`
	BillingMode      string `json:"billingMode"`
}

// DatabaseSchema represents the schema of a DynamoDB database
type DatabaseSchema struct {
	Tables     []common.TableInfo          `json:"tables"`
	EnumTypes  []common.EnumInfo           `json:"enumTypes"`
	Schemas    []common.DatabaseSchemaInfo `json:"schemas"`
	Functions  []common.FunctionInfo       `json:"functions"`
	Triggers   []common.TriggerInfo        `json:"triggers"`
	Sequences  []common.SequenceInfo       `json:"sequences"`
	Extensions []common.ExtensionInfo      `json:"extensions"`
}

// DynamoDBReplicationSourceDetails contains information about DynamoDB change streams (not supported)
type DynamoDBReplicationSourceDetails struct {
	TableName  string `json:"table_name"`
	DatabaseID string `json:"database_id"`
}

// DynamoDBReplicationChange represents a change in DynamoDB (not supported)
type DynamoDBReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}

// DynamoDBTableDetails contains detailed information about a DynamoDB table
type DynamoDBTableDetails struct {
	TableName              string                       `json:"tableName"`
	BillingMode            string                       `json:"billingMode"`
	TableStatus            string                       `json:"tableStatus"`
	CreationDateTime       string                       `json:"creationDateTime"`
	TableSizeBytes         int64                        `json:"tableSizeBytes"`
	ItemCount              int64                        `json:"itemCount"`
	GlobalSecondaryIndexes []DynamoDBSecondaryIndexInfo `json:"globalSecondaryIndexes"`
	LocalSecondaryIndexes  []DynamoDBSecondaryIndexInfo `json:"localSecondaryIndexes"`
}

// DynamoDBSecondaryIndexInfo contains information about DynamoDB secondary indexes
type DynamoDBSecondaryIndexInfo struct {
	IndexName        string   `json:"indexName"`
	KeySchema        []string `json:"keySchema"`
	ProjectionType   string   `json:"projectionType"`
	IndexSizeBytes   int64    `json:"indexSizeBytes"`
	ItemCount        int64    `json:"itemCount"`
	IndexStatus      string   `json:"indexStatus"`
	BillingMode      string   `json:"billingMode"`
	ProvisionedRead  int64    `json:"provisionedRead,omitempty"`
	ProvisionedWrite int64    `json:"provisionedWrite,omitempty"`
}
