package dynamodb

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// CreateDynamoDBUnifiedModel creates a UnifiedModel for DynamoDB with database details
func CreateDynamoDBUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.DynamoDB,
		Tables:       make(map[string]unifiedmodel.Table),
		Indexes:      make(map[string]unifiedmodel.Index),
	}
	return um
}

// ConvertDynamoDBTable converts common.TableInfo to unifiedmodel.Table for DynamoDB
func ConvertDynamoDBTable(tableInfo common.TableInfo) unifiedmodel.Table {
	table := unifiedmodel.Table{
		Name:        tableInfo.Name,
		Columns:     make(map[string]unifiedmodel.Column),
		Indexes:     make(map[string]unifiedmodel.Index),
		Constraints: make(map[string]unifiedmodel.Constraint),
	}

	// Convert columns (DynamoDB has flexible schema, but we can represent known attributes)
	for _, col := range tableInfo.Columns {
		var defaultValue string
		if col.ColumnDefault != nil {
			defaultValue = *col.ColumnDefault
		}
		table.Columns[col.Name] = unifiedmodel.Column{
			Name:           col.Name,
			DataType:       col.DataType,
			Nullable:       col.IsNullable,
			Default:        defaultValue,
			IsPrimaryKey:   col.IsPrimaryKey,
			IsPartitionKey: true, // DynamoDB partition key
		}
	}

	// Convert indexes (DynamoDB has GSI and LSI)
	for _, idx := range tableInfo.Indexes {
		table.Indexes[idx.Name] = unifiedmodel.Index{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.IsUnique,
		}
	}

	return table
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
