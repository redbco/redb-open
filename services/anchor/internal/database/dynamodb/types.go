package dynamodb

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
