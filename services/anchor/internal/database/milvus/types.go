package milvus

// MilvusCollectionInfo represents information about a Milvus collection
type MilvusCollectionInfo struct {
	Name        string                 `json:"name"`
	ID          string                 `json:"id"`
	Description string                 `json:"description"`
	Schema      MilvusCollectionSchema `json:"schema"`
	ShardsNum   int32                  `json:"shards_num"`
	Status      string                 `json:"status"`
	RowCount    int64                  `json:"row_count"`
	Size        int64                  `json:"size"`
}

// MilvusCollectionSchema represents the schema of a Milvus collection
type MilvusCollectionSchema struct {
	Fields []MilvusFieldInfo `json:"fields"`
}

// MilvusFieldInfo represents information about a field in a Milvus collection
type MilvusFieldInfo struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	PrimaryKey  bool                   `json:"primary_key"`
	AutoID      bool                   `json:"auto_id"`
	Description string                 `json:"description"`
	Params      map[string]interface{} `json:"params,omitempty"`
}

// MilvusVector represents a vector in Milvus
type MilvusVector struct {
	ID       string                 `json:"id"`
	Vector   []float32              `json:"vector"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// MilvusQueryRequest represents a query request to Milvus
type MilvusQueryRequest struct {
	CollectionName string                 `json:"collection_name"`
	Filter         string                 `json:"filter,omitempty"`
	OutputFields   []string               `json:"output_fields,omitempty"`
	Limit          int64                  `json:"limit"`
	Offset         int64                  `json:"offset,omitempty"`
	PartitionNames []string               `json:"partition_names,omitempty"`
	Expr           string                 `json:"expr,omitempty"`
	Params         map[string]interface{} `json:"params,omitempty"`
}

// MilvusQueryResult represents a query result from Milvus
type MilvusQueryResult struct {
	Status     string                   `json:"status"`
	Data       []map[string]interface{} `json:"data"`
	FieldsData []MilvusFieldData        `json:"fields_data"`
}

// MilvusFieldData represents field data in a query result
type MilvusFieldData struct {
	FieldName string        `json:"field_name"`
	Type      string        `json:"type"`
	Data      []interface{} `json:"data"`
}

// MilvusClient represents a client for interacting with Milvus
type MilvusClient struct {
	BaseURL     string
	Host        string
	Port        int
	Username    string
	Password    string
	SSL         bool
	IsConnected int32 // Add this field for health checks
}
