package weaviate

// WeaviateDetails contains information about a Weaviate vector database
type WeaviateDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	Host             string `json:"host"`
	Port             int    `json:"port"`
	ClassCount       int64  `json:"classCount"`
	TotalObjects     int64  `json:"totalObjects"`
}

// WeaviateSchema represents the schema of a Weaviate vector database
type WeaviateSchema struct {
	Classes []WeaviateClassInfo `json:"classes"`
}

// WeaviateClassInfo represents information about a Weaviate class
type WeaviateClassInfo struct {
	Class               string                 `json:"class"`
	Description         string                 `json:"description"`
	Properties          []WeaviatePropertyInfo `json:"properties"`
	Vectorizer          string                 `json:"vectorizer"`
	ModuleConfig        map[string]interface{} `json:"moduleConfig"`
	ShardingConfig      map[string]interface{} `json:"shardingConfig"`
	ReplicationConfig   map[string]interface{} `json:"replicationConfig"`
	MultiTenancyConfig  map[string]interface{} `json:"multiTenancyConfig"`
	InvertedIndexConfig map[string]interface{} `json:"invertedIndexConfig"`
	ObjectCount         int64                  `json:"objectCount"`
	Size                int64                  `json:"size"`
}

// WeaviatePropertyInfo represents information about a property in a Weaviate class
type WeaviatePropertyInfo struct {
	Name            string                 `json:"name"`
	DataType        []string               `json:"dataType"`
	Description     string                 `json:"description"`
	ModuleConfig    map[string]interface{} `json:"moduleConfig"`
	IndexInverted   bool                   `json:"indexInverted"`
	IndexFilterable bool                   `json:"indexFilterable"`
	IndexSearchable bool                   `json:"indexSearchable"`
	Tokenization    string                 `json:"tokenization"`
}

// WeaviateObject represents an object in Weaviate
type WeaviateObject struct {
	ID         string                 `json:"id"`
	Class      string                 `json:"class"`
	Properties map[string]interface{} `json:"properties"`
	Vector     []float32              `json:"vector,omitempty"`
	Additional map[string]interface{} `json:"additional,omitempty"`
}

// WeaviateQueryRequest represents a query request to Weaviate
type WeaviateQueryRequest struct {
	Class      string                 `json:"class"`
	Properties []string               `json:"properties,omitempty"`
	Where      map[string]interface{} `json:"where,omitempty"`
	Limit      int                    `json:"limit"`
	Offset     int                    `json:"offset,omitempty"`
	Additional []string               `json:"additional,omitempty"`
	Sort       []map[string]string    `json:"sort,omitempty"`
	Group      map[string]interface{} `json:"group,omitempty"`
}

// WeaviateQueryResult represents a query result from Weaviate
type WeaviateQueryResult struct {
	Data WeaviateQueryData `json:"data"`
}

// WeaviateQueryData represents the data in a query result
type WeaviateQueryData struct {
	Get map[string][]WeaviateObject `json:"Get"`
}

// WeaviateClient represents a client for interacting with Weaviate
type WeaviateClient struct {
	BaseURL     string
	Host        string
	Port        int
	Username    string
	Password    string
	SSL         bool
	IsConnected int32 // Add this field for health checks
}
