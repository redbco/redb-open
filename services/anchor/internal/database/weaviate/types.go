package weaviate

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// CreateWeaviateUnifiedModel creates a UnifiedModel for Weaviate with database details
func CreateWeaviateUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Weaviate,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
		Types:         make(map[string]unifiedmodel.Type),
	}
	return um
}

// ConvertWeaviateClass converts WeaviateClassInfo to unifiedmodel.VectorIndex for Weaviate
func ConvertWeaviateClass(classInfo WeaviateClassInfo) unifiedmodel.VectorIndex {
	return unifiedmodel.VectorIndex{
		Name:      classInfo.Class,
		Dimension: 0,        // Weaviate doesn't expose vector dimensions directly
		Metric:    "cosine", // Default metric for Weaviate
	}
}

// ConvertWeaviateObject converts WeaviateObject to unifiedmodel.Vector for Weaviate
func ConvertWeaviateObject(objectInfo WeaviateObject) unifiedmodel.Vector {
	return unifiedmodel.Vector{
		Name:      objectInfo.ID,
		Dimension: len(objectInfo.Vector),
		Metric:    "cosine", // Default metric for Weaviate
	}
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
