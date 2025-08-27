package pinecone

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// CreatePineconeUnifiedModel creates a UnifiedModel for Pinecone with database details
func CreatePineconeUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Pinecone,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
		Namespaces:    make(map[string]unifiedmodel.Namespace),
	}
	return um
}

// ConvertPineconeIndex converts PineconeIndexInfo to unifiedmodel.VectorIndex for Pinecone
func ConvertPineconeIndex(indexInfo PineconeIndexInfo) unifiedmodel.VectorIndex {
	return unifiedmodel.VectorIndex{
		Name:      indexInfo.Name,
		Dimension: indexInfo.Dimension,
		Metric:    indexInfo.Metric,
	}
}

// ConvertPineconeVector converts PineconeVector to unifiedmodel.Vector for Pinecone
func ConvertPineconeVector(vectorInfo PineconeVector) unifiedmodel.Vector {
	return unifiedmodel.Vector{
		Name:      vectorInfo.ID,
		Dimension: len(vectorInfo.Values),
		Metric:    "cosine", // Default metric for Pinecone
	}
}

// PineconeIndexInfo represents information about a Pinecone index
type PineconeIndexInfo struct {
	Name           string            `json:"name"`
	Dimension      int               `json:"dimension"`
	Metric         string            `json:"metric"`
	Pods           int               `json:"pods"`
	Replicas       int               `json:"replicas"`
	PodType        string            `json:"podType"`
	Metadata       map[string]string `json:"metadata"`
	VectorCount    int64             `json:"vectorCount"`
	IndexSize      int64             `json:"indexSize"`
	Namespaces     []string          `json:"namespaces"`
	Shards         int               `json:"shards"`
	Status         string            `json:"status"`
	Environment    string            `json:"environment"`
	Region         string            `json:"region"`
	MetadataConfig MetadataConfig    `json:"metadataConfig"`
}

// PineconeCollectionInfo represents information about a Pinecone collection
type PineconeCollectionInfo struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	VectorCount int64  `json:"vectorCount"`
	Status      string `json:"status"`
	Environment string `json:"environment"`
	Region      string `json:"region"`
}

// MetadataConfig represents the metadata configuration for a Pinecone index
type MetadataConfig struct {
	Indexed []string `json:"indexed"`
}

// PineconeVector represents a vector in Pinecone
type PineconeVector struct {
	ID       string                 `json:"id"`
	Values   []float32              `json:"values"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// PineconeQueryRequest represents a query request to Pinecone
type PineconeQueryRequest struct {
	Namespace       string                 `json:"namespace,omitempty"`
	TopK            int                    `json:"topK"`
	Vector          []float32              `json:"vector,omitempty"`
	ID              string                 `json:"id,omitempty"`
	Filter          map[string]interface{} `json:"filter,omitempty"`
	IncludeValues   bool                   `json:"includeValues"`
	IncludeMetadata bool                   `json:"includeMetadata"`
}

// PineconeQueryResult represents a query result from Pinecone
type PineconeQueryResult struct {
	Matches   []PineconeMatch `json:"matches"`
	Namespace string          `json:"namespace,omitempty"`
}

// PineconeMatch represents a match in a Pinecone query result
type PineconeMatch struct {
	ID       string                 `json:"id"`
	Score    float32                `json:"score"`
	Values   []float32              `json:"values,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// PineconeClient represents a client for interacting with Pinecone
type PineconeClient struct {
	APIKey      string
	Environment string
	ProjectID   string
	BaseURL     string
	IsConnected int32 // Add this field for health checks
}
