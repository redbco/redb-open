package milvus

import (
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// CreateMilvusUnifiedModel creates a UnifiedModel for Milvus with database details
func CreateMilvusUnifiedModel(uniqueIdentifier, version string, databaseSize int64) *unifiedmodel.UnifiedModel {
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Milvus,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
	}
	return um
}

// ConvertMilvusCollection converts MilvusCollectionInfo to unifiedmodel.VectorIndex for Milvus
func ConvertMilvusCollection(collectionInfo MilvusCollectionInfo) unifiedmodel.VectorIndex {
	// Find vector field to get dimensions
	var dimensions int
	for _, field := range collectionInfo.Schema.Fields {
		if field.Type == "FloatVector" || field.Type == "BinaryVector" {
			if dim, ok := field.Params["dim"].(float64); ok {
				dimensions = int(dim)
			}
			break
		}
	}

	return unifiedmodel.VectorIndex{
		Name:      collectionInfo.Name,
		Dimension: dimensions,
		Metric:    "L2", // Default metric for Milvus
	}
}

// ConvertMilvusVector converts MilvusVector to unifiedmodel.Vector for Milvus
func ConvertMilvusVector(vectorInfo MilvusVector) unifiedmodel.Vector {
	return unifiedmodel.Vector{
		Name:      vectorInfo.ID,
		Dimension: len(vectorInfo.Vector),
		Metric:    "L2", // Default metric for Milvus
	}
}

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
