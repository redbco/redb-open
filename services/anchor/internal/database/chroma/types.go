package chroma

import (
	chromav2 "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// ConvertChromaCollection converts ChromaCollectionInfo to unifiedmodel.VectorIndex for Chroma
func ConvertChromaCollection(collectionInfo ChromaCollectionInfo) unifiedmodel.VectorIndex {
	return unifiedmodel.VectorIndex{
		Name:      collectionInfo.Name,
		Dimension: collectionInfo.Dimension,
		Metric:    collectionInfo.DistanceFunction,
	}
}

// ConvertChromaVector converts ChromaVector to unifiedmodel.Vector for Chroma
func ConvertChromaVector(vectorInfo ChromaVector) unifiedmodel.Vector {
	return unifiedmodel.Vector{
		Name:      vectorInfo.ID,
		Dimension: len(vectorInfo.Embedding),
		Metric:    "cosine", // Default metric for Chroma
	}
}

// ChromaCollectionInfo represents information about a Chroma collection
type ChromaCollectionInfo struct {
	Name              string                 `json:"name"`
	ID                string                 `json:"id"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	Count             int64                  `json:"count"`
	Size              int64                  `json:"size"`
	EmbeddingFunction string                 `json:"embedding_function,omitempty"`
	Dimension         int                    `json:"dimension,omitempty"`
	DistanceFunction  string                 `json:"distance_function,omitempty"`
}

// ChromaVector represents a vector in Chroma
type ChromaVector struct {
	ID        string                 `json:"id"`
	Embedding []float32              `json:"embedding"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Document  string                 `json:"document,omitempty"`
}

// ChromaQueryRequest represents a query request to Chroma
type ChromaQueryRequest struct {
	QueryEmbeddings [][]float32            `json:"query_embeddings"`
	QueryTexts      []string               `json:"query_texts,omitempty"`
	NResults        int                    `json:"n_results"`
	Where           map[string]interface{} `json:"where,omitempty"`
	WhereDocument   map[string]interface{} `json:"where_document,omitempty"`
	Include         []string               `json:"include,omitempty"`
}

// ChromaQueryResult represents a query result from Chroma
type ChromaQueryResult struct {
	IDs        [][]string                 `json:"ids"`
	Embeddings [][][]float32              `json:"embeddings,omitempty"`
	Documents  [][]string                 `json:"documents,omitempty"`
	Metadatas  [][]map[string]interface{} `json:"metadatas,omitempty"`
	Distances  [][]float32                `json:"distances,omitempty"`
}

// ChromaClient represents a client for interacting with Chroma
type ChromaClient struct {
	// API is the underlying chroma-go v2 client
	API chromav2.Client

	// Retain connection params for metadata and fallbacks
	BaseURL     string
	Host        string
	Port        int
	Username    string
	Password    string
	SSL         bool
	IsConnected int32
}
