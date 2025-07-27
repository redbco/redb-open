package chroma

// ChromaDetails contains information about a Chroma vector database
type ChromaDetails struct {
	UniqueIdentifier string `json:"uniqueIdentifier"`
	DatabaseType     string `json:"databaseType"`
	DatabaseEdition  string `json:"databaseEdition"`
	Version          string `json:"version"`
	DatabaseSize     int64  `json:"databaseSize"`
	Host             string `json:"host"`
	Port             int    `json:"port"`
	CollectionCount  int64  `json:"collectionCount"`
	TotalVectors     int64  `json:"totalVectors"`
}

// ChromaSchema represents the schema of a Chroma vector database
type ChromaSchema struct {
	Collections []ChromaCollectionInfo `json:"collections"`
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
	BaseURL     string
	Host        string
	Port        int
	Username    string
	Password    string
	SSL         bool
	IsConnected int32 // Add this field for health checks
}
