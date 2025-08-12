package models

// ChromaSchema represents the schema of a Chroma vector database
type ChromaSchema struct {
	SchemaType  string             `json:"schemaType"`
	Collections []ChromaCollection `json:"collections"`
}

// ChromaCollection represents a Chroma collection (similar to a table of vectors)
type ChromaCollection struct {
	Name              string                 `json:"name"`
	ID                string                 `json:"id"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	Count             int64                  `json:"count"`
	Size              int64                  `json:"size"`
	EmbeddingFunction string                 `json:"embedding_function,omitempty"`
	Dimension         int                    `json:"dimension,omitempty"`
	DistanceFunction  string                 `json:"distance_function,omitempty"`
}
