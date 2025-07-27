package models

// PineconeSchema represents the schema of a Pinecone vector database
type PineconeSchema struct {
	SchemaType  string               `json:"schemaType"`
	Indexes     []PineconeIndex      `json:"indexes"`
	Collections []PineconeCollection `json:"collections"`
}

// PineconeIndex represents a Pinecone index
type PineconeIndex struct {
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

// PineconeCollection represents a Pinecone collection
type PineconeCollection struct {
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
