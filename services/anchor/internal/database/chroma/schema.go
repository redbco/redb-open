package chroma

import (
	"context"
	"fmt"
	"time"

	chromav2 "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DiscoverSchema discovers the Chroma database schema and returns a UnifiedModel
func DiscoverSchema(client *ChromaClient) (*unifiedmodel.UnifiedModel, error) {
	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Chroma,
		VectorIndexes: make(map[string]unifiedmodel.VectorIndex),
		Collections:   make(map[string]unifiedmodel.Collection),
		Vectors:       make(map[string]unifiedmodel.Vector),
		Embeddings:    make(map[string]unifiedmodel.Embedding),
	}

	// Get all collections
	collections, err := listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	// Get details for each collection and convert to unified model
	for _, collectionName := range collections {
		details, err := describeCollection(client, collectionName)
		if err != nil {
			continue // Skip collections we can't describe
		}

		// Convert to Embedding (primary container for vector databases)
		embedding := unifiedmodel.Embedding{
			Name:  details.Name,
			Model: "chroma", // Default model name
			Options: map[string]any{
				"id":       details.ID,
				"metadata": details.Metadata,
			},
		}
		um.Embeddings[details.Name] = embedding

		// Keep VectorIndex for compatibility
		vectorIndex := ConvertChromaCollection(*details)
		um.VectorIndexes[details.Name] = vectorIndex

		// Also create a collection entry for compatibility
		um.Collections[details.Name] = unifiedmodel.Collection{
			Name: details.Name,
		}
	}

	return um, nil
}

// CreateStructure creates database structures from a UnifiedModel
func CreateStructure(client *ChromaClient, um *unifiedmodel.UnifiedModel) error {
	if um == nil {
		return fmt.Errorf("unified model cannot be nil")
	}

	// Create collections from UnifiedModel
	for _, collection := range um.Collections {
		if err := createCollectionFromUnified(client, collection); err != nil {
			return fmt.Errorf("error creating collection %s: %v", collection.Name, err)
		}
	}

	// Create vector indexes from UnifiedModel
	for _, vectorIndex := range um.VectorIndexes {
		if err := createVectorIndexFromUnified(client, vectorIndex); err != nil {
			return fmt.Errorf("error creating vector index %s: %v", vectorIndex.Name, err)
		}
	}

	return nil
}

// CreateCollection creates a new collection in Chroma
func CreateCollection(client *ChromaClient, collectionName string, metadata map[string]interface{}) error {
	if collectionName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use chroma-go GetOrCreate with metadata
	_, err := client.API.GetOrCreateCollection(ctx, collectionName,
		chromav2.WithCollectionMetadataCreate(chromav2.NewMetadataFromMap(metadata)),
	)
	return err
}

// DropCollection deletes a collection from Chroma
func DropCollection(client *ChromaClient, collectionName string) error {
	if collectionName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.API.DeleteCollection(ctx, collectionName)
}

// createCollectionFromUnified creates a collection from UnifiedModel Collection
func createCollectionFromUnified(client *ChromaClient, collection unifiedmodel.Collection) error {
	if collection.Name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Extract metadata from collection options if available
	metadata := make(map[string]interface{})
	if collection.Options != nil {
		if meta, ok := collection.Options["metadata"].(map[string]interface{}); ok {
			metadata = meta
		}
	}

	// Use chroma-go GetOrCreate with metadata
	_, err := client.API.GetOrCreateCollection(ctx, collection.Name,
		chromav2.WithCollectionMetadataCreate(chromav2.NewMetadataFromMap(metadata)),
	)
	return err
}

// createVectorIndexFromUnified creates a vector index from UnifiedModel VectorIndex
func createVectorIndexFromUnified(client *ChromaClient, vectorIndex unifiedmodel.VectorIndex) error {
	if vectorIndex.Name == "" {
		return fmt.Errorf("vector index name cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// In Chroma, vector indexes are essentially collections with vector capabilities
	// Extract metadata from vector index parameters if available
	metadata := make(map[string]interface{})
	if vectorIndex.Parameters != nil {
		if meta, ok := vectorIndex.Parameters["metadata"].(map[string]interface{}); ok {
			metadata = meta
		}
	}

	// Add vector-specific metadata
	if vectorIndex.Dimension > 0 {
		metadata["dimension"] = vectorIndex.Dimension
	}
	if vectorIndex.Metric != "" {
		metadata["metric"] = vectorIndex.Metric
	}

	// Use chroma-go GetOrCreate with metadata
	_, err := client.API.GetOrCreateCollection(ctx, vectorIndex.Name,
		chromav2.WithCollectionMetadataCreate(chromav2.NewMetadataFromMap(metadata)),
	)
	return err
}
