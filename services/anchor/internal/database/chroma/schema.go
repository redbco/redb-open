package chroma

import (
	"context"
	"fmt"
	"time"

	chromav2 "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema discovers the Chroma database schema
func DiscoverSchema(client *ChromaClient) (*ChromaSchema, error) {
	// Get all collections
	collections, err := listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	// Get details for each collection
	collectionDetails := make([]ChromaCollectionInfo, 0, len(collections))
	for _, collectionName := range collections {
		details, err := describeCollection(client, collectionName)
		if err != nil {
			continue // Skip collections we can't describe
		}
		collectionDetails = append(collectionDetails, *details)
	}

	return &ChromaSchema{
		Collections: collectionDetails,
	}, nil
}

// CreateStructure creates database structures based on parameters
func CreateStructure(client *ChromaClient, params common.StructureParams) error {
	// Chroma doesn't support creating traditional database structures
	// Instead, we can create collections if needed
	return fmt.Errorf("structure creation is not supported for Chroma. Use collection creation instead")
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
