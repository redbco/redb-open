package chroma

import (
	"context"
	"fmt"
	"time"

	chromav2 "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/amikos-tech/chroma-go/pkg/embeddings"
)

func toDocumentIDs(ids []string) []chromav2.DocumentID {
	out := make([]chromav2.DocumentID, 0, len(ids))
	for _, id := range ids {
		out = append(out, chromav2.DocumentID(id))
	}
	return out
}

// FetchData retrieves vectors from a specified collection
func FetchData(client *ChromaClient, collectionName string, limit int) ([]map[string]interface{}, error) {
	if collectionName == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}

	// Use API client to fetch from collection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	col, err := client.API.GetCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	// Chroma-go Get supports options for include and limit
	include := []string{"embeddings", "metadatas", "documents"}
	// Build include options using typed Include constants
	var includeOpts []chromav2.Include
	for _, inc := range include {
		includeOpts = append(includeOpts, chromav2.Include(inc))
	}
	getOpts := []chromav2.CollectionGetOption{
		chromav2.WithIncludeGet(includeOpts...),
	}
	if limit > 0 {
		getOpts = append(getOpts, chromav2.WithLimitGet(limit))
	}
	res, err := col.Get(ctx, getOpts...)
	if err != nil {
		return nil, err
	}

	ids := res.GetIDs()
	embeddings := res.GetEmbeddings()
	documents := res.GetDocuments()

	result := make([]map[string]interface{}, 0, len(ids))
	for i, id := range ids {
		item := map[string]interface{}{"id": string(id)}
		if i < len(embeddings) && embeddings[i] != nil && embeddings[i].IsDefined() {
			item["embedding"] = embeddings[i].ContentAsFloat32()
		}
		if i < len(documents) && documents[i] != nil && documents[i].ContentString() != "" {
			item["document"] = documents[i].ContentString()
		}
		result = append(result, item)
	}

	return result, nil
}

// InsertData inserts vectors into a specified collection
func InsertData(client *ChromaClient, collectionName string, data []map[string]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if collectionName == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	// Prepare vectors for insertion
	var ids []string
	var embeddingsData [][]float32
	var metadatas []map[string]interface{}
	var documents []string

	for _, item := range data {
		// Extract ID
		id, ok := item["id"].(string)
		if !ok {
			return 0, fmt.Errorf("each vector must have an 'id' field")
		}
		ids = append(ids, id)

		// Extract embedding
		if embedding, ok := item["embedding"].([]float32); ok {
			embeddingsData = append(embeddingsData, embedding)
		} else {
			return 0, fmt.Errorf("each vector must have an 'embedding' field")
		}

		// Extract metadata (optional)
		metadata := make(map[string]interface{})
		for k, v := range item {
			if k != "id" && k != "embedding" && k != "document" {
				metadata[k] = v
			}
		}
		if len(metadata) > 0 {
			metadatas = append(metadatas, metadata)
		} else {
			metadatas = append(metadatas, nil)
		}

		// Extract document (optional)
		if document, ok := item["document"].(string); ok {
			documents = append(documents, document)
		} else {
			documents = append(documents, "")
		}
	}

	// Use client to get collection and add records
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	col, err := client.API.GetCollection(ctx, collectionName)
	if err != nil {
		return 0, err
	}

	// Add supports ids, embeddings, metadatas, documents
	// Convert embeddings [][]float32 to embeddings.Embeddings
	embObjs, err := embeddings.NewEmbeddingsFromFloat32(embeddingsData)
	if err != nil {
		return 0, err
	}
	// Convert metadatas to chroma-go DocumentMetadata
	docMetas := make([]chromav2.DocumentMetadata, 0, len(metadatas))
	for _, m := range metadatas {
		dm, err := chromav2.NewDocumentMetadataFromMap(m)
		if err != nil {
			return 0, err
		}
		docMetas = append(docMetas, dm)
	}
	// Prepare add options
	addOpts := []chromav2.CollectionAddOption{
		chromav2.WithIDs(toDocumentIDs(ids)...),
		chromav2.WithEmbeddings(embObjs...),
	}
	if len(docMetas) > 0 {
		addOpts = append(addOpts, chromav2.WithMetadatas(docMetas...))
	}
	if len(documents) > 0 {
		addOpts = append(addOpts, chromav2.WithTexts(documents...))
	}
	err = col.Add(ctx, addOpts...)
	if err != nil {
		return 0, err
	}

	return int64(len(ids)), nil
}

// UpdateData updates existing vectors in a specified collection
func UpdateData(client *ChromaClient, collectionName string, data []map[string]interface{}, whereColumns []string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	if collectionName == "" {
		return 0, fmt.Errorf("collection name cannot be empty")
	}

	// Chroma client: delete then add
	// First, extract IDs to delete
	var idsToDelete []string
	for _, item := range data {
		if id, ok := item["id"].(string); ok {
			idsToDelete = append(idsToDelete, id)
		}
	}

	// Delete existing vectors
	if len(idsToDelete) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		col, err := client.API.GetCollection(ctx, collectionName)
		if err != nil {
			return 0, err
		}
		if err := col.Delete(ctx, chromav2.WithIDsDelete(toDocumentIDs(idsToDelete)...)); err != nil {
			return 0, err
		}
	}

	// Insert the updated vectors
	return InsertData(client, collectionName, data)
}

// UpsertData inserts or updates vectors based on unique constraints
func UpsertData(client *ChromaClient, collectionName string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
	// For Chroma, upsert is the same as insert since it will overwrite existing IDs
	return InsertData(client, collectionName, data)
}

// WipeDatabase removes all data from the Chroma database
func WipeDatabase(client *ChromaClient) error {
	// Get all collections
	collections, err := listCollections(client)
	if err != nil {
		return fmt.Errorf("error listing collections: %v", err)
	}

	// Delete each collection
	for _, collectionName := range collections {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := client.API.DeleteCollection(ctx, collectionName); err != nil {
			return fmt.Errorf("error deleting collection %s: %w", collectionName, err)
		}
	}

	return nil
}
