package steps

import (
	"context"
	"encoding/json"
	"fmt"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
)

type TargetStorage struct {
	AnchorClient anchorv1.AnchorServiceClient
	BatchSize    int
}

type StorageResult struct {
	ProcessedDocuments int32
	StoredEmbeddings   int32
	FailedDocuments    int32
}

func (t *TargetStorage) Store(ctx context.Context, results <-chan []*integrationv1.IngestResult, cfg *integrationv1.TargetConfiguration) (*StorageResult, error) {
	if t.AnchorClient == nil {
		return nil, fmt.Errorf("anchor client is nil")
	}
	if t.BatchSize <= 0 {
		t.BatchSize = 100
	}
	summary := &StorageResult{}
	// For MVP, store via InsertDataStream into a target table/collection
	// Convert results into generic rows {document_id, embedding(model,vector), metadata}
	// We assume cfg.collection_name is the target table name
	batch := make([]map[string]any, 0, t.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		payload, _ := json.Marshal(batch)
		req := &anchorv1.InsertDataRequest{
			TenantId:    "",
			WorkspaceId: "",
			DatabaseId:  cfg.GetDatabaseId(),
			TableName:   cfg.GetCollectionName(),
			Data:        payload,
		}
		_, err := t.AnchorClient.InsertData(ctx, req)
		batch = batch[:0]
		return err
	}

	for r := range results {
		for _, item := range r {
			summary.ProcessedDocuments++
			if item.GetErrorMessage() != "" {
				summary.FailedDocuments++
				continue
			}
			for _, emb := range item.GetEmbeddings() {
				summary.StoredEmbeddings++
				row := map[string]any{
					"document_id": item.GetDocumentId(),
					"embedding": map[string]any{
						"model":   emb.GetModel(),
						"vector":  emb.GetVector(),
						"content": emb.GetContent(),
					},
					"metadata": emb.GetMetadata().AsMap(),
				}
				batch = append(batch, row)
				if len(batch) >= t.BatchSize {
					if err := flush(); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return summary, nil
}
