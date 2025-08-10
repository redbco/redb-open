package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
	"github.com/redbco/redb-open/services/integration/internal/transform"
)

type SourceExtractor struct {
	AnchorClient anchorv1.AnchorServiceClient
	BatchSize    int
	Transformer  *transform.DataTransformer
}

func (s *SourceExtractor) Extract(ctx context.Context, cfg *integrationv1.SourceConfiguration) (<-chan []*integrationv1.Document, error) {
	if s.AnchorClient == nil {
		return nil, fmt.Errorf("anchor client is nil")
	}
	if s.BatchSize <= 0 {
		s.BatchSize = 100
	}
	out := make(chan []*integrationv1.Document, 4)

	go func() {
		defer close(out)
		// Use streaming API from Anchor to fetch in chunks
		// We map SourceConfiguration.query to table name or command for MVP
		req := &anchorv1.FetchDataStreamRequest{
			TenantId:    "", // optional: could be from metadata in a later iteration
			WorkspaceId: "",
			DatabaseId:  cfg.GetDatabaseId(),
			TableName:   cfg.GetQuery(),
			Options:     nil,
		}
		stream, err := s.AnchorClient.FetchDataStream(ctx, req)
		if err != nil {
			// emit a terminal batch with error embedded? For now, just stop.
			return
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}
			if !resp.GetSuccess() || len(resp.GetData()) == 0 {
				continue
			}
			var rows []map[string]any
			if err := json.Unmarshal(resp.GetData(), &rows); err != nil {
				continue
			}
			// transform rows into Documents with basic batching
			batch := make([]*integrationv1.Document, 0, s.BatchSize)
			for _, row := range rows {
				meta := map[string]any{
					"source_database_id": cfg.GetDatabaseId(),
					"query":              cfg.GetQuery(),
					"fetched_at":         time.Now().UTC().Format(time.RFC3339),
				}
				doc, err := s.Transformer.ToDocument(row, meta)
				if err != nil {
					continue
				}
				batch = append(batch, doc)
				if len(batch) >= s.BatchSize {
					out <- batch
					batch = make([]*integrationv1.Document, 0, s.BatchSize)
				}
			}
			if len(batch) > 0 {
				out <- batch
			}
		}
	}()

	return out, nil
}
