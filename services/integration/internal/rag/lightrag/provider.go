package lightrag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
	"github.com/redbco/redb-open/services/integration/internal/rag"
	"google.golang.org/protobuf/types/known/structpb"
)

type LightRAGConfig struct {
	BaseURL        string
	APIKey         string
	TimeoutSeconds int
}

type LightRAGProvider struct {
	client  *http.Client
	config  LightRAGConfig
	baseURL string
}

func New(config LightRAGConfig) *LightRAGProvider {
	timeout := time.Duration(config.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &LightRAGProvider{
		client:  &http.Client{Timeout: timeout},
		config:  config,
		baseURL: config.BaseURL,
	}
}

func (p *LightRAGProvider) Ingest(ctx context.Context, documents []*integrationv1.Document) ([]*integrationv1.IngestResult, error) {
	payload := map[string]any{
		"documents": transformDocs(documents),
	}
	buf, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/ingest", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.config.APIKey)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("lightrag ingest error: status %d", resp.StatusCode)
	}
	var out struct {
		Results []struct {
			DocumentID string   `json:"document_id"`
			ChunkIDs   []string `json:"chunk_ids"`
			Embeddings []struct {
				ID      string         `json:"id"`
				Vector  []float64      `json:"vector"`
				Content string         `json:"content"`
				Model   string         `json:"model"`
				Meta    map[string]any `json:"metadata"`
			} `json:"embeddings"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	results := make([]*integrationv1.IngestResult, 0, len(out.Results))
	for _, r := range out.Results {
		convEmb := make([]*integrationv1.Embedding, 0, len(r.Embeddings))
		for _, e := range r.Embeddings {
			m, _ := toStruct(e.Meta)
			convEmb = append(convEmb, &integrationv1.Embedding{
				Id:       e.ID,
				Vector:   e.Vector,
				Content:  e.Content,
				Metadata: m,
				Model:    e.Model,
			})
		}
		results = append(results, &integrationv1.IngestResult{
			DocumentId: r.DocumentID,
			ChunkIds:   r.ChunkIDs,
			Embeddings: convEmb,
		})
	}
	return results, nil
}

func (p *LightRAGProvider) Query(ctx context.Context, query string, params map[string]any) (*integrationv1.QueryResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func (p *LightRAGProvider) GetStatus() rag.ProviderStatus {
	return rag.ProviderStatus{Healthy: true, Message: "ok"}
}
func (p *LightRAGProvider) Close() error { return nil }

func transformDocs(docs []*integrationv1.Document) []map[string]any {
	out := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		out = append(out, map[string]any{
			"id":       d.GetId(),
			"content":  d.GetContent(),
			"metadata": d.GetMetadata().AsMap(),
		})
	}
	return out
}

func toStruct(m map[string]any) (*structpb.Struct, error) {
	if m == nil {
		return structpb.NewStruct(map[string]any{})
	}
	return structpb.NewStruct(m)
}
