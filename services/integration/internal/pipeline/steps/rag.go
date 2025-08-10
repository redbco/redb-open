package steps

import (
	"context"
	"fmt"
	"sync"
	"time"

	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
)

type RAGProviderGetter interface {
	GetProvider(ctx context.Context, integrationID string) (RAGProvider, error)
}

type RAGProvider interface {
	Ingest(ctx context.Context, documents []*integrationv1.Document) ([]*integrationv1.IngestResult, error)
}

type RAGProcessor struct {
	RAGManager RAGProviderGetter
	Workers    int
}

func (r *RAGProcessor) Process(ctx context.Context, documents <-chan []*integrationv1.Document, integrationID string) (<-chan []*integrationv1.IngestResult, error) {
	if r.RAGManager == nil {
		return nil, fmt.Errorf("rag manager is nil")
	}
	if r.Workers <= 0 {
		r.Workers = 4
	}
	provider, err := r.RAGManager.GetProvider(ctx, integrationID)
	if err != nil {
		return nil, err
	}
	out := make(chan []*integrationv1.IngestResult, r.Workers)

	go func() {
		defer close(out)
		wg := &sync.WaitGroup{}
		sem := make(chan struct{}, r.Workers)
		for batch := range documents {
			b := batch
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				// basic timeout per batch
				cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()
				res, err := provider.Ingest(cctx, b)
				if err != nil {
					// convert error to results with error_message to preserve alignment
					failed := make([]*integrationv1.IngestResult, 0, len(b))
					for _, d := range b {
						failed = append(failed, &integrationv1.IngestResult{DocumentId: d.GetId(), ErrorMessage: err.Error()})
					}
					out <- failed
					return
				}
				out <- res
			}()
		}
		wg.Wait()
	}()

	return out, nil
}
