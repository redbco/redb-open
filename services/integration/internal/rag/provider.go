package rag

import (
	"context"

	integrationv1 "github.com/redbco/redb-open/api/proto/integration/v1"
)

type ProviderStatus struct {
	Healthy bool
	Message string
}

type RAGProvider interface {
	Ingest(ctx context.Context, documents []*integrationv1.Document) ([]*integrationv1.IngestResult, error)
	Query(ctx context.Context, query string, params map[string]any) (*integrationv1.QueryResult, error)
	GetStatus() ProviderStatus
	Close() error
}
