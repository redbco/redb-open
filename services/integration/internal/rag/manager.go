package rag

import (
	"context"
	"fmt"
	"sync"
)

// ProviderCache is a trivial cache for instantiated providers
type ProviderCache struct {
	mu        sync.RWMutex
	providers map[string]RAGProvider
}

func newCache() *ProviderCache { return &ProviderCache{providers: map[string]RAGProvider{}} }

type RAGProviderManager struct {
	providers map[string]RAGProvider
	cache     *ProviderCache
	factory   ProviderFactory
}

type ProviderFactory interface {
	Create(ctx context.Context, integrationID string) (RAGProvider, error)
}

func NewManager(factory ProviderFactory) *RAGProviderManager {
	return &RAGProviderManager{providers: map[string]RAGProvider{}, cache: newCache(), factory: factory}
}

func (m *RAGProviderManager) GetProvider(ctx context.Context, integrationID string) (RAGProvider, error) {
	// Check cache
	m.cache.mu.RLock()
	if p, ok := m.cache.providers[integrationID]; ok {
		m.cache.mu.RUnlock()
		return p, nil
	}
	m.cache.mu.RUnlock()
	// Create via factory
	if m.factory == nil {
		return nil, fmt.Errorf("no provider factory configured")
	}
	p, err := m.factory.Create(ctx, integrationID)
	if err != nil {
		return nil, err
	}
	m.cache.mu.Lock()
	m.cache.providers[integrationID] = p
	m.cache.mu.Unlock()
	return p, nil
}
