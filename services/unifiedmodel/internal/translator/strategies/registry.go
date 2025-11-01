package strategies

import (
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// StrategyRegistry manages available conversion strategies
type StrategyRegistry struct {
	strategies map[ParadigmPair]ParadigmConversionStrategy
	mu         sync.RWMutex
}

// ParadigmPair represents a source-target paradigm combination
type ParadigmPair struct {
	Source dbcapabilities.DataParadigm
	Target dbcapabilities.DataParadigm
}

// NewStrategyRegistry creates a new strategy registry
func NewStrategyRegistry() *StrategyRegistry {
	return &StrategyRegistry{
		strategies: make(map[ParadigmPair]ParadigmConversionStrategy),
	}
}

// Register adds a strategy to the registry
func (r *StrategyRegistry) Register(strategy ParadigmConversionStrategy) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	pair := ParadigmPair{
		Source: strategy.SourceParadigm(),
		Target: strategy.TargetParadigm(),
	}

	if _, exists := r.strategies[pair]; exists {
		return fmt.Errorf("strategy for %s→%s already registered", pair.Source, pair.Target)
	}

	r.strategies[pair] = strategy
	return nil
}

// GetStrategy retrieves a strategy for the given paradigm pair
func (r *StrategyRegistry) GetStrategy(source, target dbcapabilities.DataParadigm) (ParadigmConversionStrategy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pair := ParadigmPair{Source: source, Target: target}
	strategy, exists := r.strategies[pair]
	if !exists {
		return nil, fmt.Errorf("no strategy found for %s→%s conversion", source, target)
	}

	return strategy, nil
}

// HasStrategy checks if a strategy exists for the given paradigm pair
func (r *StrategyRegistry) HasStrategy(source, target dbcapabilities.DataParadigm) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pair := ParadigmPair{Source: source, Target: target}
	_, exists := r.strategies[pair]
	return exists
}

// ListStrategies returns all registered paradigm pairs
func (r *StrategyRegistry) ListStrategies() []ParadigmPair {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pairs := make([]ParadigmPair, 0, len(r.strategies))
	for pair := range r.strategies {
		pairs = append(pairs, pair)
	}
	return pairs
}

// GetStrategyByName retrieves a strategy by its name
func (r *StrategyRegistry) GetStrategyByName(name string) (ParadigmConversionStrategy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, strategy := range r.strategies {
		if strategy.Name() == name {
			return strategy, nil
		}
	}

	return nil, fmt.Errorf("no strategy found with name: %s", name)
}

// Unregister removes a strategy from the registry
func (r *StrategyRegistry) Unregister(source, target dbcapabilities.DataParadigm) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	pair := ParadigmPair{Source: source, Target: target}
	if _, exists := r.strategies[pair]; !exists {
		return fmt.Errorf("no strategy found for %s→%s", source, target)
	}

	delete(r.strategies, pair)
	return nil
}

// Clear removes all strategies from the registry
func (r *StrategyRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.strategies = make(map[ParadigmPair]ParadigmConversionStrategy)
}

// GlobalRegistry is the default global strategy registry
var GlobalRegistry = NewStrategyRegistry()

// RegisterStrategy is a convenience function to register a strategy in the global registry
func RegisterStrategy(strategy ParadigmConversionStrategy) error {
	return GlobalRegistry.Register(strategy)
}

// GetStrategy is a convenience function to get a strategy from the global registry
func GetStrategy(source, target dbcapabilities.DataParadigm) (ParadigmConversionStrategy, error) {
	return GlobalRegistry.GetStrategy(source, target)
}
