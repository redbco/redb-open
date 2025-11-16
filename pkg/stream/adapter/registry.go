package adapter

import (
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

// AdapterFactory is a function that creates a new adapter instance.
type AdapterFactory func() StreamAdapter

var (
	registry = make(map[streamcapabilities.StreamPlatform]AdapterFactory)
	mu       sync.RWMutex
)

// RegisterAdapter registers an adapter factory for a platform type.
// This should be called from each adapter's init() function.
func RegisterAdapter(platform streamcapabilities.StreamPlatform, factory AdapterFactory) {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := registry[platform]; exists {
		// Allow re-registration during tests, but log in production
		// In production, this might indicate a duplicate registration
	}

	registry[platform] = factory
}

// GetAdapter returns a new adapter instance for the given platform.
// Returns an error if no adapter is registered for the platform.
func GetAdapter(platform streamcapabilities.StreamPlatform) (StreamAdapter, error) {
	mu.RLock()
	defer mu.RUnlock()

	factory, exists := registry[platform]
	if !exists {
		return nil, fmt.Errorf("no adapter registered for platform: %s", platform)
	}

	return factory(), nil
}

// IsRegistered checks if an adapter is registered for the given platform.
func IsRegistered(platform streamcapabilities.StreamPlatform) bool {
	mu.RLock()
	defer mu.RUnlock()

	_, exists := registry[platform]
	return exists
}

// ListRegisteredPlatforms returns all registered platform types.
func ListRegisteredPlatforms() []streamcapabilities.StreamPlatform {
	mu.RLock()
	defer mu.RUnlock()

	platforms := make([]streamcapabilities.StreamPlatform, 0, len(registry))
	for platform := range registry {
		platforms = append(platforms, platform)
	}
	return platforms
}

// UnregisterAdapter removes an adapter registration.
// This is primarily for testing purposes.
func UnregisterAdapter(platform streamcapabilities.StreamPlatform) {
	mu.Lock()
	defer mu.Unlock()

	delete(registry, platform)
}

// ClearRegistry removes all adapter registrations.
// This is primarily for testing purposes.
func ClearRegistry() {
	mu.Lock()
	defer mu.Unlock()

	registry = make(map[streamcapabilities.StreamPlatform]AdapterFactory)
}
