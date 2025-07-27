package mesh

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mesh/internal/storage"
)

// CommandType represents the type of command being executed
type CommandType string

const (
	SetStateCmd    CommandType = "set_state"
	DeleteStateCmd CommandType = "delete_state"
	UpdateRouteCmd CommandType = "update_route"
	DeleteRouteCmd CommandType = "delete_route"
)

// Command represents a command to be executed by the state machine
type Command struct {
	Type    CommandType `json:"type"`
	Key     string      `json:"key"`
	Value   interface{} `json:"value"`
	Version uint64      `json:"version"`
}

// StateMachine represents the state machine for the consensus protocol
type StateMachine struct {
	store    storage.Interface
	logger   *logger.Logger
	state    map[string]interface{}
	versions map[string]uint64
	mu       sync.RWMutex
}

// NewStateMachine creates a new state machine
func NewStateMachine(store storage.Interface, logger *logger.Logger) *StateMachine {
	return &StateMachine{
		store:    store,
		logger:   logger,
		state:    make(map[string]interface{}),
		versions: make(map[string]uint64),
	}
}

// Apply applies a command to the state machine
func (sm *StateMachine) Apply(cmd Command) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check version
	currentVersion, exists := sm.versions[cmd.Key]
	if exists && cmd.Version <= currentVersion {
		return fmt.Errorf("command version %d is not greater than current version %d", cmd.Version, currentVersion)
	}

	// Apply command
	switch cmd.Type {
	case SetStateCmd:
		if err := sm.applySetState(cmd); err != nil {
			return fmt.Errorf("failed to apply set state command: %v", err)
		}
	case DeleteStateCmd:
		if err := sm.applyDeleteState(cmd); err != nil {
			return fmt.Errorf("failed to apply delete state command: %v", err)
		}
	case UpdateRouteCmd:
		if err := sm.applyUpdateRoute(cmd); err != nil {
			return fmt.Errorf("failed to apply update route command: %v", err)
		}
	case DeleteRouteCmd:
		if err := sm.applyDeleteRoute(cmd); err != nil {
			return fmt.Errorf("failed to apply delete route command: %v", err)
		}
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	// Update version
	sm.versions[cmd.Key] = cmd.Version

	// Persist state
	if err := sm.persistState(cmd.Key); err != nil {
		return fmt.Errorf("failed to persist state: %v", err)
	}

	return nil
}

// GetState returns the current state for a key
func (sm *StateMachine) GetState(key string) (interface{}, uint64, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	value, exists := sm.state[key]
	if !exists {
		return nil, 0, fmt.Errorf("key not found: %s", key)
	}

	version := sm.versions[key]
	return value, version, nil
}

// applySetState applies a set state command
func (sm *StateMachine) applySetState(cmd Command) error {
	sm.state[cmd.Key] = cmd.Value
	return nil
}

// applyDeleteState applies a delete state command
func (sm *StateMachine) applyDeleteState(cmd Command) error {
	delete(sm.state, cmd.Key)
	delete(sm.versions, cmd.Key)
	return nil
}

// applyUpdateRoute applies an update route command
func (sm *StateMachine) applyUpdateRoute(cmd Command) error {
	route, ok := cmd.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid route value type")
	}

	// Validate route
	if err := sm.validateRoute(route); err != nil {
		return fmt.Errorf("invalid route: %v", err)
	}

	sm.state[cmd.Key] = route
	return nil
}

// applyDeleteRoute applies a delete route command
func (sm *StateMachine) applyDeleteRoute(cmd Command) error {
	delete(sm.state, cmd.Key)
	delete(sm.versions, cmd.Key)
	return nil
}

// validateRoute validates a route
func (sm *StateMachine) validateRoute(route map[string]interface{}) error {
	// Check required fields
	required := []string{"destination", "next_hop", "cost"}
	for _, field := range required {
		if _, exists := route[field]; !exists {
			return fmt.Errorf("missing required field: %s", field)
		}
	}

	// Validate cost
	cost, ok := route["cost"].(float64)
	if !ok || cost < 0 {
		return fmt.Errorf("invalid cost value")
	}

	return nil
}

// persistState persists the state to storage
func (sm *StateMachine) persistState(key string) error {
	value := sm.state[key]
	version := sm.versions[key]

	// Marshal value
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %v", err)
	}

	// Store in database
	if err := sm.store.StoreState(context.Background(), key, data); err != nil {
		return fmt.Errorf("failed to store state: %v", err)
	}

	// Store version
	versionData, err := json.Marshal(version)
	if err != nil {
		return fmt.Errorf("failed to marshal version: %v", err)
	}

	if err := sm.store.StoreState(context.Background(), key+":version", versionData); err != nil {
		return fmt.Errorf("failed to store version: %v", err)
	}

	return nil
}

// LoadState loads the state from storage
func (sm *StateMachine) LoadState() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// TODO: Implement state loading from storage
	// This would involve:
	// 1. Loading all state keys from storage
	// 2. Loading versions for each key
	// 3. Reconstructing the state map

	return nil
}

// Snapshot creates a snapshot of the current state
func (sm *StateMachine) Snapshot() (map[string]interface{}, map[string]uint64, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Create copies of state and versions
	stateCopy := make(map[string]interface{}, len(sm.state))
	versionsCopy := make(map[string]uint64, len(sm.versions))

	for k, v := range sm.state {
		stateCopy[k] = v
	}
	for k, v := range sm.versions {
		versionsCopy[k] = v
	}

	return stateCopy, versionsCopy, nil
}

// Restore restores the state from a snapshot
func (sm *StateMachine) Restore(state map[string]interface{}, versions map[string]uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Clear current state
	sm.state = make(map[string]interface{})
	sm.versions = make(map[string]uint64)

	// Restore state and versions
	for k, v := range state {
		sm.state[k] = v
	}
	for k, v := range versions {
		sm.versions[k] = v
	}

	// Persist restored state
	for k := range state {
		if err := sm.persistState(k); err != nil {
			return fmt.Errorf("failed to persist restored state: %v", err)
		}
	}

	return nil
}
