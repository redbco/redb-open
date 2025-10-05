package state

import (
	"sync"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/config"
	dbmanager "github.com/redbco/redb-open/services/anchor/internal/database"
)

// GlobalState manages the global state of the anchor service
type GlobalState struct {
	// Connection management using adapter pattern
	ConnectionManager  *dbmanager.ConnectionManager
	ConnectionRegistry *dbmanager.ConnectionRegistry
	ConfigRepository   *config.Repository
	db                 *database.PostgreSQL
	nodeID             string
	mu                 sync.RWMutex
}

var (
	instance *GlobalState
	once     sync.Once
)

// GetInstance returns the singleton instance of GlobalState
func GetInstance() *GlobalState {
	once.Do(func() {
		connMgr := dbmanager.NewConnectionManager()
		instance = &GlobalState{
			ConnectionManager:  connMgr,
			ConnectionRegistry: dbmanager.NewConnectionRegistry(connMgr),
		}
	})
	return instance
}

// Initialize sets up the GlobalState with necessary dependencies
func (gs *GlobalState) Initialize(configRepository *config.Repository, nodeID string) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	gs.ConfigRepository = configRepository
	gs.nodeID = nodeID
}

// GetConnectionManager returns the connection manager instance
func (gs *GlobalState) GetConnectionManager() *dbmanager.ConnectionManager {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.ConnectionManager
}

// GetConnectionRegistry returns the connection registry for watchers
func (gs *GlobalState) GetConnectionRegistry() *dbmanager.ConnectionRegistry {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.ConnectionRegistry
}

// GetConfigRepository returns the config repository instance
func (gs *GlobalState) GetConfigRepository() *config.Repository {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.ConfigRepository
}

// GetDB returns the internal PostgreSQL database instance
func (gs *GlobalState) GetDB() *database.PostgreSQL {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.db
}

// GetNodeID returns the node ID
func (gs *GlobalState) GetNodeID() string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.nodeID
}

// SetLogger sets the logger for all managers
func (gs *GlobalState) SetLogger(logger *logger.Logger) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	// Set logger on ConnectionManager
	if gs.ConnectionManager != nil {
		gs.ConnectionManager.SetLogger(logger)
	}

	// Set logger on ConnectionRegistry
	if gs.ConnectionRegistry != nil {
		gs.ConnectionRegistry.SetLogger(logger)
	}
}

// SetDB sets the internal PostgreSQL database instance
func (gs *GlobalState) SetDB(db *database.PostgreSQL) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.db = db
}
