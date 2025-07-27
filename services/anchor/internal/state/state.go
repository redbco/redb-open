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
	DatabaseManager  *dbmanager.DatabaseManager
	ConfigRepository *config.Repository
	db               *database.PostgreSQL
	nodeID           string
	mu               sync.RWMutex
}

var (
	instance *GlobalState
	once     sync.Once
)

// GetInstance returns the singleton instance of GlobalState
func GetInstance() *GlobalState {
	once.Do(func() {
		instance = &GlobalState{
			DatabaseManager: dbmanager.NewDatabaseManager(),
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

// GetDatabaseManager returns the database manager instance
func (gs *GlobalState) GetDatabaseManager() *dbmanager.DatabaseManager {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.DatabaseManager
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

// SetLogger sets the logger for the DatabaseManager
func (gs *GlobalState) SetLogger(logger *logger.Logger) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if gs.DatabaseManager != nil {
		gs.DatabaseManager.SetLogger(logger)
	}
}

// SetDB sets the internal PostgreSQL database instance
func (gs *GlobalState) SetDB(db *database.PostgreSQL) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.db = db
}
