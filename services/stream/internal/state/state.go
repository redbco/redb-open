package state

import (
	"sync"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/stream/adapter"
	"github.com/redbco/redb-open/services/stream/internal/config"
)

// State holds the global state for the stream service
type State struct {
	mu               sync.RWMutex
	configRepository *config.Repository
	db               *database.PostgreSQL
	nodeID           string
	logger           *logger.Logger
	connections      map[string]adapter.Connection // streamID -> connection
}

var (
	instance *State
	once     sync.Once
)

// GetInstance returns the singleton instance of the global state
func GetInstance() *State {
	once.Do(func() {
		instance = &State{
			connections: make(map[string]adapter.Connection),
		}
	})
	return instance
}

// Initialize initializes the state with the config repository and node ID
func (s *State) Initialize(configRepo *config.Repository, nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configRepository = configRepo
	s.nodeID = nodeID
}

// SetDB sets the database connection
func (s *State) SetDB(db *database.PostgreSQL) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db = db
}

// GetDB returns the database connection
func (s *State) GetDB() *database.PostgreSQL {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db
}

// SetLogger sets the logger
func (s *State) SetLogger(logger *logger.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger = logger
}

// GetLogger returns the logger
func (s *State) GetLogger() *logger.Logger {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logger
}

// GetConfigRepository returns the config repository
func (s *State) GetConfigRepository() *config.Repository {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configRepository
}

// GetNodeID returns the node ID
func (s *State) GetNodeID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nodeID
}

// AddConnection adds a connection to the state
func (s *State) AddConnection(streamID string, conn adapter.Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[streamID] = conn
}

// RemoveConnection removes a connection from the state
func (s *State) RemoveConnection(streamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connections, streamID)
}

// GetConnection retrieves a connection from the state
func (s *State) GetConnection(streamID string) (adapter.Connection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	conn, ok := s.connections[streamID]
	return conn, ok
}

// GetAllStreamIDs returns all stream IDs
func (s *State) GetAllStreamIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	streamIDs := make([]string, 0, len(s.connections))
	for id := range s.connections {
		streamIDs = append(streamIDs, id)
	}
	return streamIDs
}
