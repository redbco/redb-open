package stores

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redis/go-redis/v9"
)

// RedisSnapshotStore implements the SnapshotStore interface using Redis
type RedisSnapshotStore struct {
	client *redis.Client
	logger *logger.Logger
	mu     sync.RWMutex
}

// RedisSnapshot represents a snapshot stored in Redis
type RedisSnapshot struct {
	ID        string                 `json:"id"`
	Index     uint64                 `json:"index"`
	Term      uint64                 `json:"term"`
	Data      []byte                 `json:"data"`
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt time.Time              `json:"created_at"`
}

// NewRedisSnapshotStore creates a new Redis-based snapshot store
func NewRedisSnapshotStore(redisDB *database.Redis, logger *logger.Logger) (*RedisSnapshotStore, error) {
	if redisDB == nil {
		return nil, fmt.Errorf("redis connection is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	client := redisDB.Client()
	if client == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	store := &RedisSnapshotStore{
		client: client,
		logger: logger,
	}

	return store, nil
}

// Create creates a new snapshot
func (s *RedisSnapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate a unique snapshot ID
	snapshotID := fmt.Sprintf("snapshot_%d_%d_%d", index, term, time.Now().UnixNano())

	// Create the snapshot sink
	sink := &RedisSnapshotSink{
		store:       s,
		snapshotID:  snapshotID,
		index:       index,
		term:        term,
		version:     version,
		config:      configuration,
		configIndex: configurationIndex,
		trans:       trans,
		data:        make([]byte, 0),
		metadata:    make(map[string]interface{}),
		createdAt:   time.Now(),
	}

	return sink, nil
}

// List lists all snapshots
func (s *RedisSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ctx := context.Background()
	pattern := "raft:snapshot:*"

	keys, err := s.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshot keys: %v", err)
	}

	var snapshots []*raft.SnapshotMeta

	for _, key := range keys {
		data, err := s.client.Get(ctx, key).Result()
		if err != nil {
			s.logger.Warn("Failed to get snapshot data for key %s: %v", key, err)
			continue
		}

		var snapshot RedisSnapshot
		if err := json.Unmarshal([]byte(data), &snapshot); err != nil {
			s.logger.Warn("Failed to unmarshal snapshot data for key %s: %v", key, err)
			continue
		}

		meta := &raft.SnapshotMeta{
			ID:                 snapshot.ID,
			Index:              snapshot.Index,
			Term:               snapshot.Term,
			Version:            raft.SnapshotVersion(1), // Default version
			Configuration:      raft.Configuration{},    // Would need to be stored separately
			ConfigurationIndex: 0,                       // Would need to be stored separately
		}

		snapshots = append(snapshots, meta)
	}

	return snapshots, nil
}

// Open opens a snapshot by ID
func (s *RedisSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ctx := context.Background()
	key := fmt.Sprintf("raft:snapshot:%s", id)

	data, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil, fmt.Errorf("snapshot not found: %s", id)
		}
		return nil, nil, fmt.Errorf("failed to get snapshot %s: %v", id, err)
	}

	var snapshot RedisSnapshot
	if err := json.Unmarshal([]byte(data), &snapshot); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal snapshot data: %v", err)
	}

	meta := &raft.SnapshotMeta{
		ID:                 snapshot.ID,
		Index:              snapshot.Index,
		Term:               snapshot.Term,
		Version:            raft.SnapshotVersion(1), // Default version
		Configuration:      raft.Configuration{},    // Would need to be stored separately
		ConfigurationIndex: 0,                       // Would need to be stored separately
	}

	reader := io.NopCloser(bytes.NewReader(snapshot.Data))

	return meta, reader, nil
}

// RedisSnapshotSink implements the SnapshotSink interface
type RedisSnapshotSink struct {
	store       *RedisSnapshotStore
	snapshotID  string
	index       uint64
	term        uint64
	version     raft.SnapshotVersion
	config      raft.Configuration
	configIndex uint64
	trans       raft.Transport
	data        []byte
	metadata    map[string]interface{}
	createdAt   time.Time
	written     bool
	canceled    bool
}

// Write writes data to the snapshot
func (s *RedisSnapshotSink) Write(p []byte) (n int, err error) {
	if s.canceled {
		return 0, fmt.Errorf("snapshot sink is canceled")
	}

	s.data = append(s.data, p...)
	return len(p), nil
}

// Close closes the snapshot sink and saves the snapshot
func (s *RedisSnapshotSink) Close() error {
	if s.canceled {
		return nil
	}

	s.written = true

	// Create the snapshot object
	snapshot := RedisSnapshot{
		ID:        s.snapshotID,
		Index:     s.index,
		Term:      s.term,
		Data:      s.data,
		Metadata:  s.metadata,
		CreatedAt: s.createdAt,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %v", err)
	}

	// Store in Redis
	ctx := context.Background()
	key := fmt.Sprintf("raft:snapshot:%s", s.snapshotID)

	err = s.store.client.Set(ctx, key, data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to store snapshot in Redis: %v", err)
	}

	s.store.logger.Info("Snapshot saved to Redis: (id: %s, index: %d, term: %d)", s.snapshotID, s.index, s.term)
	return nil
}

// Cancel cancels the snapshot creation
func (s *RedisSnapshotSink) Cancel() error {
	s.canceled = true
	return nil
}

// ID returns the snapshot ID
func (s *RedisSnapshotSink) ID() string {
	return s.snapshotID
}
