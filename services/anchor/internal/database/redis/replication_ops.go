package redis

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redis/go-redis/v9"
)

// ReplicationOps implements adapter.ReplicationOperator for Redis using keyspace notifications.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Redis supports CDC-like functionality through keyspace notifications
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"keyspace_notifications", "pubsub"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if keyspace notifications are enabled
	result, err := r.conn.client.Do(ctx, "CONFIG", "GET", "notify-keyspace-events").Result()
	if err != nil {
		return adapter.WrapError(dbcapabilities.Redis, "check_replication_prerequisites", err)
	}

	currentConfig := ""
	if resultSlice, ok := result.([]interface{}); ok && len(resultSlice) > 1 {
		if configVal, ok := resultSlice[1].(string); ok {
			currentConfig = configVal
		}
	}

	// Check if at least basic keyspace events are enabled
	if currentConfig == "" || currentConfig == " " {
		return adapter.NewDatabaseError(
			dbcapabilities.Redis,
			"check_replication_prerequisites",
			adapter.ErrConfigurationError,
		).WithContext("error", "keyspace notifications not enabled, required: notify-keyspace-events=KEA")
	}

	return nil
}

// Connect creates a new replication connection using Redis keyspace notifications.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Enable keyspace notifications if not already enabled
	result, err := r.conn.client.Do(ctx, "CONFIG", "GET", "notify-keyspace-events").Result()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "connect_replication", err)
	}

	currentConfig := ""
	if resultSlice, ok := result.([]interface{}); ok && len(resultSlice) > 1 {
		if configVal, ok := resultSlice[1].(string); ok {
			currentConfig = configVal
		}
	}

	// We need at least "KEA" for keyspace events (Keys, Events, All commands)
	requiredConfig := "KEA"
	newConfig := currentConfig

	// Check if all required flags are present
	for _, c := range requiredConfig {
		if !strings.ContainsRune(currentConfig, c) {
			newConfig += string(c)
		}
	}

	// Update config if needed
	if newConfig != currentConfig {
		_, err = r.conn.client.Do(ctx, "CONFIG", "SET", "notify-keyspace-events", newConfig).Result()
		if err != nil {
			return nil, adapter.WrapError(dbcapabilities.Redis, "connect_replication", err)
		}
	}

	// Determine key pattern (use table names as key patterns)
	keyPattern := "*" // Default to all keys
	if len(config.TableNames) > 0 {
		// Use first table name as pattern, or combine if multiple
		keyPattern = config.TableNames[0]
	}

	// Create the replication source
	source := &RedisReplicationSource{
		id:             config.ReplicationID,
		databaseID:     config.DatabaseID,
		client:         r.conn.client,
		config:         config,
		keyPattern:     keyPattern,
		active:         0,
		stopChan:       make(chan struct{}),
		previousValues: make(map[string]interface{}),
	}

	// Wrap the event handler to match the expected signature
	if config.EventHandler != nil {
		source.eventHandler = func(event map[string]interface{}) error {
			config.EventHandler(event)
			return nil
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	// Get keyspace notification config
	result, err := r.conn.client.Do(ctx, "CONFIG", "GET", "notify-keyspace-events").Result()
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Redis, "get_replication_status", err)
	}

	status := map[string]interface{}{
		"database_id": r.conn.id,
		"status":      "active",
	}

	if resultSlice, ok := result.([]interface{}); ok && len(resultSlice) > 1 {
		if configVal, ok := resultSlice[1].(string); ok {
			status["notify_keyspace_events"] = configVal
		}
	}

	return status, nil
}

// GetLag returns the replication lag (not applicable for Redis pub/sub).
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"lag":         0, // Real-time pub/sub has negligible lag
	}, nil
}

// ListSlots is not applicable for Redis keyspace notifications.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "list replication slots", "not applicable for Redis keyspace notifications")
}

// DropSlot is not applicable for Redis keyspace notifications.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "drop replication slot", "not applicable for Redis keyspace notifications")
}

// ListPublications is not applicable for Redis keyspace notifications.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "list publications", "not applicable for Redis keyspace notifications")
}

// DropPublication is not applicable for Redis keyspace notifications.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Redis, "drop publication", "not applicable for Redis keyspace notifications")
}

// RedisReplicationSource implements adapter.ReplicationSource for Redis keyspace notifications.
type RedisReplicationSource struct {
	id             string
	databaseID     string
	client         *redis.Client
	config         adapter.ReplicationConfig
	keyPattern     string
	active         int32
	stopChan       chan struct{}
	wg             sync.WaitGroup
	previousValues map[string]interface{}
	eventHandler   func(event map[string]interface{}) error
	mu             sync.Mutex
	pubsub         *redis.PubSub
}

// GetSourceID returns the replication source ID.
func (s *RedisReplicationSource) GetSourceID() string {
	return s.id
}

// GetDatabaseID returns the database ID.
func (s *RedisReplicationSource) GetDatabaseID() string {
	return s.databaseID
}

// GetStatus returns the replication source status.
func (s *RedisReplicationSource) GetStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"active":       s.IsActive(),
		"key_pattern":  s.keyPattern,
		"tracked_keys": len(s.previousValues),
	}
}

// GetMetadata returns the replication source metadata.
func (s *RedisReplicationSource) GetMetadata() map[string]interface{} {
	return s.config.Options
}

// IsActive returns whether the replication source is active.
func (s *RedisReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&s.active) == 1
}

// Start starts the replication source.
func (s *RedisReplicationSource) Start() error {
	if !atomic.CompareAndSwapInt32(&s.active, 0, 1) {
		return fmt.Errorf("replication source already active")
	}

	// Create pub/sub subscription for keyspace events
	ctx := context.Background()
	s.pubsub = s.client.PSubscribe(ctx, "__keyspace@*__:*")

	s.wg.Add(1)
	go s.listenForKeyspaceEvents()
	return nil
}

// Stop stops the replication source.
func (s *RedisReplicationSource) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.active, 1, 0) {
		return fmt.Errorf("replication source not active")
	}

	close(s.stopChan)

	// Close pub/sub
	if s.pubsub != nil {
		s.pubsub.Close()
	}

	s.wg.Wait()
	return nil
}

// Close closes the replication source.
func (s *RedisReplicationSource) Close() error {
	if s.IsActive() {
		s.Stop()
	}
	return nil
}

// GetPosition returns the current replication position (not applicable for Redis).
func (s *RedisReplicationSource) GetPosition() (string, error) {
	// Redis keyspace notifications don't have a position
	return "", nil
}

// SetPosition sets the starting replication position (not applicable for Redis).
func (s *RedisReplicationSource) SetPosition(position string) error {
	// Redis keyspace notifications don't support position seeking
	return nil
}

// SaveCheckpoint persists the current replication position.
func (s *RedisReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	return s.SetPosition(position)
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (s *RedisReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	// Placeholder for external checkpointing
}

func (s *RedisReplicationSource) listenForKeyspaceEvents() {
	defer s.wg.Done()

	ctx := context.Background()
	ch := s.pubsub.Channel()

	for {
		select {
		case <-s.stopChan:
			return
		case msg, ok := <-ch:
			if !ok {
				// Channel closed, attempt to reconnect
				time.Sleep(1 * time.Second)
				return
			}

			// Extract key from channel pattern: __keyspace@0__:mykey
			parts := strings.Split(msg.Channel, ":")
			if len(parts) < 2 {
				continue
			}

			key := strings.Join(parts[1:], ":")
			operation := msg.Payload

			// Check if key matches our pattern
			if !s.matchesPattern(key) {
				continue
			}

			// Get the current value
			var currentValue interface{}

			if operation != "del" && operation != "expired" {
				keyType, err := s.client.Type(ctx, key).Result()
				if err != nil {
					continue
				}

				// Get value based on type
				switch keyType {
				case "string":
					currentValue, _ = s.client.Get(ctx, key).Result()
				case "list":
					currentValue, _ = s.client.LRange(ctx, key, 0, -1).Result()
				case "set":
					currentValue, _ = s.client.SMembers(ctx, key).Result()
				case "zset":
					currentValue, _ = s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
				case "hash":
					currentValue, _ = s.client.HGetAll(ctx, key).Result()
				case "stream":
					currentValue, _ = s.client.XRange(ctx, key, "-", "+").Result()
				}
			}

			// Determine the operation type
			var eventType string
			var oldValue interface{}

			s.mu.Lock()
			switch operation {
			case "set":
				if _, exists := s.previousValues[key]; exists {
					eventType = "UPDATE"
					oldValue = s.previousValues[key]
				} else {
					eventType = "INSERT"
				}
				s.previousValues[key] = currentValue
			case "del", "expired":
				eventType = "DELETE"
				oldValue = s.previousValues[key]
				delete(s.previousValues, key)
			default:
				// For other operations like lpush, hset, etc.
				if _, exists := s.previousValues[key]; exists {
					eventType = "UPDATE"
					oldValue = s.previousValues[key]
				} else {
					eventType = "INSERT"
				}
				s.previousValues[key] = currentValue
			}
			s.mu.Unlock()

			// Create event data
			event := map[string]interface{}{
				"key":        key,
				"operation":  eventType,
				"data":       currentValue,
				"old_data":   oldValue,
				"command":    operation,
				"table_name": "redis_keys", // Redis doesn't have tables, use generic name
			}

			// Send event to handler
			if s.eventHandler != nil {
				if err := s.eventHandler(event); err != nil {
					// Log error, continue processing
				}
			}
		}
	}
}

func (s *RedisReplicationSource) matchesPattern(key string) bool {
	if s.keyPattern == "*" {
		return true
	}

	// Simple pattern matching
	if strings.HasSuffix(s.keyPattern, "*") {
		prefix := strings.TrimSuffix(s.keyPattern, "*")
		return strings.HasPrefix(key, prefix)
	}

	return key == s.keyPattern
}
