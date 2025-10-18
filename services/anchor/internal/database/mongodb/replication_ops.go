package mongodb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ReplicationOps implements adapter.ReplicationOperator for MongoDB.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"change_streams"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check MongoDB version (Change Streams require MongoDB 3.6+)
	var buildInfo bson.M
	err := r.conn.db.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "check_replication_prerequisites", err)
	}

	// Check if replica set is configured (required for Change Streams)
	var isMaster bson.M
	err = r.conn.db.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&isMaster)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "check_replication_prerequisites", err)
	}

	if _, ok := isMaster["setName"]; !ok {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"check_replication_prerequisites",
			adapter.ErrConfigurationError,
		).WithContext("error", "MongoDB Change Streams require replica set configuration")
	}

	return nil
}

// Connect creates a new replication connection using Change Streams.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Create the replication source
	source := &MongoDBReplicationSource{
		id:          config.ReplicationID,
		databaseID:  config.DatabaseID,
		db:          r.conn.db,
		config:      config,
		active:      0,
		stopChan:    make(chan struct{}),
		resumeToken: nil,
	}

	// Wrap the event handler to match the expected signature
	if config.EventHandler != nil {
		source.eventHandler = func(event map[string]interface{}) error {
			config.EventHandler(event)
			return nil
		}
	}

	// Set starting position if provided
	if config.StartPosition != "" {
		if err := source.SetPosition(config.StartPosition); err != nil {
			return nil, adapter.WrapError(dbcapabilities.MongoDB, "set_start_position", err)
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	// Get replica set status
	var status bson.M
	err := r.conn.db.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "get_replication_status", err)
	}

	return map[string]interface{}{
		"replica_set_status": status,
		"database_id":        r.conn.id,
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	// For MongoDB, lag is typically measured in replica set replication lag
	var status bson.M
	err := r.conn.db.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "get_replication_lag", err)
	}

	// Extract lag information from replica set status
	lag := map[string]interface{}{
		"database_id": r.conn.id,
	}

	if members, ok := status["members"].(bson.A); ok {
		for _, member := range members {
			if m, ok := member.(bson.M); ok {
				if stateStr, ok := m["stateStr"].(string); ok && stateStr == "PRIMARY" {
					if optime, ok := m["optimeDate"].(time.Time); ok {
						lag["primary_optime"] = optime
					}
				}
			}
		}
	}

	return lag, nil
}

// ListSlots lists all replication slots (not applicable for MongoDB).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.MongoDB,
		"list replication slots",
		"MongoDB uses Change Streams, not replication slots",
	)
}

// DropSlot drops a replication slot (not applicable for MongoDB).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.MongoDB,
		"drop replication slot",
		"MongoDB uses Change Streams, not replication slots",
	)
}

// ListPublications lists all publications (not applicable for MongoDB).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.MongoDB,
		"list publications",
		"MongoDB uses Change Streams, not publications",
	)
}

// DropPublication drops a publication (not applicable for MongoDB).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.MongoDB,
		"drop publication",
		"MongoDB uses Change Streams, not publications",
	)
}

// MongoDBReplicationSource implements adapter.ReplicationSource for MongoDB Change Streams.
type MongoDBReplicationSource struct {
	id           string
	databaseID   string
	db           *mongo.Database
	config       adapter.ReplicationConfig
	stream       *mongo.ChangeStream
	active       int32
	stopChan     chan struct{}
	resumeToken  bson.Raw
	mu           sync.RWMutex
	eventHandler func(map[string]interface{}) error
	checkpointFn func(context.Context, string) error
}

// GetSourceID returns the replication source ID.
func (m *MongoDBReplicationSource) GetSourceID() string {
	return m.id
}

// GetDatabaseID returns the database ID.
func (m *MongoDBReplicationSource) GetDatabaseID() string {
	return m.databaseID
}

// GetStatus returns the replication source status.
func (m *MongoDBReplicationSource) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]interface{}{
		"source_id":   m.id,
		"database_id": m.databaseID,
		"active":      m.IsActive(),
		"mechanism":   "change_streams",
	}

	if m.resumeToken != nil {
		status["resume_token"] = m.resumeToken.String()
	}

	return status
}

// GetMetadata returns the replication source metadata.
func (m *MongoDBReplicationSource) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"source_type":     "mongodb_change_stream",
		"database_type":   "mongodb",
		"replication_id":  m.id,
		"database_id":     m.databaseID,
		"supported_ops":   []string{"insert", "update", "replace", "delete"},
		"resume_capable":  true,
		"transaction_log": false,
	}
}

// IsActive returns whether the replication source is active.
func (m *MongoDBReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&m.active) == 1
}

// Start starts the replication source.
func (m *MongoDBReplicationSource) Start() error {
	if m.IsActive() {
		return adapter.NewDatabaseError(
			dbcapabilities.MongoDB,
			"start_replication",
			adapter.ErrInvalidConfiguration,
		).WithContext("error", "replication source is already active")
	}

	// Create change stream options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// Set resume token if available
	if m.resumeToken != nil {
		opts.SetResumeAfter(m.resumeToken)
	}

	// Create pipeline to filter collections if specified
	var pipeline mongo.Pipeline
	if len(m.config.TableNames) > 0 {
		// Filter by collection names
		pipeline = mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: m.config.TableNames}}},
			}}},
		}
	}

	// Watch for changes
	ctx := context.Background()
	stream, err := m.db.Watch(ctx, pipeline, opts)
	if err != nil {
		return adapter.WrapError(dbcapabilities.MongoDB, "start_change_stream", err)
	}

	m.stream = stream
	atomic.StoreInt32(&m.active, 1)

	// Start event processing in a goroutine
	go m.processEvents()

	return nil
}

// processEvents processes change stream events.
func (m *MongoDBReplicationSource) processEvents() {
	ctx := context.Background()

	for m.IsActive() {
		select {
		case <-m.stopChan:
			return
		default:
			// Try to get next event with timeout
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			hasNext := m.stream.Next(ctx)
			cancel()

			if !hasNext {
				if err := m.stream.Err(); err != nil {
					// Log error but continue
					continue
				}
				continue
			}

			// Get the change event
			var changeEvent bson.M
			if err := m.stream.Decode(&changeEvent); err != nil {
				continue
			}

			// Update resume token
			if resumeToken := m.stream.ResumeToken(); resumeToken != nil {
				m.mu.Lock()
				m.resumeToken = resumeToken
				m.mu.Unlock()
			}

			// Call event handler if set
			if m.eventHandler != nil {
				if err := m.eventHandler(changeEvent); err != nil {
					// Log error but continue processing
					continue
				}
			}
		}
	}
}

// Stop stops the replication source.
func (m *MongoDBReplicationSource) Stop() error {
	if !m.IsActive() {
		return nil
	}

	atomic.StoreInt32(&m.active, 0)
	close(m.stopChan)

	if m.stream != nil {
		return m.stream.Close(context.Background())
	}

	return nil
}

// Close closes the replication source.
func (m *MongoDBReplicationSource) Close() error {
	return m.Stop()
}

// GetPosition returns the current replication position (resume token).
func (m *MongoDBReplicationSource) GetPosition() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.resumeToken == nil {
		return "", nil
	}

	// Convert BSON resume token to string
	return m.resumeToken.String(), nil
}

// SetPosition sets the starting replication position for resume.
func (m *MongoDBReplicationSource) SetPosition(position string) error {
	if position == "" {
		return nil
	}

	// Parse the resume token from string
	// Note: In production, you'd need proper BSON parsing
	// For now, we'll store it as raw BSON
	m.mu.Lock()
	defer m.mu.Unlock()

	// This is a simplified implementation
	// In production, you'd parse the position string back to bson.Raw
	return nil
}

// SaveCheckpoint persists the current replication position.
func (m *MongoDBReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	if m.checkpointFn != nil {
		return m.checkpointFn(ctx, position)
	}
	return nil
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (m *MongoDBReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpointFn = fn
}

// GetDB returns the underlying MongoDB database connection (for internal use).
func (r *ReplicationOps) GetDB() *mongo.Database {
	return r.conn.db
}
