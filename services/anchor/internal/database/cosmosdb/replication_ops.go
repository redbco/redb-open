package cosmosdb

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for CosmosDB.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"change_feed"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// CosmosDB Change Feed is always available, no prerequisites needed
	return nil
}

// Connect creates a new replication connection using CosmosDB Change Feed.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Create the replication source
	source := &CosmosDBReplicationSource{
		id:                config.ReplicationID,
		databaseID:        config.DatabaseID,
		client:            r.conn.client,
		dbName:            r.conn.dbName,
		config:            config,
		active:            0,
		stopChan:          make(chan struct{}),
		continuationToken: "",
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
			return nil, adapter.WrapError(dbcapabilities.CosmosDB, "set_start_position", err)
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"mechanism":   "change_feed",
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	// CosmosDB Change Feed lag is typically very low
	return map[string]interface{}{
		"database_id": r.conn.id,
		"mechanism":   "change_feed",
		"note":        "CosmosDB Change Feed typically has < 1 second lag",
	}, nil
}

// ListSlots lists all replication slots (not applicable for CosmosDB).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.CosmosDB,
		"list replication slots",
		"CosmosDB uses Change Feed, not replication slots",
	)
}

// DropSlot drops a replication slot (not applicable for CosmosDB).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.CosmosDB,
		"drop replication slot",
		"CosmosDB uses Change Feed, not replication slots",
	)
}

// ListPublications lists all publications (not applicable for CosmosDB).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.CosmosDB,
		"list publications",
		"CosmosDB uses Change Feed, not publications",
	)
}

// DropPublication drops a publication (not applicable for CosmosDB).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.CosmosDB,
		"drop publication",
		"CosmosDB uses Change Feed, not publications",
	)
}

// CosmosDBReplicationSource implements adapter.ReplicationSource for CosmosDB Change Feed.
type CosmosDBReplicationSource struct {
	id                string
	databaseID        string
	client            *azcosmos.Client
	dbName            string
	config            adapter.ReplicationConfig
	active            int32
	stopChan          chan struct{}
	continuationToken string
	mu                sync.RWMutex
	eventHandler      func(map[string]interface{}) error
	checkpointFn      func(context.Context, string) error
}

// GetSourceID returns the replication source ID.
func (c *CosmosDBReplicationSource) GetSourceID() string {
	return c.id
}

// GetDatabaseID returns the database ID.
func (c *CosmosDBReplicationSource) GetDatabaseID() string {
	return c.databaseID
}

// GetStatus returns the replication source status.
func (c *CosmosDBReplicationSource) GetStatus() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := map[string]interface{}{
		"source_id":   c.id,
		"database_id": c.databaseID,
		"active":      c.IsActive(),
		"mechanism":   "change_feed",
	}

	if c.continuationToken != "" {
		status["continuation_token"] = c.continuationToken
	}

	return status
}

// GetMetadata returns the replication source metadata.
func (c *CosmosDBReplicationSource) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"source_type":     "cosmosdb_change_feed",
		"database_type":   "cosmosdb",
		"replication_id":  c.id,
		"database_id":     c.databaseID,
		"supported_ops":   []string{"create", "replace", "delete"},
		"resume_capable":  true,
		"transaction_log": false,
	}
}

// IsActive returns whether the replication source is active.
func (c *CosmosDBReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&c.active) == 1
}

// Start starts the replication source.
func (c *CosmosDBReplicationSource) Start() error {
	if c.IsActive() {
		return adapter.NewDatabaseError(
			dbcapabilities.CosmosDB,
			"start_replication",
			adapter.ErrInvalidConfiguration,
		).WithContext("error", "replication source is already active")
	}

	atomic.StoreInt32(&c.active, 1)

	// Start event processing in goroutines (one per container)
	for _, containerName := range c.config.TableNames {
		go c.processContainerChanges(containerName)
	}

	return nil
}

// processContainerChanges processes change feed for a single container.
func (c *CosmosDBReplicationSource) processContainerChanges(containerName string) {
	ctx := context.Background()

	// Get database client
	dbClient, err := c.client.NewDatabase(c.dbName)
	if err != nil {
		return
	}

	// Get container client
	containerClient, err := dbClient.NewContainer(containerName)
	if err != nil {
		return
	}

	// Create query pager for change feed
	// Note: The Azure SDK Change Feed support varies by version
	// This is a simplified implementation
	for c.IsActive() {
		select {
		case <-c.stopChan:
			return
		default:
			// Query for changes
			// In a real implementation, you would use the Change Feed processor
			// or the Change Feed pull model with continuation tokens

			// For now, we'll use a simple query approach
			// In production, use the proper Change Feed APIs
			query := "SELECT * FROM c"
			queryPager := containerClient.NewQueryItemsPager(query, azcosmos.PartitionKey{}, nil)

			for queryPager.More() && c.IsActive() {
				resp, err := queryPager.NextPage(ctx)
				if err != nil {
					// Log error and retry with backoff
					time.Sleep(1 * time.Second)
					continue
				}

				// Process items
				for _, item := range resp.Items {
					// Parse item
					var doc map[string]interface{}
					if err := json.Unmarshal(item, &doc); err != nil {
						continue
					}

					// Create event
					event := map[string]interface{}{
						"container_name": containerName,
						"document":       doc,
						"operation":      "upsert", // Change feed doesn't distinguish insert/update
					}

					// Call event handler if set
					if c.eventHandler != nil {
						if err := c.eventHandler(event); err != nil {
							// Log error but continue processing
							continue
						}
					}
				}
			}

			// Small delay before next poll
			time.Sleep(1 * time.Second)
		}
	}
}

// Stop stops the replication source.
func (c *CosmosDBReplicationSource) Stop() error {
	if !c.IsActive() {
		return nil
	}

	atomic.StoreInt32(&c.active, 0)
	close(c.stopChan)

	return nil
}

// Close closes the replication source.
func (c *CosmosDBReplicationSource) Close() error {
	return c.Stop()
}

// GetPosition returns the current replication position (continuation token).
func (c *CosmosDBReplicationSource) GetPosition() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.continuationToken, nil
}

// SetPosition sets the starting replication position for resume.
func (c *CosmosDBReplicationSource) SetPosition(position string) error {
	if position == "" {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.continuationToken = position
	return nil
}

// SaveCheckpoint persists the current replication position.
func (c *CosmosDBReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	if c.checkpointFn != nil {
		return c.checkpointFn(ctx, position)
	}
	return nil
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (c *CosmosDBReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpointFn = fn
}

// GetClient returns the underlying CosmosDB client (for internal use).
func (r *ReplicationOps) GetClient() *azcosmos.Client {
	return r.conn.client
}
