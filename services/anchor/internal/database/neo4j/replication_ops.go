package neo4j

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for Neo4j.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Neo4j supports CDC through transaction logs and change tracking
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"transaction_log", "polling"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check Neo4j version and APOC availability
	session := r.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	// Check if we can execute queries
	_, err := session.Run(ctx, "RETURN 1", nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Neo4j, "check_replication_prerequisites", err)
	}

	return nil
}

// Connect creates a new replication connection using Neo4j change tracking.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Create the replication source
	source := &Neo4jReplicationSource{
		id:                config.ReplicationID,
		databaseID:        config.DatabaseID,
		driver:            r.conn.driver,
		config:            config,
		active:            0,
		stopChan:          make(chan struct{}),
		lastTransactionID: 0,
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
			return nil, adapter.WrapError(dbcapabilities.Neo4j, "set_start_position", err)
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	session := r.conn.driver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)

	// Get last transaction ID
	result, err := session.Run(ctx, "CALL dbms.queryJmx('org.neo4j:instance=kernel#0,name=Transactions') YIELD attributes RETURN attributes.LastCommittedTxId.value AS lastTxId", nil)
	if err != nil {
		// If JMX query fails, return basic status
		return map[string]interface{}{
			"database_id": r.conn.id,
			"status":      "active",
		}, nil
	}

	status := map[string]interface{}{
		"database_id": r.conn.id,
		"status":      "active",
	}

	if result.Next(ctx) {
		record := result.Record()
		if lastTxId, ok := record.Get("lastTxId"); ok {
			status["last_transaction_id"] = lastTxId
		}
	}

	return status, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"lag":         "not_implemented", // TODO: Implement actual lag calculation
	}, nil
}

// ListSlots is not directly applicable for Neo4j.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "list replication slots", "not applicable for Neo4j")
}

// DropSlot is not applicable for Neo4j.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "drop replication slot", "not applicable for Neo4j")
}

// ListPublications is not applicable for Neo4j.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "list publications", "not applicable for Neo4j")
}

// DropPublication is not applicable for Neo4j.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Neo4j, "drop publication", "not applicable for Neo4j")
}

// Neo4jReplicationSource implements adapter.ReplicationSource for Neo4j.
type Neo4jReplicationSource struct {
	id                string
	databaseID        string
	driver            neo4jdriver.DriverWithContext
	config            adapter.ReplicationConfig
	active            int32
	stopChan          chan struct{}
	wg                sync.WaitGroup
	lastTransactionID int64
	eventHandler      func(event map[string]interface{}) error
	mu                sync.Mutex
}

// GetSourceID returns the replication source ID.
func (s *Neo4jReplicationSource) GetSourceID() string {
	return s.id
}

// GetDatabaseID returns the database ID.
func (s *Neo4jReplicationSource) GetDatabaseID() string {
	return s.databaseID
}

// GetStatus returns the replication source status.
func (s *Neo4jReplicationSource) GetStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"active":              s.IsActive(),
		"last_transaction_id": s.lastTransactionID,
		"tracked_labels":      s.config.TableNames, // In Neo4j, table names map to node labels
	}
}

// GetMetadata returns the replication source metadata.
func (s *Neo4jReplicationSource) GetMetadata() map[string]interface{} {
	return s.config.Options
}

// IsActive returns whether the replication source is active.
func (s *Neo4jReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&s.active) == 1
}

// Start starts the replication source.
func (s *Neo4jReplicationSource) Start() error {
	if !atomic.CompareAndSwapInt32(&s.active, 0, 1) {
		return fmt.Errorf("replication source already active")
	}

	s.wg.Add(1)
	go s.pollChanges()
	return nil
}

// Stop stops the replication source.
func (s *Neo4jReplicationSource) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.active, 1, 0) {
		return fmt.Errorf("replication source not active")
	}
	close(s.stopChan)
	s.wg.Wait()
	return nil
}

// Close closes the replication source.
func (s *Neo4jReplicationSource) Close() error {
	if s.IsActive() {
		s.Stop()
	}
	return nil
}

// GetPosition returns the current replication position (last transaction ID).
func (s *Neo4jReplicationSource) GetPosition() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("%d", s.lastTransactionID), nil
}

// SetPosition sets the starting replication position.
func (s *Neo4jReplicationSource) SetPosition(position string) error {
	var txID int64
	if position != "" {
		if _, err := fmt.Sscanf(position, "%d", &txID); err != nil {
			return fmt.Errorf("invalid position format: %w", err)
		}
	}
	s.mu.Lock()
	s.lastTransactionID = txID
	s.mu.Unlock()
	return nil
}

// SaveCheckpoint persists the current replication position.
func (s *Neo4jReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	return s.SetPosition(position)
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (s *Neo4jReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	// Placeholder for external checkpointing
}

func (s *Neo4jReplicationSource) pollChanges() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second) // Poll every second
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.detectChanges()
		}
	}
}

func (s *Neo4jReplicationSource) detectChanges() {
	ctx := context.Background()
	session := s.driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeRead,
		DatabaseName: s.config.DatabaseName,
	})
	defer session.Close(ctx)

	// For each tracked label (table name in relational terms)
	for _, label := range s.config.TableNames {
		// Query nodes with this label that have been modified
		// This is a simplified approach - in production, you'd want to use
		// transaction logs, APOC procedures, or Neo4j Streams
		query := fmt.Sprintf(`
			MATCH (n:%s)
			WHERE n._cdc_timestamp IS NOT NULL 
			  AND n._cdc_timestamp > $lastCheck
			RETURN 
				id(n) AS nodeId, 
				n AS node, 
				labels(n) AS labels,
				n._cdc_operation AS operation,
				n._cdc_timestamp AS timestamp
			ORDER BY n._cdc_timestamp
			LIMIT 1000
		`, label)

		s.mu.Lock()
		lastCheck := s.lastTransactionID
		s.mu.Unlock()

		result, err := session.Run(ctx, query, map[string]interface{}{
			"lastCheck": lastCheck,
		})
		if err != nil {
			// Log error and continue
			continue
		}

		for result.Next(ctx) {
			record := result.Record()

			nodeId, _ := record.Get("nodeId")
			node, _ := record.Get("node")
			labels, _ := record.Get("labels")
			operation, _ := record.Get("operation")
			timestamp, _ := record.Get("timestamp")

			// Convert node to map
			nodeMap := make(map[string]interface{})
			if nodeVal, ok := node.(neo4jdriver.Node); ok {
				nodeMap = nodeVal.Props
			}

			// Create event
			event := map[string]interface{}{
				"node_id":    nodeId,
				"labels":     labels,
				"properties": nodeMap,
				"operation":  operation,
				"timestamp":  timestamp,
				"type":       "node",
			}

			// Call event handler
			if s.eventHandler != nil {
				if err := s.eventHandler(event); err != nil {
					// Log error, continue processing
				}
			}

			// Update last transaction ID
			if ts, ok := timestamp.(int64); ok {
				s.mu.Lock()
				if ts > s.lastTransactionID {
					s.lastTransactionID = ts
				}
				s.mu.Unlock()
			}
		}
	}
}
