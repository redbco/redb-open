package cockroach

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for CockroachDB.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// CockroachDB supports CDC through changefeeds
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"changefeed", "core_changefeed"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check CockroachDB version to ensure changefeed support
	var version string
	err := r.conn.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CockroachDB, "check_replication_prerequisites", err)
	}

	// CockroachDB changefeeds are available in all versions (core changefeeds are experimental)
	// Enterprise changefeeds require a license
	// For now, we'll assume prerequisites are met
	return nil
}

// Connect creates a new replication connection using CockroachDB Changefeeds.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Create the replication source
	source := &CockroachDBReplicationSource{
		id:         config.ReplicationID,
		databaseID: config.DatabaseID,
		pool:       r.conn.pool,
		config:     config,
		active:     0,
		stopChan:   make(chan struct{}),
		cursor:     "",
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
			return nil, adapter.WrapError(dbcapabilities.CockroachDB, "set_start_position", err)
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"status":      "not_implemented", // TODO: Implement actual status retrieval
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"lag":         "not_implemented", // TODO: Implement actual lag calculation
	}, nil
}

// ListSlots is not directly applicable for CockroachDB Changefeeds.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	// CockroachDB changefeeds don't use slots, but we can list jobs
	query := `
		SELECT job_id, job_type, description, status, created
		FROM [SHOW JOBS]
		WHERE job_type = 'CHANGEFEED'
		ORDER BY created DESC
	`
	rows, err := r.conn.pool.Query(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "list_changefeed_jobs", err)
	}
	defer rows.Close()

	var jobs []map[string]interface{}
	for rows.Next() {
		var jobID int64
		var jobType, description, status string
		var created time.Time
		if err := rows.Scan(&jobID, &jobType, &description, &status, &created); err != nil {
			return nil, adapter.WrapError(dbcapabilities.CockroachDB, "list_changefeed_jobs", err)
		}
		jobs = append(jobs, map[string]interface{}{
			"job_id":      jobID,
			"job_type":    jobType,
			"description": description,
			"status":      status,
			"created":     created,
		})
	}

	return jobs, rows.Err()
}

// DropSlot cancels a changefeed job (if job_id is provided).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	// In CockroachDB, we cancel changefeed jobs
	query := fmt.Sprintf("CANCEL JOB %s", slotName)
	_, err := r.conn.pool.Exec(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.CockroachDB, "cancel_changefeed_job", err)
	}
	return nil
}

// ListPublications is not applicable for CockroachDB Changefeeds.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.CockroachDB, "list publications", "not applicable for CockroachDB Changefeeds")
}

// DropPublication is not applicable for CockroachDB Changefeeds.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.CockroachDB, "drop publication", "not applicable for CockroachDB Changefeeds")
}

// CockroachDBReplicationSource implements adapter.ReplicationSource for CockroachDB Changefeeds.
type CockroachDBReplicationSource struct {
	id           string
	databaseID   string
	pool         *pgxpool.Pool
	config       adapter.ReplicationConfig
	active       int32
	stopChan     chan struct{}
	wg           sync.WaitGroup
	cursor       string // High-water timestamp
	eventHandler func(event map[string]interface{}) error
	mu           sync.Mutex
}

// GetSourceID returns the replication source ID.
func (s *CockroachDBReplicationSource) GetSourceID() string {
	return s.id
}

// GetDatabaseID returns the database ID.
func (s *CockroachDBReplicationSource) GetDatabaseID() string {
	return s.databaseID
}

// GetStatus returns the replication source status.
func (s *CockroachDBReplicationSource) GetStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"active":      s.IsActive(),
		"cursor":      s.cursor,
		"table_names": s.config.TableNames,
	}
}

// GetMetadata returns the replication source metadata.
func (s *CockroachDBReplicationSource) GetMetadata() map[string]interface{} {
	return s.config.Options
}

// IsActive returns whether the replication source is active.
func (s *CockroachDBReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&s.active) == 1
}

// Start starts the replication source.
func (s *CockroachDBReplicationSource) Start() error {
	if !atomic.CompareAndSwapInt32(&s.active, 0, 1) {
		return fmt.Errorf("replication source already active")
	}

	s.wg.Add(1)
	go s.run()
	return nil
}

// Stop stops the replication source.
func (s *CockroachDBReplicationSource) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.active, 1, 0) {
		return fmt.Errorf("replication source not active")
	}
	close(s.stopChan)
	s.wg.Wait()
	return nil
}

// Close closes the replication source.
func (s *CockroachDBReplicationSource) Close() error {
	if s.IsActive() {
		s.Stop()
	}
	return nil
}

// GetPosition returns the current replication position (cursor).
func (s *CockroachDBReplicationSource) GetPosition() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, nil
}

// SetPosition sets the starting replication position.
func (s *CockroachDBReplicationSource) SetPosition(position string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = position
	return nil
}

// SaveCheckpoint persists the current replication position.
func (s *CockroachDBReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	return s.SetPosition(position)
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (s *CockroachDBReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	// This is where the external checkpointing mechanism would be hooked in.
	// For this implementation, we'll rely on the internal SaveCheckpoint.
}

func (s *CockroachDBReplicationSource) run() {
	defer s.wg.Done()

	ctx := context.Background()

	// Build table list for changefeed
	tableList := ""
	if len(s.config.TableNames) > 0 {
		tables := make([]string, len(s.config.TableNames))
		for i, t := range s.config.TableNames {
			tables[i] = fmt.Sprintf("TABLE %s", t)
		}
		tableList = strings.Join(tables, ", ")
	} else {
		// If no tables specified, we can't create a changefeed
		return
	}

	// Build changefeed options
	options := []string{
		"updated",            // Include updated timestamp
		"resolved='1s'",      // Emit resolved timestamps every 1 second
		"diff",               // Include before/after images
		"format=json",        // JSON format
		"envelope='wrapped'", // Use wrapped envelope
	}

	// Add cursor if resuming from a position
	if s.cursor != "" {
		options = append(options, fmt.Sprintf("cursor='%s'", s.cursor))
	}

	optionsStr := strings.Join(options, ", ")

	// Create a core changefeed (experimental)
	// Note: This requires EXPERIMENTAL CHANGEFEED to be enabled
	query := fmt.Sprintf(
		"EXPERIMENTAL CHANGEFEED FOR %s WITH %s",
		tableList,
		optionsStr,
	)

	// Execute the changefeed query
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		// Log error
		return
	}
	defer rows.Close()

	// Process changefeed events
	for {
		select {
		case <-s.stopChan:
			return
		default:
			if !rows.Next() {
				// Check for errors
				if err := rows.Err(); err != nil {
					// Log error
					return
				}
				// No more rows, exit
				return
			}

			// Changefeed returns: table, key, value
			var table, key, value *string
			if err := rows.Scan(&table, &key, &value); err != nil {
				// Log error
				continue
			}

			// Skip resolved timestamps (they have nil key and value)
			if key == nil && value == nil {
				// This is a resolved timestamp marker
				// We could update cursor here if needed
				continue
			}

			// Parse the changefeed event
			var eventMap map[string]interface{}
			if value != nil {
				if err := json.Unmarshal([]byte(*value), &eventMap); err != nil {
					// Log error
					continue
				}
			} else {
				eventMap = make(map[string]interface{})
			}

			// Add table name and key to the event
			if table != nil {
				eventMap["table"] = *table
			}
			if key != nil {
				var keyMap map[string]interface{}
				if err := json.Unmarshal([]byte(*key), &keyMap); err == nil {
					eventMap["key"] = keyMap
				}
			}

			// Update cursor if present in the event
			if updated, ok := eventMap["updated"].(string); ok {
				s.mu.Lock()
				s.cursor = updated
				s.mu.Unlock()
			}

			// Call the event handler
			if s.eventHandler != nil {
				if err := s.eventHandler(eventMap); err != nil {
					// Log error, continue processing
				}
			}
		}
	}
}
