package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for Snowflake.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Snowflake supports CDC through Streams
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"streams", "table_streams"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if we can create streams (requires appropriate privileges)
	// For now, assume prerequisites are met
	return nil
}

// Connect creates a new replication connection using Snowflake Streams.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Create the replication source
	source := &SnowflakeReplicationSource{
		id:          config.ReplicationID,
		databaseID:  config.DatabaseID,
		db:          r.conn.db,
		config:      config,
		active:      0,
		stopChan:    make(chan struct{}),
		streamNames: make(map[string]string), // Map table name to stream name
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
			return nil, adapter.WrapError(dbcapabilities.Snowflake, "set_start_position", err)
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

// ListSlots lists Snowflake streams.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	query := "SHOW STREAMS"
	rows, err := r.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.Snowflake, "list_streams", err)
	}
	defer rows.Close()

	var streams []map[string]interface{}
	for rows.Next() {
		// SHOW STREAMS returns many columns, we'll scan the key ones
		var createdOn, name, databaseName, schemaName, owner, comment, tableOn, sourceType, baseTables, streamType, stale, mode, staleness string
		err := rows.Scan(&createdOn, &name, &databaseName, &schemaName, &owner, &comment, &tableOn, &sourceType, &baseTables, &streamType, &stale, &mode, &staleness)
		if err != nil {
			continue
		}

		streams = append(streams, map[string]interface{}{
			"created_on":    createdOn,
			"name":          name,
			"database_name": databaseName,
			"schema_name":   schemaName,
			"table_on":      tableOn,
			"stream_type":   streamType,
			"stale":         stale,
			"mode":          mode,
		})
	}

	return streams, rows.Err()
}

// DropSlot drops a Snowflake stream.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	query := fmt.Sprintf("DROP STREAM IF EXISTS %s", r.quoteIdentifier(slotName))
	_, err := r.conn.db.ExecContext(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Snowflake, "drop_stream", err)
	}
	return nil
}

// ListPublications is not applicable for Snowflake Streams.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "list publications", "not applicable for Snowflake Streams")
}

// DropPublication is not applicable for Snowflake Streams.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Snowflake, "drop publication", "not applicable for Snowflake Streams")
}

// SnowflakeReplicationSource implements adapter.ReplicationSource for Snowflake Streams.
type SnowflakeReplicationSource struct {
	id           string
	databaseID   string
	db           *sql.DB
	config       adapter.ReplicationConfig
	active       int32
	stopChan     chan struct{}
	wg           sync.WaitGroup
	streamNames  map[string]string // Map table name to stream name
	eventHandler func(event map[string]interface{}) error
	mu           sync.Mutex
}

// GetSourceID returns the replication source ID.
func (s *SnowflakeReplicationSource) GetSourceID() string {
	return s.id
}

// GetDatabaseID returns the database ID.
func (s *SnowflakeReplicationSource) GetDatabaseID() string {
	return s.databaseID
}

// GetStatus returns the replication source status.
func (s *SnowflakeReplicationSource) GetStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"active":       s.IsActive(),
		"stream_names": s.streamNames,
		"table_names":  s.config.TableNames,
	}
}

// GetMetadata returns the replication source metadata.
func (s *SnowflakeReplicationSource) GetMetadata() map[string]interface{} {
	return s.config.Options
}

// IsActive returns whether the replication source is active.
func (s *SnowflakeReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&s.active) == 1
}

// Start starts the replication source.
func (s *SnowflakeReplicationSource) Start() error {
	if !atomic.CompareAndSwapInt32(&s.active, 0, 1) {
		return fmt.Errorf("replication source already active")
	}

	// Create streams for each table
	ctx := context.Background()
	for _, tableName := range s.config.TableNames {
		streamName := fmt.Sprintf("%s_stream_%s", tableName, s.id[:8])

		// Create stream
		createStreamSQL := fmt.Sprintf(
			"CREATE STREAM IF NOT EXISTS %s ON TABLE %s",
			streamName,
			tableName,
		)
		_, err := s.db.ExecContext(ctx, createStreamSQL)
		if err != nil {
			s.Stop()
			return adapter.WrapError(dbcapabilities.Snowflake, "create_stream", err)
		}

		s.mu.Lock()
		s.streamNames[tableName] = streamName
		s.mu.Unlock()

		// Start goroutine to poll this stream
		s.wg.Add(1)
		go s.pollStream(streamName, tableName)
	}

	return nil
}

// Stop stops the replication source.
func (s *SnowflakeReplicationSource) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.active, 1, 0) {
		return fmt.Errorf("replication source not active")
	}
	close(s.stopChan)
	s.wg.Wait()
	return nil
}

// Close closes the replication source and cleans up streams.
func (s *SnowflakeReplicationSource) Close() error {
	if s.IsActive() {
		s.Stop()
	}

	// Drop created streams
	ctx := context.Background()
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, streamName := range s.streamNames {
		dropStreamSQL := fmt.Sprintf("DROP STREAM IF EXISTS %s", streamName)
		s.db.ExecContext(ctx, dropStreamSQL) // Ignore errors during cleanup
	}

	return nil
}

// GetPosition returns the current replication position.
func (s *SnowflakeReplicationSource) GetPosition() (string, error) {
	// Snowflake streams track position internally
	// We could return the last processed timestamp or other metadata
	return "", nil
}

// SetPosition sets the starting replication position.
func (s *SnowflakeReplicationSource) SetPosition(position string) error {
	// Snowflake streams don't support arbitrary position setting
	// Position is managed by Snowflake internally
	return nil
}

// SaveCheckpoint persists the current replication position.
func (s *SnowflakeReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	return s.SetPosition(position)
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (s *SnowflakeReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	// Placeholder for external checkpointing
}

func (s *SnowflakeReplicationSource) pollStream(streamName, tableName string) {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second) // Poll every second
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			// Query the stream for changes
			query := fmt.Sprintf("SELECT * FROM %s", streamName)
			rows, err := s.db.Query(query)
			if err != nil {
				// Log error
				continue
			}

			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				continue
			}

			// Process each row
			for rows.Next() {
				// Create a map to hold the row data
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range columns {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					continue
				}

				// Convert to map
				rowMap := make(map[string]interface{})
				var metadataAction string
				var metadataIsUpdate bool

				for i, col := range columns {
					val := values[i]
					// Handle byte slices (convert to string)
					if b, ok := val.([]byte); ok {
						val = string(b)
					}

					rowMap[col] = val

					// Extract Snowflake metadata columns
					if col == "METADATA$ACTION" {
						if v, ok := val.(string); ok {
							metadataAction = v
						}
					} else if col == "METADATA$ISUPDATE" {
						if v, ok := val.(bool); ok {
							metadataIsUpdate = v
						} else if v, ok := val.(string); ok {
							metadataIsUpdate = (v == "TRUE" || v == "true")
						}
					}
				}

				// Add table name
				rowMap["table_name"] = tableName

				// Determine operation type
				if metadataIsUpdate {
					rowMap["operation"] = "UPDATE"
				} else if metadataAction == "INSERT" {
					rowMap["operation"] = "INSERT"
				} else if metadataAction == "DELETE" {
					rowMap["operation"] = "DELETE"
				}

				// Call the event handler
				if s.eventHandler != nil {
					if err := s.eventHandler(rowMap); err != nil {
						// Log error, continue processing
					}
				}
			}
			rows.Close()
		}
	}
}
