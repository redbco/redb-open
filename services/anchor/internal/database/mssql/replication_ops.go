package mssql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for SQL Server.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"cdc", "change_tracking"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if CDC is enabled on the database
	var cdcEnabled int
	query := `SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()`
	err := r.conn.db.QueryRowContext(ctx, query).Scan(&cdcEnabled)
	if err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "check_cdc_enabled", err)
	}

	if cdcEnabled == 0 {
		return adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"check_replication_prerequisites",
			adapter.ErrConfigurationError,
		).WithContext("error", "SQL Server CDC is not enabled on database. Enable with: EXEC sys.sp_cdc_enable_db")
	}

	return nil
}

// Connect creates a new replication connection using SQL Server CDC.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Create the replication source
	source := &MSSQLReplicationSource{
		id:         config.ReplicationID,
		databaseID: config.DatabaseID,
		db:         r.conn.db,
		config:     config,
		active:     0,
		stopChan:   make(chan struct{}),
		lastLSN:    nil,
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
			return nil, adapter.WrapError(dbcapabilities.SQLServer, "set_start_position", err)
		}
	}

	return source, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	// Query CDC status
	query := `
		SELECT 
			is_cdc_enabled,
			(SELECT COUNT(*) FROM cdc.change_tables) as cdc_table_count
		FROM sys.databases 
		WHERE name = DB_NAME()
	`

	var cdcEnabled int
	var tableCount int
	err := r.conn.db.QueryRowContext(ctx, query).Scan(&cdcEnabled, &tableCount)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "get_replication_status", err)
	}

	return map[string]interface{}{
		"database_id":     r.conn.id,
		"mechanism":       "sql_server_cdc",
		"cdc_enabled":     cdcEnabled == 1,
		"cdc_table_count": tableCount,
	}, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	// Get the current LSN
	var currentLSN []byte
	err := r.conn.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&currentLSN)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "get_current_lsn", err)
	}

	return map[string]interface{}{
		"database_id": r.conn.id,
		"current_lsn": hex.EncodeToString(currentLSN),
		"mechanism":   "sql_server_cdc",
	}, nil
}

// ListSlots lists all replication slots (not applicable for SQL Server CDC).
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(
		dbcapabilities.SQLServer,
		"list replication slots",
		"SQL Server CDC doesn't use slots",
	)
}

// DropSlot drops a replication slot (not applicable for SQL Server CDC).
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(
		dbcapabilities.SQLServer,
		"drop replication slot",
		"SQL Server CDC doesn't use slots",
	)
}

// ListPublications lists all publications (CDC-enabled tables).
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			OBJECT_NAME(source_object_id) as table_name,
			capture_instance,
			start_lsn
		FROM cdc.change_tables
	`

	rows, err := r.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.SQLServer, "list_cdc_tables", err)
	}
	defer rows.Close()

	var publications []map[string]interface{}
	for rows.Next() {
		var tableName, captureInstance string
		var startLSN []byte
		if err := rows.Scan(&tableName, &captureInstance, &startLSN); err != nil {
			continue
		}

		publications = append(publications, map[string]interface{}{
			"table_name":       tableName,
			"capture_instance": captureInstance,
			"start_lsn":        hex.EncodeToString(startLSN),
		})
	}

	return publications, rows.Err()
}

// DropPublication drops a publication (disables CDC on table).
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	// Disable CDC on the specified table
	query := fmt.Sprintf("EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = '%s', @capture_instance = 'all'", publicationName)
	_, err := r.conn.db.ExecContext(ctx, query)
	if err != nil {
		return adapter.WrapError(dbcapabilities.SQLServer, "drop_publication", err)
	}
	return nil
}

// MSSQLReplicationSource implements adapter.ReplicationSource for SQL Server CDC.
type MSSQLReplicationSource struct {
	id           string
	databaseID   string
	db           *sql.DB
	config       adapter.ReplicationConfig
	active       int32
	stopChan     chan struct{}
	lastLSN      []byte
	mu           sync.RWMutex
	eventHandler func(map[string]interface{}) error
	checkpointFn func(context.Context, string) error
}

// GetSourceID returns the replication source ID.
func (m *MSSQLReplicationSource) GetSourceID() string {
	return m.id
}

// GetDatabaseID returns the database ID.
func (m *MSSQLReplicationSource) GetDatabaseID() string {
	return m.databaseID
}

// GetStatus returns the replication source status.
func (m *MSSQLReplicationSource) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]interface{}{
		"source_id":   m.id,
		"database_id": m.databaseID,
		"active":      m.IsActive(),
		"mechanism":   "sql_server_cdc",
	}

	if m.lastLSN != nil {
		status["last_lsn"] = hex.EncodeToString(m.lastLSN)
	}

	return status
}

// GetMetadata returns the replication source metadata.
func (m *MSSQLReplicationSource) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"source_type":     "sql_server_cdc",
		"database_type":   "mssql",
		"replication_id":  m.id,
		"database_id":     m.databaseID,
		"supported_ops":   []string{"INSERT", "UPDATE", "DELETE"},
		"resume_capable":  true,
		"transaction_log": true,
	}
}

// IsActive returns whether the replication source is active.
func (m *MSSQLReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&m.active) == 1
}

// Start starts the replication source.
func (m *MSSQLReplicationSource) Start() error {
	if m.IsActive() {
		return adapter.NewDatabaseError(
			dbcapabilities.SQLServer,
			"start_replication",
			adapter.ErrInvalidConfiguration,
		).WithContext("error", "replication source is already active")
	}

	atomic.StoreInt32(&m.active, 1)

	// Start polling for changes
	for _, tableName := range m.config.TableNames {
		go m.pollTableChanges(tableName)
	}

	return nil
}

// pollTableChanges polls CDC changes for a specific table.
func (m *MSSQLReplicationSource) pollTableChanges(tableName string) {
	ctx := context.Background()

	for m.IsActive() {
		select {
		case <-m.stopChan:
			return
		default:
			// Get the capture instance name for this table
			var captureInstance string
			query := fmt.Sprintf("SELECT capture_instance FROM cdc.change_tables WHERE OBJECT_NAME(source_object_id) = '%s'", tableName)
			err := m.db.QueryRowContext(ctx, query).Scan(&captureInstance)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			// Get current LSN range
			var minLSN, maxLSN []byte
			if m.lastLSN == nil {
				// Get starting LSN
				err = m.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_min_lsn(@capture_instance)", sql.Named("capture_instance", captureInstance)).Scan(&minLSN)
			} else {
				// Use last processed LSN
				minLSN = m.lastLSN
			}

			err = m.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			// Query for changes
			cdcQuery := fmt.Sprintf(`
				SELECT 
					__$operation,
					__$start_lsn,
					*
				FROM cdc.fn_cdc_get_all_changes_%s(@from_lsn, @to_lsn, 'all')
			`, captureInstance)

			rows, err := m.db.QueryContext(ctx, cdcQuery,
				sql.Named("from_lsn", minLSN),
				sql.Named("to_lsn", maxLSN))
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			// Process rows
			columns, _ := rows.Columns()
			for rows.Next() {
				// Create slice for scanning
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					continue
				}

				// Build event map
				event := make(map[string]interface{})
				for i, col := range columns {
					event[col] = values[i]
				}
				event["table_name"] = tableName

				// Call event handler
				if m.eventHandler != nil {
					if err := m.eventHandler(event); err != nil {
						continue
					}
				}

				// Update last LSN
				if lsn, ok := values[1].([]byte); ok {
					m.mu.Lock()
					m.lastLSN = lsn
					m.mu.Unlock()
				}
			}
			rows.Close()

			// Poll interval
			time.Sleep(1 * time.Second)
		}
	}
}

// Stop stops the replication source.
func (m *MSSQLReplicationSource) Stop() error {
	if !m.IsActive() {
		return nil
	}

	atomic.StoreInt32(&m.active, 0)
	close(m.stopChan)

	return nil
}

// Close closes the replication source.
func (m *MSSQLReplicationSource) Close() error {
	return m.Stop()
}

// GetPosition returns the current replication position (LSN).
func (m *MSSQLReplicationSource) GetPosition() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.lastLSN == nil {
		return "", nil
	}

	return hex.EncodeToString(m.lastLSN), nil
}

// SetPosition sets the starting replication position for resume.
func (m *MSSQLReplicationSource) SetPosition(position string) error {
	if position == "" {
		return nil
	}

	// Decode hex LSN
	lsn, err := hex.DecodeString(position)
	if err != nil {
		return fmt.Errorf("invalid LSN format: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastLSN = lsn
	return nil
}

// SaveCheckpoint persists the current replication position.
func (m *MSSQLReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	if m.checkpointFn != nil {
		return m.checkpointFn(ctx, position)
	}
	return nil
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (m *MSSQLReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpointFn = fn
}

// GetDB returns the underlying database connection (for internal use).
func (r *ReplicationOps) GetDB() *sql.DB {
	return r.conn.db
}
