package cassandra

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator for Cassandra.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// Cassandra supports CDC in 3.8+ and DSE 6.0+
	// For now, we'll use polling-based CDC which works on all versions
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	// Check if native CDC is available
	if r.hasCDCSupport() {
		return []string{"cdc", "polling"}
	}
	return []string{"polling"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Verify connection is working
	if err := r.conn.session.Query("SELECT now() FROM system.local").WithContext(ctx).Exec(); err != nil {
		return adapter.WrapError(dbcapabilities.Cassandra, "check_replication_prerequisites", err)
	}
	return nil
}

// Connect creates a new replication connection using Cassandra CDC or polling.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// For Cassandra, table names should be in format "keyspace.table"
	if len(config.TableNames) == 0 {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.Cassandra,
			"connect_replication",
			adapter.ErrInvalidData,
		).WithContext("error", "at least one table name required")
	}

	// Create the replication source
	source := &CassandraReplicationSource{
		id:            config.ReplicationID,
		databaseID:    config.DatabaseID,
		session:       r.conn.session,
		config:        config,
		active:        0,
		stopChan:      make(chan struct{}),
		currentStates: make(map[string]map[string]map[string]interface{}),
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
	status := map[string]interface{}{
		"database_id": r.conn.id,
		"status":      "active",
	}

	// Check if CDC is supported
	if r.hasCDCSupport() {
		status["cdc_support"] = "available"
	} else {
		status["cdc_support"] = "polling_fallback"
	}

	return status, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	return map[string]interface{}{
		"database_id": r.conn.id,
		"lag":         "polling_based", // Polling has inherent lag based on poll interval
	}, nil
}

// ListSlots is not directly applicable for Cassandra.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "list replication slots", "not applicable for Cassandra")
}

// DropSlot is not applicable for Cassandra.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "drop replication slot", "not applicable for Cassandra")
}

// ListPublications is not applicable for Cassandra.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "list publications", "not applicable for Cassandra")
}

// DropPublication is not applicable for Cassandra.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	return adapter.NewUnsupportedOperationError(dbcapabilities.Cassandra, "drop publication", "not applicable for Cassandra")
}

// hasCDCSupport checks if Cassandra CDC is available.
func (r *ReplicationOps) hasCDCSupport() bool {
	var version string
	err := r.conn.session.Query("SELECT release_version FROM system.local").Scan(&version)
	if err != nil {
		return false
	}

	// Check for DSE (DataStax Enterprise) which has CDC support
	if strings.Contains(strings.ToLower(version), "dse") {
		return true
	}

	// Check for Apache Cassandra 3.8+ (basic version check)
	// This is simplified - in production, you'd want more robust version parsing
	return strings.HasPrefix(version, "3.8") || strings.HasPrefix(version, "3.9") ||
		strings.HasPrefix(version, "4.") || strings.HasPrefix(version, "5.")
}

// CassandraReplicationSource implements adapter.ReplicationSource for Cassandra.
type CassandraReplicationSource struct {
	id            string
	databaseID    string
	session       *gocql.Session
	config        adapter.ReplicationConfig
	active        int32
	stopChan      chan struct{}
	wg            sync.WaitGroup
	currentStates map[string]map[string]map[string]interface{} // table -> row_key -> row_data
	eventHandler  func(event map[string]interface{}) error
	mu            sync.Mutex
}

// GetSourceID returns the replication source ID.
func (s *CassandraReplicationSource) GetSourceID() string {
	return s.id
}

// GetDatabaseID returns the database ID.
func (s *CassandraReplicationSource) GetDatabaseID() string {
	return s.databaseID
}

// GetStatus returns the replication source status.
func (s *CassandraReplicationSource) GetStatus() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"active":       s.IsActive(),
		"tables":       s.config.TableNames,
		"tracked_rows": s.getTrackedRowCount(),
	}
}

// GetMetadata returns the replication source metadata.
func (s *CassandraReplicationSource) GetMetadata() map[string]interface{} {
	return s.config.Options
}

// IsActive returns whether the replication source is active.
func (s *CassandraReplicationSource) IsActive() bool {
	return atomic.LoadInt32(&s.active) == 1
}

// Start starts the replication source.
func (s *CassandraReplicationSource) Start() error {
	if !atomic.CompareAndSwapInt32(&s.active, 0, 1) {
		return fmt.Errorf("replication source already active")
	}

	// Start polling for each table
	for _, tableName := range s.config.TableNames {
		s.wg.Add(1)
		go s.pollTable(tableName)
	}

	return nil
}

// Stop stops the replication source.
func (s *CassandraReplicationSource) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.active, 1, 0) {
		return fmt.Errorf("replication source not active")
	}

	close(s.stopChan)
	s.wg.Wait()
	return nil
}

// Close closes the replication source.
func (s *CassandraReplicationSource) Close() error {
	if s.IsActive() {
		s.Stop()
	}
	return nil
}

// GetPosition returns the current replication position (not applicable for polling).
func (s *CassandraReplicationSource) GetPosition() (string, error) {
	// Polling-based CDC doesn't have a position
	return "", nil
}

// SetPosition sets the starting replication position (not applicable for polling).
func (s *CassandraReplicationSource) SetPosition(position string) error {
	// Polling-based CDC doesn't support position setting
	return nil
}

// SaveCheckpoint persists the current replication position.
func (s *CassandraReplicationSource) SaveCheckpoint(ctx context.Context, position string) error {
	return s.SetPosition(position)
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (s *CassandraReplicationSource) SetCheckpointFunc(fn func(context.Context, string) error) {
	// Placeholder for external checkpointing
}

func (s *CassandraReplicationSource) pollTable(tableName string) {
	defer s.wg.Done()

	// Parse table name into keyspace and table
	parts := strings.Split(tableName, ".")
	var keyspace, table string
	if len(parts) == 2 {
		keyspace = parts[0]
		table = parts[1]
	} else {
		// Try to get default keyspace
		keyspace = s.session.Query("SELECT keyspace_name FROM system_schema.keyspaces LIMIT 1").String()
		table = tableName
	}

	ctx := context.Background()

	// Get table schema information
	columnNames, primaryKey, err := s.getTableSchema(ctx, keyspace, table)
	if err != nil {
		// Log error and exit
		return
	}

	// Initialize current state
	s.mu.Lock()
	s.currentStates[tableName] = make(map[string]map[string]interface{})
	s.mu.Unlock()

	// Polling loop
	ticker := time.NewTicker(5 * time.Second) // Poll every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			// Fetch current state
			newState, err := s.fetchAllRows(ctx, keyspace, table, columnNames, primaryKey)
			if err != nil {
				// Log error and continue
				continue
			}

			// Compare states to detect changes
			s.mu.Lock()
			currentState := s.currentStates[tableName]
			changes := s.detectChanges(currentState, newState, primaryKey, tableName)
			s.currentStates[tableName] = newState
			s.mu.Unlock()

			// Process changes
			for _, change := range changes {
				event := map[string]interface{}{
					"table_name": tableName,
					"keyspace":   keyspace,
					"operation":  change.Operation,
					"data":       change.Data,
					"old_data":   change.OldData,
				}

				if s.eventHandler != nil {
					if err := s.eventHandler(event); err != nil {
						// Log error, continue processing
					}
				}
			}
		}
	}
}

func (s *CassandraReplicationSource) getTableSchema(ctx context.Context, keyspace, table string) ([]string, []string, error) {
	// Get column names
	var columns []string
	iter := s.session.Query(
		"SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
		keyspace, table,
	).WithContext(ctx).Iter()

	var columnName string
	for iter.Scan(&columnName) {
		columns = append(columns, columnName)
	}
	if err := iter.Close(); err != nil {
		return nil, nil, err
	}

	// Get primary key columns
	var primaryKey []string
	iter = s.session.Query(
		"SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'",
		keyspace, table,
	).WithContext(ctx).Iter()

	for iter.Scan(&columnName) {
		primaryKey = append(primaryKey, columnName)
	}
	if err := iter.Close(); err != nil {
		return columns, nil, err
	}

	// Also get clustering keys
	iter = s.session.Query(
		"SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND kind = 'clustering'",
		keyspace, table,
	).WithContext(ctx).Iter()

	for iter.Scan(&columnName) {
		primaryKey = append(primaryKey, columnName)
	}
	if err := iter.Close(); err != nil {
		return columns, primaryKey, err
	}

	return columns, primaryKey, nil
}

func (s *CassandraReplicationSource) fetchAllRows(ctx context.Context, keyspace, table string, columns, primaryKey []string) (map[string]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(columns, ", "),
		QuoteIdentifier(keyspace),
		QuoteIdentifier(table))

	iter := s.session.Query(query).WithContext(ctx).Iter()

	result := make(map[string]map[string]interface{})
	row := make(map[string]interface{})

	for iter.MapScan(row) {
		// Create a key from primary key columns
		key := s.createRowKey(row, primaryKey)

		// Create a copy of the row
		rowCopy := make(map[string]interface{})
		for k, v := range row {
			rowCopy[k] = ConvertCassandraValueToGo(v)
		}

		result[key] = rowCopy

		// Clear the map for the next iteration
		for k := range row {
			delete(row, k)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *CassandraReplicationSource) createRowKey(row map[string]interface{}, primaryKey []string) string {
	var keyParts []string
	for _, col := range primaryKey {
		val := row[col]
		keyParts = append(keyParts, fmt.Sprintf("%v", val))
	}
	return strings.Join(keyParts, ":")
}

func (s *CassandraReplicationSource) detectChanges(oldState, newState map[string]map[string]interface{}, primaryKey []string, tableName string) []CassandraReplicationChange {
	var changes []CassandraReplicationChange

	// Detect deletions
	for key, oldRow := range oldState {
		if _, exists := newState[key]; !exists {
			changes = append(changes, CassandraReplicationChange{
				Operation: "DELETE",
				OldData:   oldRow,
				Data:      nil,
			})
		}
	}

	// Detect inserts and updates
	for key, newRow := range newState {
		oldRow, exists := oldState[key]
		if !exists {
			// Insert
			changes = append(changes, CassandraReplicationChange{
				Operation: "INSERT",
				OldData:   nil,
				Data:      newRow,
			})
		} else if !s.mapsEqual(oldRow, newRow) {
			// Update
			changes = append(changes, CassandraReplicationChange{
				Operation: "UPDATE",
				OldData:   oldRow,
				Data:      newRow,
			})
		}
	}

	return changes
}

func (s *CassandraReplicationSource) mapsEqual(m1, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return false
	}

	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			return false
		}

		// Compare values
		if !reflect.DeepEqual(v1, v2) {
			return false
		}
	}

	return true
}

func (s *CassandraReplicationSource) getTrackedRowCount() int {
	count := 0
	for _, tableState := range s.currentStates {
		count += len(tableState)
	}
	return count
}
