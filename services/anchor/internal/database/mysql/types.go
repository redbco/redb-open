package mysql

import (
	"context"
	"fmt"
	"sync"
)

// MySQLReplicationSourceDetails contains details about a MySQL replication source
type MySQLReplicationSourceDetails struct {
	BinlogFile     string `json:"binlog_file"`
	BinlogPosition uint32 `json:"binlog_position"`
	TableName      string `json:"table_name"`
	DatabaseID     string `json:"database_id"`
	SlotName       string `json:"slot_name"` // For compatibility with interface

	// Position tracking for graceful shutdown and resume
	positionMutex  sync.RWMutex
	checkpointFunc func(context.Context, string) error
	isActive       bool
}

// MySQLReplicationChange represents a change in MySQL replication
type MySQLReplicationChange struct {
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
}

// GetSourceID returns the replication source ID (slot name or database ID).
func (m *MySQLReplicationSourceDetails) GetSourceID() string {
	if m.SlotName != "" {
		return m.SlotName
	}
	return m.DatabaseID
}

// GetDatabaseID returns the database ID.
func (m *MySQLReplicationSourceDetails) GetDatabaseID() string {
	return m.DatabaseID
}

// GetStatus returns the replication status.
func (m *MySQLReplicationSourceDetails) GetStatus() map[string]interface{} {
	m.positionMutex.RLock()
	defer m.positionMutex.RUnlock()

	return map[string]interface{}{
		"binlog_file":     m.BinlogFile,
		"binlog_position": m.BinlogPosition,
		"table_name":      m.TableName,
		"database_id":     m.DatabaseID,
		"is_active":       m.isActive,
	}
}

// GetMetadata returns the replication metadata.
func (m *MySQLReplicationSourceDetails) GetMetadata() map[string]interface{} {
	m.positionMutex.RLock()
	defer m.positionMutex.RUnlock()

	return map[string]interface{}{
		"binlog_file":     m.BinlogFile,
		"binlog_position": m.BinlogPosition,
		"table_name":      m.TableName,
		"database_id":     m.DatabaseID,
	}
}

// IsActive returns whether the replication source is active.
func (m *MySQLReplicationSourceDetails) IsActive() bool {
	m.positionMutex.RLock()
	defer m.positionMutex.RUnlock()
	return m.isActive
}

// Start starts the replication source.
func (m *MySQLReplicationSourceDetails) Start() error {
	m.positionMutex.Lock()
	defer m.positionMutex.Unlock()

	if m.isActive {
		return nil // Already active
	}

	m.isActive = true
	// TODO: Start MySQL binlog streaming when implemented
	return nil
}

// Stop stops the replication source.
func (m *MySQLReplicationSourceDetails) Stop() error {
	m.positionMutex.Lock()
	defer m.positionMutex.Unlock()

	if !m.isActive {
		return nil // Already stopped
	}

	m.isActive = false
	// TODO: Stop MySQL binlog streaming when implemented
	return nil
}

// Close closes the replication source.
func (m *MySQLReplicationSourceDetails) Close() error {
	// Stop replication first
	if err := m.Stop(); err != nil {
		return fmt.Errorf("failed to stop replication: %w", err)
	}

	// TODO: Close MySQL binlog connection when implemented
	return nil
}

// GetPosition returns the current binlog position as a string.
// Format: "filename:position" (e.g., "mysql-bin.000001:12345")
func (m *MySQLReplicationSourceDetails) GetPosition() (string, error) {
	m.positionMutex.RLock()
	defer m.positionMutex.RUnlock()

	if m.BinlogFile == "" {
		return "", fmt.Errorf("no binlog position available")
	}

	return fmt.Sprintf("%s:%d", m.BinlogFile, m.BinlogPosition), nil
}

// SetPosition sets the starting binlog position for replication resume.
// The position string should be in format "filename:position" (e.g., "mysql-bin.000001:12345").
func (m *MySQLReplicationSourceDetails) SetPosition(position string) error {
	if position == "" {
		return nil // No position to set, will start from beginning
	}

	// Parse position string "filename:position"
	var file string
	var pos uint32
	_, err := fmt.Sscanf(position, "%s:%d", &file, &pos)
	if err != nil {
		return fmt.Errorf("invalid binlog position %q (expected format: filename:position): %w", position, err)
	}

	m.positionMutex.Lock()
	m.BinlogFile = file
	m.BinlogPosition = pos
	m.positionMutex.Unlock()

	return nil
}

// SaveCheckpoint persists the current replication position.
func (m *MySQLReplicationSourceDetails) SaveCheckpoint(ctx context.Context, position string) error {
	if m.checkpointFunc == nil {
		// No checkpoint function configured
		return nil
	}

	return m.checkpointFunc(ctx, position)
}

// UpdatePosition updates the current binlog position.
// This should be called by the replication stream handler after processing each event.
func (m *MySQLReplicationSourceDetails) UpdatePosition(file string, position uint32) {
	m.positionMutex.Lock()
	m.BinlogFile = file
	m.BinlogPosition = position
	m.positionMutex.Unlock()
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (m *MySQLReplicationSourceDetails) SetCheckpointFunc(fn func(context.Context, string) error) {
	m.checkpointFunc = fn
}
