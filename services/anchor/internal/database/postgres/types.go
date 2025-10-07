package postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redbco/redb-open/pkg/logger"
)

type PostgresReplicationSourceDetails struct {
	SlotName        string                                `json:"slot_name"`
	PublicationName string                                `json:"publication_name"`
	DatabaseID      string                                `json:"database_id"`
	ReplicationConn *pgconn.PgConn                        `json:"-"`
	StopChan        chan struct{}                         `json:"-"`
	isActive        bool                                  `json:"-"`
	EventHandler    func(map[string]interface{})          `json:"-"`
	TableNames      map[string]struct{}                   `json:"table_names"` // Set of tables being replicated
	logger          *logger.Logger                        `json:"-"`
	relations       map[uint32]*pglogrepl.RelationMessage `json:"-"` // Cache of relation metadata by relation ID
	relationsMutex  sync.RWMutex                          `json:"-"` // Protects relations map

	// LSN tracking for graceful shutdown and resume
	currentLSN     pglogrepl.LSN                       `json:"-"` // Current replication position
	startLSN       pglogrepl.LSN                       `json:"-"` // Starting replication position (for resume)
	lsnMutex       sync.RWMutex                        `json:"-"` // Protects LSN access
	checkpointFunc func(context.Context, string) error `json:"-"` // Callback to persist checkpoint
}

// AddTable adds a table to the replication source
func (p *PostgresReplicationSourceDetails) AddTable(table string) {
	if p.TableNames == nil {
		p.TableNames = make(map[string]struct{})
	}
	p.TableNames[table] = struct{}{}
}

// RemoveTable removes a table from the replication source
func (p *PostgresReplicationSourceDetails) RemoveTable(table string) {
	if p.TableNames != nil {
		delete(p.TableNames, table)
	}
}

// HasTable checks if the replication source is replicating a given table
func (p *PostgresReplicationSourceDetails) HasTable(table string) bool {
	_, ok := p.TableNames[table]
	return ok
}

// GetTables returns a slice of all tables being replicated
func (p *PostgresReplicationSourceDetails) GetTables() []string {
	tables := make([]string, 0, len(p.TableNames))
	for t := range p.TableNames {
		tables = append(tables, t)
	}
	return tables
}

// Implement ReplicationSourceInterface
func (p *PostgresReplicationSourceDetails) GetSourceID() string {
	return p.SlotName
}

func (p *PostgresReplicationSourceDetails) GetDatabaseID() string {
	return p.DatabaseID
}

func (p *PostgresReplicationSourceDetails) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"slot_name":        p.SlotName,
		"publication_name": p.PublicationName,
		"table_names":      p.GetTables(),
		"database_id":      p.DatabaseID,
		"is_active":        p.isActive,
		"has_connection":   p.ReplicationConn != nil,
	}
}

func (p *PostgresReplicationSourceDetails) Start() error {
	if p.ReplicationConn == nil {
		return fmt.Errorf("no replication connection available")
	}

	if p.isActive {
		return nil // Already active
	}

	// Start the replication streaming for all tables with logger
	go streamReplicationEvents(p.ReplicationConn, p, p.EventHandler, p.logger)
	p.isActive = true
	return nil
}

func (p *PostgresReplicationSourceDetails) Stop() error {
	if !p.isActive {
		return nil // Already stopped
	}

	if p.StopChan != nil {
		close(p.StopChan)
		p.StopChan = make(chan struct{}) // Create new channel for future use
	}

	p.isActive = false
	return nil
}

func (p *PostgresReplicationSourceDetails) IsActive() bool {
	return p.isActive
}

func (p *PostgresReplicationSourceDetails) SetLogger(log *logger.Logger) {
	p.logger = log
}

func (p *PostgresReplicationSourceDetails) GetMetadata() map[string]interface{} {
	return map[string]interface{}{
		"slot_name":        p.SlotName,
		"publication_name": p.PublicationName,
		"table_names":      p.GetTables(),
		"database_id":      p.DatabaseID,
	}
}

func (p *PostgresReplicationSourceDetails) Close() error {
	// Stop replication first
	if err := p.Stop(); err != nil {
		return fmt.Errorf("failed to stop replication: %w", err)
	}

	// Close the replication connection
	if p.ReplicationConn != nil {
		p.ReplicationConn.Close(context.Background())
		p.ReplicationConn = nil
	}

	return nil
}

// GetPosition returns the current LSN as a string.
func (p *PostgresReplicationSourceDetails) GetPosition() (string, error) {
	p.lsnMutex.RLock()
	defer p.lsnMutex.RUnlock()

	if p.currentLSN == 0 {
		return "", fmt.Errorf("no LSN position available")
	}

	return p.currentLSN.String(), nil
}

// SetPosition sets the starting LSN for replication resume.
// The position string should be in PostgreSQL LSN format (e.g., "0/12345678").
func (p *PostgresReplicationSourceDetails) SetPosition(position string) error {
	if position == "" {
		return nil // No position to set, will start from beginning
	}

	lsn, err := pglogrepl.ParseLSN(position)
	if err != nil {
		return fmt.Errorf("invalid LSN position %q: %w", position, err)
	}

	p.lsnMutex.Lock()
	p.startLSN = lsn
	p.currentLSN = lsn
	p.lsnMutex.Unlock()

	if p.logger != nil {
		p.logger.Info("Set replication start position to LSN %s for slot %s", position, p.SlotName)
	}

	return nil
}

// SaveCheckpoint persists the current replication position.
func (p *PostgresReplicationSourceDetails) SaveCheckpoint(ctx context.Context, position string) error {
	if p.checkpointFunc == nil {
		// No checkpoint function configured - log warning but don't error
		if p.logger != nil {
			p.logger.Warn("No checkpoint function configured for slot %s, position will not be persisted", p.SlotName)
		}
		return nil
	}

	return p.checkpointFunc(ctx, position)
}

// UpdateLSN updates the current LSN position.
// This should be called by the replication stream handler after processing each message.
func (p *PostgresReplicationSourceDetails) UpdateLSN(lsn pglogrepl.LSN) {
	p.lsnMutex.Lock()
	p.currentLSN = lsn
	p.lsnMutex.Unlock()
}

// SetCheckpointFunc sets the callback function for persisting checkpoints.
func (p *PostgresReplicationSourceDetails) SetCheckpointFunc(fn func(context.Context, string) error) {
	p.checkpointFunc = fn
}

type PostgresReplicationChange struct {
	Operation string                 `json:"operation"`
	TableName string                 `json:"table_name"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}
