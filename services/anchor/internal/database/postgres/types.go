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

type PostgresReplicationChange struct {
	Operation string                 `json:"operation"`
	TableName string                 `json:"table_name"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
}
