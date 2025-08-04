package stores

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// PostgresLogStore implements the LogStore interface using PostgreSQL
type PostgresLogStore struct {
	pool    *pgxpool.Pool
	logger  *logger.Logger
	groupID string
	mu      sync.RWMutex
}

// NewPostgresLogStore creates a new PostgreSQL-based log store
func NewPostgresLogStore(db *database.PostgreSQL, logger *logger.Logger, groupID string) (*PostgresLogStore, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if groupID == "" {
		return nil, fmt.Errorf("group ID is required")
	}

	pool := db.Pool()
	if pool == nil {
		return nil, fmt.Errorf("database pool is nil")
	}

	store := &PostgresLogStore{
		pool:    pool,
		logger:  logger,
		groupID: groupID,
	}

	// Initialize the table
	if err := store.initializeTable(); err != nil {
		return nil, fmt.Errorf("failed to initialize log store table: %v", err)
	}

	return store, nil
}

// initializeTable creates the log store table if it doesn't exist
func (s *PostgresLogStore) initializeTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS raft_logs (
			id BIGSERIAL PRIMARY KEY,
			group_id VARCHAR(255) NOT NULL,
			log_index BIGINT NOT NULL,
			log_term BIGINT NOT NULL,
			log_type SMALLINT NOT NULL,
			log_data BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(group_id, log_index)
		);
		
		CREATE INDEX IF NOT EXISTS idx_raft_logs_group_index ON raft_logs(group_id, log_index);
		CREATE INDEX IF NOT EXISTS idx_raft_logs_group_term ON raft_logs(group_id, log_term);
	`

	ctx := context.Background()
	_, err := s.pool.Exec(ctx, query)
	return err
}

// FirstIndex returns the first index written
func (s *PostgresLogStore) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT MIN(log_index) FROM raft_logs WHERE group_id = $1`

	ctx := context.Background()
	var index *uint64
	err := s.pool.QueryRow(ctx, query, s.groupID).Scan(&index)

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get first index: %v", err)
	}

	if index == nil {
		return 0, nil
	}

	return *index, nil
}

// LastIndex returns the last index written
func (s *PostgresLogStore) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT MAX(log_index) FROM raft_logs WHERE group_id = $1`

	ctx := context.Background()
	var index *uint64
	err := s.pool.QueryRow(ctx, query, s.groupID).Scan(&index)

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last index: %v", err)
	}

	if err == pgx.ErrNoRows {
		return 0, nil
	}

	return *index, nil
}

// GetLog gets a log entry at a given index
func (s *PostgresLogStore) GetLog(index uint64, log *raft.Log) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT log_index, log_term, log_type, log_data FROM raft_logs WHERE group_id = $1 AND log_index = $2`

	ctx := context.Background()
	var logIndex, logTerm uint64
	var logType int16
	var logData []byte

	err := s.pool.QueryRow(ctx, query, s.groupID, index).Scan(&logIndex, &logTerm, &logType, &logData)
	if err != nil {
		if err == pgx.ErrNoRows {
			return raft.ErrLogNotFound
		}
		return fmt.Errorf("failed to get log at index %d: %v", index, err)
	}

	log.Index = logIndex
	log.Term = logTerm
	log.Type = raft.LogType(logType)
	log.Data = logData

	return nil
}

// StoreLog stores a log entry
func (s *PostgresLogStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries
func (s *PostgresLogStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	query := `
		INSERT INTO raft_logs (group_id, log_index, log_term, log_type, log_data)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (group_id, log_index) DO UPDATE SET
			log_term = EXCLUDED.log_term,
			log_type = EXCLUDED.log_type,
			log_data = EXCLUDED.log_data
	`

	for _, log := range logs {
		_, err := tx.Exec(ctx, query, s.groupID, log.Index, log.Term, int16(log.Type), log.Data)
		if err != nil {
			return fmt.Errorf("failed to store log at index %d: %v", log.Index, err)
		}
	}

	return tx.Commit(ctx)
}

// DeleteRange deletes a range of log entries
func (s *PostgresLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `DELETE FROM raft_logs WHERE group_id = $1 AND log_index >= $2 AND log_index <= $3`

	ctx := context.Background()
	_, err := s.pool.Exec(ctx, query, s.groupID, min, max)
	if err != nil {
		return fmt.Errorf("failed to delete logs from %d to %d: %v", min, max, err)
	}

	return nil
}
