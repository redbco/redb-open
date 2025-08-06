package stores

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// PostgresStableStore implements the StableStore interface using PostgreSQL
type PostgresStableStore struct {
	pool    *pgxpool.Pool
	logger  *logger.Logger
	groupID string
	mu      sync.RWMutex
}

// NewPostgresStableStore creates a new PostgreSQL-based stable store
func NewPostgresStableStore(db *database.PostgreSQL, logger *logger.Logger, groupID string) (*PostgresStableStore, error) {
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

	store := &PostgresStableStore{
		pool:    pool,
		logger:  logger,
		groupID: groupID,
	}

	// Initialize the table
	if err := store.initializeTable(); err != nil {
		return nil, fmt.Errorf("failed to initialize stable store table: %v", err)
	}

	return store, nil
}

// initializeTable creates the stable store table if it doesn't exist
func (s *PostgresStableStore) initializeTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS raft_stable_store (
			id BIGSERIAL PRIMARY KEY,
			group_id VARCHAR(255) NOT NULL,
			key_name VARCHAR(255) NOT NULL,
			value BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(group_id, key_name)
		);
		
		CREATE INDEX IF NOT EXISTS idx_raft_stable_store_group_key ON raft_stable_store(group_id, key_name);
	`

	ctx := context.Background()
	_, err := s.pool.Exec(ctx, query)
	return err
}

// SetUint64 sets a key to a uint64 value
func (s *PostgresStableStore) SetUint64(key []byte, val uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		INSERT INTO raft_stable_store (group_id, key_name, value)
		VALUES ($1, $2, $3)
		ON CONFLICT (group_id, key_name) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = CURRENT_TIMESTAMP
	`

	ctx := context.Background()
	_, err := s.pool.Exec(ctx, query, s.groupID, string(key), val)
	if err != nil {
		return fmt.Errorf("failed to set uint64 for key %s: %v", string(key), err)
	}

	return nil
}

// GetUint64 gets a uint64 value for a key
func (s *PostgresStableStore) GetUint64(key []byte) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT value FROM raft_stable_store WHERE group_id = $1 AND key_name = $2`

	ctx := context.Background()
	var value uint64
	err := s.pool.QueryRow(ctx, query, s.groupID, string(key)).Scan(&value)

	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get uint64 for key %s: %v", string(key), err)
	}

	return value, nil
}

// Set sets a key to a value
func (s *PostgresStableStore) Set(key []byte, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		INSERT INTO raft_stable_store (group_id, key_name, value)
		VALUES ($1, $2, $3)
		ON CONFLICT (group_id, key_name) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = CURRENT_TIMESTAMP
	`

	ctx := context.Background()
	_, err := s.pool.Exec(ctx, query, s.groupID, string(key), val)
	if err != nil {
		return fmt.Errorf("failed to set value for key %s: %v", string(key), err)
	}

	return nil
}

// Get gets a value for a key
func (s *PostgresStableStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT value FROM raft_stable_store WHERE group_id = $1 AND key_name = $2`

	ctx := context.Background()
	var value []byte
	err := s.pool.QueryRow(ctx, query, s.groupID, string(key)).Scan(&value)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get value for key %s: %v", string(key), err)
	}

	return value, nil
}
