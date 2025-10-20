package timescaledb

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// ReplicationOps implements replication operations for TimescaleDB.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether CDC/replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	// TimescaleDB uses PostgreSQL logical replication
	return true
}

// GetSupportedMechanisms returns the list of supported CDC mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"logical_replication", "pgoutput", "wal2json", "decoderbufs"}
}

// CheckPrerequisites checks if prerequisites for CDC are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Check if logical replication is enabled
	var walLevel string
	err := r.conn.db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel)
	if err != nil {
		return fmt.Errorf("failed to check wal_level: %w", err)
	}

	if walLevel != "logical" {
		return fmt.Errorf("wal_level must be set to 'logical' for CDC, current value: %s", walLevel)
	}

	// Check if max_replication_slots > 0
	var maxSlots int
	err = r.conn.db.QueryRowContext(ctx, "SHOW max_replication_slots").Scan(&maxSlots)
	if err != nil {
		return fmt.Errorf("failed to check max_replication_slots: %w", err)
	}

	if maxSlots == 0 {
		return fmt.Errorf("max_replication_slots must be > 0 for CDC")
	}

	return nil
}

// Connect establishes a CDC connection.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	return nil, fmt.Errorf("CDC connection not yet implemented for TimescaleDB")
}

// GetStatus returns the CDC status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	status := map[string]interface{}{
		"supported": true,
		"mechanism": "postgresql_logical_replication",
	}

	// Check WAL level
	var walLevel string
	err := r.conn.db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel)
	if err == nil {
		status["wal_level"] = walLevel
		status["enabled"] = walLevel == "logical"
	}

	// Get replication slot count
	var slotCount int
	err = r.conn.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_replication_slots").Scan(&slotCount)
	if err == nil {
		status["replication_slot_count"] = slotCount
	}

	return status, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	query := `
		SELECT 
			slot_name,
			COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0) AS lag_bytes
		FROM pg_replication_slots
		WHERE slot_type = 'logical'
	`

	rows, err := r.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get replication lag: %w", err)
	}
	defer rows.Close()

	lagInfo := make(map[string]interface{})
	slots := make([]map[string]interface{}, 0)

	for rows.Next() {
		var slotName string
		var lagBytes int64

		if err := rows.Scan(&slotName, &lagBytes); err != nil {
			continue
		}

		slots = append(slots, map[string]interface{}{
			"slot_name": slotName,
			"lag_bytes": lagBytes,
		})
	}

	lagInfo["slots"] = slots
	return lagInfo, rows.Err()
}

// ListSlots lists replication slots.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			slot_name,
			plugin,
			slot_type,
			database,
			active,
			restart_lsn,
			confirmed_flush_lsn
		FROM pg_replication_slots
		ORDER BY slot_name
	`

	rows, err := r.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list replication slots: %w", err)
	}
	defer rows.Close()

	var slots []map[string]interface{}

	for rows.Next() {
		var slotName, plugin, slotType, database string
		var active bool
		var restartLsn, confirmedFlushLsn *string

		if err := rows.Scan(&slotName, &plugin, &slotType, &database, &active, &restartLsn, &confirmedFlushLsn); err != nil {
			continue
		}

		slot := map[string]interface{}{
			"slot_name": slotName,
			"plugin":    plugin,
			"slot_type": slotType,
			"database":  database,
			"active":    active,
		}

		if restartLsn != nil {
			slot["restart_lsn"] = *restartLsn
		}

		if confirmedFlushLsn != nil {
			slot["confirmed_flush_lsn"] = *confirmedFlushLsn
		}

		slots = append(slots, slot)
	}

	return slots, rows.Err()
}

// DropSlot drops a replication slot.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	query := fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName)
	_, err := r.conn.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop replication slot: %w", err)
	}
	return nil
}

// ListPublications lists publications.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			pubname,
			puballtables,
			pubinsert,
			pubupdate,
			pubdelete,
			pubtruncate
		FROM pg_publication
		ORDER BY pubname
	`

	rows, err := r.conn.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list publications: %w", err)
	}
	defer rows.Close()

	var publications []map[string]interface{}

	for rows.Next() {
		var pubName string
		var pubAllTables, pubInsert, pubUpdate, pubDelete, pubTruncate bool

		if err := rows.Scan(&pubName, &pubAllTables, &pubInsert, &pubUpdate, &pubDelete, &pubTruncate); err != nil {
			continue
		}

		pub := map[string]interface{}{
			"name":         pubName,
			"all_tables":   pubAllTables,
			"pub_insert":   pubInsert,
			"pub_update":   pubUpdate,
			"pub_delete":   pubDelete,
			"pub_truncate": pubTruncate,
		}

		publications = append(publications, pub)
	}

	return publications, rows.Err()
}

// DropPublication drops a publication.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	query := fmt.Sprintf("DROP PUBLICATION %s", publicationName)
	_, err := r.conn.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}
	return nil
}

// ParseEvent parses a CDC event (not yet implemented).
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
	return nil, fmt.Errorf("ParseEvent not yet implemented for TimescaleDB")
}

// ApplyCDCEvent applies a CDC event (not yet implemented).
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
	return fmt.Errorf("ApplyCDCEvent not yet implemented for TimescaleDB")
}

// TransformData transforms data using transformation rules.
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("TransformData not yet implemented for TimescaleDB")
}
