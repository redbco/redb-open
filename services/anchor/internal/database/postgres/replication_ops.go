package postgres

import (
	"context"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// ReplicationOps implements adapter.ReplicationOperator for PostgreSQL.
type ReplicationOps struct {
	conn *Connection
}

// IsSupported returns whether replication is supported.
func (r *ReplicationOps) IsSupported() bool {
	return true
}

// GetSupportedMechanisms returns the supported replication mechanisms.
func (r *ReplicationOps) GetSupportedMechanisms() []string {
	return []string{"logical_decoding", "wal2json", "pgoutput"}
}

// CheckPrerequisites checks if replication prerequisites are met.
func (r *ReplicationOps) CheckPrerequisites(ctx context.Context) error {
	// Use existing CheckLogicalReplicationPrerequisites function
	err := CheckLogicalReplicationPrerequisites(r.conn.pool, nil)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "check_replication_prerequisites", err)
	}
	return nil
}

// Connect creates a new replication connection.
func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationSource, error) {
	// Convert adapter config to legacy config
	legacyConfig := dbclient.ReplicationConfig{
		ReplicationID:      config.ReplicationID,
		DatabaseID:         config.DatabaseID,
		WorkspaceID:        config.WorkspaceID,
		TenantID:           config.TenantID,
		EnvironmentID:      adapter.GetString(config.EnvironmentID),
		ReplicationName:    config.ReplicationName,
		ConnectionType:     config.ConnectionType,
		DatabaseVendor:     config.DatabaseVendor,
		Host:               config.Host,
		Port:               config.Port,
		Username:           config.Username,
		Password:           config.Password,
		DatabaseName:       config.DatabaseName,
		SSL:                config.SSL,
		SSLMode:            config.SSLMode,
		SSLCert:            config.SSLCert,
		SSLKey:             config.SSLKey,
		SSLRootCert:        config.SSLRootCert,
		Role:               config.Role,
		Enabled:            config.Enabled,
		ConnectedToNodeID:  config.ConnectedToNodeID,
		OwnerID:            config.OwnerID,
		TableNames:         config.TableNames,
		SlotName:           config.SlotName,
		PublicationName:    config.PublicationName,
		EventHandler:       config.EventHandler,
		ReplicationOptions: config.Options,
	}

	// Use existing ConnectReplication function
	client, source, err := ConnectReplication(legacyConfig)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "connect_replication", err)
	}

	// Type assert to PostgresReplicationSourceDetails
	pgSource, ok := source.(*PostgresReplicationSourceDetails)
	if !ok {
		return nil, adapter.NewDatabaseError(
			dbcapabilities.PostgreSQL,
			"connect_replication",
			adapter.ErrInvalidConfiguration,
		).WithContext("error", "invalid replication source type")
	}

	return &ReplicationSource{
		client: client,
		source: pgSource,
	}, nil
}

// GetStatus returns the replication status.
func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	// Use existing GetReplicationStatus function
	status, err := GetReplicationStatus(r.conn.pool, r.conn.id)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "get_replication_status", err)
	}
	return status, nil
}

// GetLag returns the replication lag.
func (r *ReplicationOps) GetLag(ctx context.Context) (map[string]interface{}, error) {
	// Use existing GetReplicationLag function
	lag, err := GetReplicationLag(r.conn.pool, r.conn.id)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "get_replication_lag", err)
	}
	return lag, nil
}

// ListSlots lists all replication slots.
func (r *ReplicationOps) ListSlots(ctx context.Context) ([]map[string]interface{}, error) {
	// Use existing ListReplicationSlots function
	slots, err := ListReplicationSlots(r.conn.pool, r.conn.id)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_replication_slots", err)
	}
	return slots, nil
}

// DropSlot drops a replication slot.
func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
	// Use existing DropReplicationSlot function
	err := DropReplicationSlot(r.conn.pool, slotName)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "drop_replication_slot", err)
	}
	return nil
}

// ListPublications lists all publications.
func (r *ReplicationOps) ListPublications(ctx context.Context) ([]map[string]interface{}, error) {
	// Use existing ListPublications function
	publications, err := ListPublications(r.conn.pool)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_publications", err)
	}
	return publications, nil
}

// DropPublication drops a publication.
func (r *ReplicationOps) DropPublication(ctx context.Context, publicationName string) error {
	// Use existing DropPublication function
	err := DropPublication(r.conn.pool, publicationName)
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "drop_publication", err)
	}
	return nil
}

// ReplicationSource implements adapter.ReplicationSource for PostgreSQL.
type ReplicationSource struct {
	client *dbclient.ReplicationClient
	source *PostgresReplicationSourceDetails
}

// GetSourceID returns the replication source ID.
func (r *ReplicationSource) GetSourceID() string {
	return r.source.GetSourceID()
}

// GetDatabaseID returns the database ID.
func (r *ReplicationSource) GetDatabaseID() string {
	return r.source.GetDatabaseID()
}

// GetStatus returns the replication source status.
func (r *ReplicationSource) GetStatus() map[string]interface{} {
	return r.source.GetStatus()
}

// GetMetadata returns the replication source metadata.
func (r *ReplicationSource) GetMetadata() map[string]interface{} {
	return r.source.GetMetadata()
}

// IsActive returns whether the replication source is active.
func (r *ReplicationSource) IsActive() bool {
	return r.source.IsActive()
}

// Start starts the replication source.
func (r *ReplicationSource) Start() error {
	return r.source.Start()
}

// Stop stops the replication source.
func (r *ReplicationSource) Stop() error {
	return r.source.Stop()
}

// Close closes the replication source.
func (r *ReplicationSource) Close() error {
	return r.source.Close()
}
