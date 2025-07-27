package config

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/syslog"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"google.golang.org/grpc"
)

type Repository struct {
	db             *database.PostgreSQL
	databaseClient corev1.DatabaseServiceClient
	instanceClient corev1.InstanceServiceClient
	commitClient   corev1.CommitServiceClient
}

func NewRepository(db *database.PostgreSQL, conn *grpc.ClientConn) *Repository {
	return &Repository{
		db:             db,
		databaseClient: corev1.NewDatabaseServiceClient(conn),
		instanceClient: corev1.NewInstanceServiceClient(conn),
		commitClient:   corev1.NewCommitServiceClient(conn),
	}
}

// NewDatabaseOnlyRepository creates a repository that only uses direct database access
// Example usage:
//
//	ctx := context.Background()
//	cfg := database.FromGlobalConfig(globalConfig)
//	db, err := database.New(ctx, cfg)
//	if err != nil { return err }
//	repo := NewDatabaseOnlyRepository(db)
//	configs, err := repo.GetAllDatabaseConfigs(ctx, nodeID)
func NewDatabaseOnlyRepository(db *database.PostgreSQL) *Repository {
	return &Repository{
		db: db,
	}
}

// GetAllDatabaseConfigs retrieves all enabled database configurations from internal database
func (r *Repository) GetAllDatabaseConfigs(ctx context.Context, nodeID string) ([]common.UnifiedDatabaseConfig, error) {
	syslog.Info("anchor", "Getting all database configurations from internal database")

	query := `
		SELECT 
			d.database_id,
			d.tenant_id,
			d.workspace_id,
			d.environment_id,
			d.connected_to_node_id,
			d.instance_id,
			d.database_name,
			d.database_description,
			d.database_type,
			d.database_vendor,
			d.database_version,
			d.database_username,
			d.database_password,
			d.database_db_name,
			d.database_enabled,
			d.policy_ids,
			d.owner_id,
			d.database_status_message,
			d.status,
			d.created,
			d.updated,
			i.instance_host,
			i.instance_port,
			i.instance_ssl_mode,
			i.instance_ssl_cert,
			i.instance_ssl_key,
			i.instance_ssl_root_cert,
			i.instance_ssl
		FROM databases d
		LEFT JOIN instances i ON d.instance_id = i.instance_id
		WHERE d.connected_to_node_id = $1 AND d.database_enabled = true
	`

	rows, err := r.db.Pool().Query(ctx, query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("error querying databases: %w", err)
	}
	defer rows.Close()

	var configs []common.UnifiedDatabaseConfig
	for rows.Next() {
		var config common.UnifiedDatabaseConfig
		var policyIDs []string // pgx can scan PostgreSQL arrays directly into Go slices

		err := rows.Scan(
			&config.DatabaseID,
			&config.TenantID,
			&config.WorkspaceID,
			&config.EnvironmentID,
			&config.ConnectedToNodeID,
			&config.InstanceID,
			&config.Name,
			&config.Description,
			&config.Type,
			&config.Vendor,
			&config.Version,
			&config.Username,
			&config.Password,
			&config.DatabaseName,
			&config.Enabled,
			&policyIDs, // pgx handles PostgreSQL arrays automatically
			&config.OwnerID,
			&config.StatusMessage,
			&config.Status,
			&config.Created,
			&config.Updated,
			&config.Host,
			&config.Port,
			&config.SSLMode,
			&config.SSLCert,
			&config.SSLKey,
			&config.SSLRootCert,
			&config.SSL,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning database row: %w", err)
		}

		// Handle potential nil policy IDs array
		if policyIDs == nil {
			config.PolicyIDs = []string{}
		} else {
			config.PolicyIDs = policyIDs
		}
		configs = append(configs, config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating database rows: %w", err)
	}

	syslog.Info("anchor", "Found %d database configurations", len(configs))

	return configs, nil
}

// GetAllInstanceConfigs retrieves all enabled instance configurations from internal database
func (r *Repository) GetAllInstanceConfigs(ctx context.Context, nodeID string) ([]common.UnifiedInstanceConfig, error) {
	syslog.Info("anchor", "Getting all instance configurations from internal database")

	query := `
		SELECT 
			instance_id,
			tenant_id,
			workspace_id,
			environment_id,
			connected_to_node_id,
			instance_name,
			instance_description,
			instance_type,
			instance_vendor,
			instance_version,
			instance_unique_identifier,
			instance_host,
			instance_port,
			instance_username,
			instance_password,
			instance_system_db_name,
			instance_enabled,
			instance_ssl,
			instance_ssl_mode,
			instance_ssl_cert,
			instance_ssl_key,
			instance_ssl_root_cert,
			policy_ids,
			owner_id,
			instance_status_message,
			status,
			created,
			updated
		FROM instances
		WHERE connected_to_node_id = $1 AND instance_enabled = true
	`

	rows, err := r.db.Pool().Query(ctx, query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("error querying instances: %w", err)
	}
	defer rows.Close()

	var configs []common.UnifiedInstanceConfig
	for rows.Next() {
		var config common.UnifiedInstanceConfig
		var policyIDs []string // pgx can scan PostgreSQL arrays directly into Go slices

		err := rows.Scan(
			&config.InstanceID,
			&config.TenantID,
			&config.WorkspaceID,
			&config.EnvironmentID,
			&config.ConnectedToNodeID,
			&config.Name,
			&config.Description,
			&config.Type,
			&config.Vendor,
			&config.Version,
			&config.UniqueIdentifier,
			&config.Host,
			&config.Port,
			&config.Username,
			&config.Password,
			&config.DatabaseName,
			&config.Enabled,
			&config.SSL,
			&config.SSLMode,
			&config.SSLCert,
			&config.SSLKey,
			&config.SSLRootCert,
			&policyIDs, // pgx handles PostgreSQL arrays automatically
			&config.OwnerID,
			&config.StatusMessage,
			&config.Status,
			&config.Created,
			&config.Updated,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning instance row: %w", err)
		}

		// Handle potential nil policy IDs array
		if policyIDs == nil {
			config.PolicyIDs = []string{}
		} else {
			config.PolicyIDs = policyIDs
		}
		configs = append(configs, config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating instance rows: %w", err)
	}

	syslog.Info("anchor", "Found %d instance configurations", len(configs))

	return configs, nil
}

// UpdateDatabaseConnectionStatus updates the connection status for a database
func (r *Repository) UpdateDatabaseConnectionStatus(ctx context.Context, databaseID string, connected bool, status string) error {
	syslog.Info("anchor", "Updating database connection status for database %s: connected=%t, status=%s", databaseID, connected, status)

	// Determine the status value based on connection state
	var dbStatus string
	if connected {
		dbStatus = "STATUS_CONNECTED"
	} else {
		dbStatus = "STATUS_DISCONNECTED"
	}

	query := `
		UPDATE databases 
		SET 
			status = $1,
			database_status_message = $2,
			updated = CURRENT_TIMESTAMP
		WHERE database_id = $3
	`

	result, err := r.db.Pool().Exec(ctx, query, dbStatus, status, databaseID)
	if err != nil {
		return fmt.Errorf("error updating database connection status: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("database with ID %s not found", databaseID)
	}

	syslog.Info("anchor", "Successfully updated database connection status for database %s", databaseID)
	return nil
}

// UpdateInstanceConnectionStatus updates the connection status for an instance
func (r *Repository) UpdateInstanceConnectionStatus(ctx context.Context, instanceID string, connected bool, status string) error {
	syslog.Info("anchor", "Updating instance connection status for instance %s: connected=%t, status=%s", instanceID, connected, status)

	// Determine the status value based on connection state
	var instStatus string
	if connected {
		instStatus = "STATUS_CONNECTED"
	} else {
		instStatus = "STATUS_DISCONNECTED"
	}

	query := `
		UPDATE instances 
		SET 
			status = $1,
			instance_status_message = $2,
			updated = CURRENT_TIMESTAMP
		WHERE instance_id = $3
	`

	result, err := r.db.Pool().Exec(ctx, query, instStatus, status, instanceID)
	if err != nil {
		return fmt.Errorf("error updating instance connection status: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("instance with ID %s not found", instanceID)
	}

	syslog.Info("anchor", "Successfully updated instance connection status for instance %s", instanceID)
	return nil
}

// UpdateDatabaseMetadata updates the database metadata for a database
func (r *Repository) UpdateDatabaseMetadata(ctx context.Context, metadata *DatabaseMetadata) error {
	syslog.Info("anchor", "Updating database metadata for database %s", metadata.DatabaseID)

	// Marshal metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("error marshaling database metadata to JSON: %w", err)
	}

	query := `
		UPDATE databases 
		SET 
			database_metadata = $1,
			updated = CURRENT_TIMESTAMP
		WHERE database_id = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, metadataJSON, metadata.DatabaseID)
	if err != nil {
		return fmt.Errorf("error updating database metadata: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("database with ID %s not found", metadata.DatabaseID)
	}

	syslog.Info("anchor", "Successfully updated database metadata for database %s", metadata.DatabaseID)
	return nil
}

// UpdateInstanceMetadata updates the instance metadata for an instance
func (r *Repository) UpdateInstanceMetadata(ctx context.Context, metadata *InstanceMetadata) error {
	syslog.Info("anchor", "Updating instance metadata for instance %s", metadata.InstanceID)

	// Marshal metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("error marshaling instance metadata to JSON: %w", err)
	}

	query := `
		UPDATE instances 
		SET 
			instance_metadata = $1,
			updated = CURRENT_TIMESTAMP
		WHERE instance_id = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, metadataJSON, metadata.InstanceID)
	if err != nil {
		return fmt.Errorf("error updating instance metadata: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("instance with ID %s not found", metadata.InstanceID)
	}

	syslog.Info("anchor", "Successfully updated instance metadata for instance %s", metadata.InstanceID)
	return nil
}

// GetDatabaseConfigByID retrieves a database configuration by its ID
func (r *Repository) GetDatabaseConfigByID(ctx context.Context, databaseID string) (*common.UnifiedDatabaseConfig, error) {
	syslog.Info("anchor", "Getting database configuration by ID %s", databaseID)

	query := `
		SELECT 
			d.database_id,
			d.tenant_id,
			d.workspace_id,
			d.environment_id,
			d.connected_to_node_id,
			d.instance_id,
			d.database_name,
			d.database_description,
			d.database_type,
			d.database_vendor,
			d.database_version,
			d.database_username,
			d.database_password,
			d.database_db_name,
			d.database_enabled,
			d.policy_ids,
			d.owner_id,
			d.database_status_message,
			d.status,
			d.created,
			d.updated,
			i.instance_host,
			i.instance_port,
			i.instance_ssl_mode,
			i.instance_ssl_cert,
			i.instance_ssl_key,
			i.instance_ssl_root_cert,
			i.instance_ssl
		FROM databases d
		LEFT JOIN instances i ON d.instance_id = i.instance_id
		WHERE d.database_id = $1
	`

	rows, err := r.db.Pool().Query(ctx, query, databaseID)
	if err != nil {
		return nil, fmt.Errorf("error querying database configuration: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("database configuration with ID %s not found", databaseID)
	}

	var config common.UnifiedDatabaseConfig
	var policyIDs []string // pgx can scan PostgreSQL arrays directly into Go slices

	err = rows.Scan(
		&config.DatabaseID,
		&config.TenantID,
		&config.WorkspaceID,
		&config.EnvironmentID,
		&config.ConnectedToNodeID,
		&config.InstanceID,
		&config.Name,
		&config.Description,
		&config.Type,
		&config.Vendor,
		&config.Version,
		&config.Username,
		&config.Password,
		&config.DatabaseName,
		&config.Enabled,
		&policyIDs, // pgx handles PostgreSQL arrays automatically
		&config.OwnerID,
		&config.StatusMessage,
		&config.Status,
		&config.Created,
		&config.Updated,
		&config.Host,
		&config.Port,
		&config.SSLMode,
		&config.SSLCert,
		&config.SSLKey,
		&config.SSLRootCert,
		&config.SSL,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning database configuration: %w", err)
	}

	// Handle potential nil policy IDs array
	if policyIDs == nil {
		config.PolicyIDs = []string{}
	} else {
		config.PolicyIDs = policyIDs
	}

	syslog.Info("anchor", "Successfully retrieved database configuration by ID %s", databaseID)
	return &config, nil
}

// GetInstanceConfigByID retrieves an instance configuration by its ID
func (r *Repository) GetInstanceConfigByID(ctx context.Context, instanceID string) (*common.UnifiedInstanceConfig, error) {
	syslog.Info("anchor", "Getting instance configuration by ID %s", instanceID)

	query := `
		SELECT 
			instance_id,
			tenant_id,
			workspace_id,
			environment_id,
			connected_to_node_id,
			instance_name,
			instance_description,
			instance_type,
			instance_vendor,
			instance_version,
			instance_unique_identifier,
			instance_host,
			instance_port,
			instance_username,
			instance_password,
			instance_system_db_name,
			instance_enabled,
			instance_ssl,
			instance_ssl_mode,
			instance_ssl_cert,
			instance_ssl_key,
			instance_ssl_root_cert,
			policy_ids,
			owner_id,
			instance_status_message,
			status,
			created,
			updated
		FROM instances
		WHERE instance_id = $1
	`

	rows, err := r.db.Pool().Query(ctx, query, instanceID)
	if err != nil {
		return nil, fmt.Errorf("error querying instance configuration: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("instance configuration with ID %s not found", instanceID)
	}

	var config common.UnifiedInstanceConfig
	var policyIDs []string // pgx can scan PostgreSQL arrays directly into Go slices

	err = rows.Scan(
		&config.InstanceID,
		&config.TenantID,
		&config.WorkspaceID,
		&config.EnvironmentID,
		&config.ConnectedToNodeID,
		&config.Name,
		&config.Description,
		&config.Type,
		&config.Vendor,
		&config.Version,
		&config.UniqueIdentifier,
		&config.Host,
		&config.Port,
		&config.Username,
		&config.Password,
		&config.DatabaseName,
		&config.Enabled,
		&config.SSL,
		&config.SSLMode,
		&config.SSLCert,
		&config.SSLKey,
		&config.SSLRootCert,
		&policyIDs, // pgx handles PostgreSQL arrays automatically
		&config.OwnerID,
		&config.StatusMessage,
		&config.Status,
		&config.Created,
		&config.Updated,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning instance configuration: %w", err)
	}

	// Handle potential nil policy IDs array
	if policyIDs == nil {
		config.PolicyIDs = []string{}
	} else {
		config.PolicyIDs = policyIDs
	}

	syslog.Info("anchor", "Successfully retrieved instance configuration by ID %s", instanceID)
	return &config, nil
}

// LatestSchemaResponse is the response from the GetLatestStoredDatabaseSchema function
type LatestSchemaResponse struct {
	CommitExists            bool
	DatabaseConnectedToRepo bool
	Schema                  interface{}
}

// GetLatestStoredDatabaseSchema retrieves the latest stored database schema for a database
func (r *Repository) GetLatestStoredDatabaseSchema(ctx context.Context, databaseID string) (*LatestSchemaResponse, error) {
	syslog.Info("anchor", "Getting latest stored database schema for database %s", databaseID)

	// First check if the database is connected to any branch
	checkConnectionQuery := `
		SELECT 
			branches.branch_id,
			branches.connected_to_database
		FROM branches
		WHERE branches.connected_database_id = $1
			AND branches.connected_to_database = true
		LIMIT 1
	`

	connectionRows, err := r.db.Pool().Query(ctx, checkConnectionQuery, databaseID)
	if err != nil {
		return &LatestSchemaResponse{
			CommitExists:            false,
			DatabaseConnectedToRepo: false,
			Schema:                  nil,
		}, fmt.Errorf("error checking database connection to repository: %w", err)
	}
	defer connectionRows.Close()

	if !connectionRows.Next() {
		// Database is not connected to any repository/branch
		syslog.Info("anchor", "Database %s is not connected to any repository", databaseID)
		return &LatestSchemaResponse{
			CommitExists:            false,
			DatabaseConnectedToRepo: false,
			Schema:                  nil,
		}, nil
	}

	var branchID string
	var connectedToDatabase bool
	err = connectionRows.Scan(&branchID, &connectedToDatabase)
	if err != nil {
		return &LatestSchemaResponse{
			CommitExists:            false,
			DatabaseConnectedToRepo: false,
			Schema:                  nil,
		}, fmt.Errorf("error scanning branch connection: %w", err)
	}
	connectionRows.Close()

	// Now check for the latest commit in the connected branch
	commitQuery := `
		SELECT 
			commits.schema_structure
		FROM commits
		WHERE commits.branch_id = $1
			AND commits.commit_is_head = true
		ORDER BY commits.created DESC
		LIMIT 1
	`

	commitRows, err := r.db.Pool().Query(ctx, commitQuery, branchID)
	if err != nil {
		return &LatestSchemaResponse{
			CommitExists:            false,
			DatabaseConnectedToRepo: true,
			Schema:                  nil,
		}, fmt.Errorf("error querying latest stored database schema: %w", err)
	}
	defer commitRows.Close()

	if !commitRows.Next() {
		// Database is connected to repository but no commits exist
		syslog.Info("anchor", "Database %s is connected to repository but no commits found", databaseID)
		return &LatestSchemaResponse{
			CommitExists:            false,
			DatabaseConnectedToRepo: true,
			Schema:                  nil,
		}, nil
	}

	var schema interface{}
	err = commitRows.Scan(&schema)
	if err != nil {
		return &LatestSchemaResponse{
			CommitExists:            false,
			DatabaseConnectedToRepo: true,
			Schema:                  nil,
		}, fmt.Errorf("error scanning latest stored database schema: %w", err)
	}

	syslog.Info("anchor", "Successfully retrieved latest stored database schema for database %s", databaseID)
	return &LatestSchemaResponse{
		CommitExists:            true,
		DatabaseConnectedToRepo: true,
		Schema:                  schema,
	}, nil
}

// ReplicationSource represents a replication source in the database
type ReplicationSource struct {
	ReplicationSourceID string    `json:"replication_source_id"`
	TenantID            string    `json:"tenant_id"`
	WorkspaceID         string    `json:"workspace_id"`
	DatabaseID          string    `json:"database_id"`
	TableName           string    `json:"table_name"`
	RelationshipID      string    `json:"relationship_id"`
	PublicationName     string    `json:"publication_name"`
	SlotName            string    `json:"slot_name"`
	StatusMessage       string    `json:"status_message"`
	Status              string    `json:"status"`
	Created             time.Time `json:"created"`
	Updated             time.Time `json:"updated"`
}

// CreateReplicationSource creates a new replication source in the database
func (r *Repository) CreateReplicationSource(ctx context.Context, source *ReplicationSource) error {
	syslog.Info("anchor", "Creating replication source for database %s, table %s", source.DatabaseID, source.TableName)

	query := `
		INSERT INTO replication_sources (
			replication_source_id,
			tenant_id,
			workspace_id,
			database_id,
			table_name,
			relationship_id,
			publication_name,
			slot_name,
			status_message,
			status,
			created,
			updated
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`

	_, err := r.db.Pool().Exec(ctx, query,
		source.ReplicationSourceID,
		source.TenantID,
		source.WorkspaceID,
		source.DatabaseID,
		source.TableName,
		source.RelationshipID,
		source.PublicationName,
		source.SlotName,
		source.StatusMessage,
		source.Status,
	)
	if err != nil {
		return fmt.Errorf("error creating replication source: %w", err)
	}

	syslog.Info("anchor", "Successfully created replication source for database %s, table %s", source.DatabaseID, source.TableName)
	return nil
}

// GetReplicationSource retrieves a replication source by ID
func (r *Repository) GetReplicationSource(ctx context.Context, replicationSourceID string) (*ReplicationSource, error) {
	syslog.Info("anchor", "Getting replication source by ID %s", replicationSourceID)

	query := `
		SELECT 
			replication_source_id,
			tenant_id,
			workspace_id,
			database_id,
			table_name,
			relationship_id,
			publication_name,
			slot_name,
			status_message,
			status,
			created,
			updated
		FROM replication_sources
		WHERE replication_source_id = $1
	`

	row := r.db.Pool().QueryRow(ctx, query, replicationSourceID)

	var source ReplicationSource
	err := row.Scan(
		&source.ReplicationSourceID,
		&source.TenantID,
		&source.WorkspaceID,
		&source.DatabaseID,
		&source.TableName,
		&source.RelationshipID,
		&source.PublicationName,
		&source.SlotName,
		&source.StatusMessage,
		&source.Status,
		&source.Created,
		&source.Updated,
	)
	if err != nil {
		return nil, fmt.Errorf("error getting replication source: %w", err)
	}

	syslog.Info("anchor", "Successfully retrieved replication source by ID %s", replicationSourceID)
	return &source, nil
}

// GetAllReplicationSources retrieves all replication sources for a workspace
func (r *Repository) GetAllReplicationSources(ctx context.Context, workspaceID string) ([]*ReplicationSource, error) {
	syslog.Info("anchor", "Getting all replication sources for workspace %s", workspaceID)

	query := `
		SELECT 
			replication_source_id,
			tenant_id,
			workspace_id,
			database_id,
			table_name,
			relationship_id,
			publication_name,
			slot_name,
			status_message,
			status,
			created,
			updated
		FROM replication_sources
		WHERE workspace_id = $1
		ORDER BY created DESC
	`

	rows, err := r.db.Pool().Query(ctx, query, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("error querying replication sources: %w", err)
	}
	defer rows.Close()

	var sources []*ReplicationSource
	for rows.Next() {
		var source ReplicationSource
		err := rows.Scan(
			&source.ReplicationSourceID,
			&source.TenantID,
			&source.WorkspaceID,
			&source.DatabaseID,
			&source.TableName,
			&source.RelationshipID,
			&source.PublicationName,
			&source.SlotName,
			&source.StatusMessage,
			&source.Status,
			&source.Created,
			&source.Updated,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning replication source: %w", err)
		}
		sources = append(sources, &source)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating replication sources: %w", err)
	}

	syslog.Info("anchor", "Found %d replication sources for workspace %s", len(sources), workspaceID)
	return sources, nil
}

// UpdateReplicationSourceStatus updates the status of a replication source
func (r *Repository) UpdateReplicationSourceStatus(ctx context.Context, replicationSourceID string, status string, statusMessage string) error {
	syslog.Info("anchor", "Updating replication source status for %s: status=%s, message=%s", replicationSourceID, status, statusMessage)

	query := `
		UPDATE replication_sources 
		SET 
			status = $1,
			status_message = $2,
			updated = CURRENT_TIMESTAMP
		WHERE replication_source_id = $3
	`

	result, err := r.db.Pool().Exec(ctx, query, status, statusMessage, replicationSourceID)
	if err != nil {
		return fmt.Errorf("error updating replication source status: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("replication source with ID %s not found", replicationSourceID)
	}

	syslog.Info("anchor", "Successfully updated replication source status for %s", replicationSourceID)
	return nil
}

// RemoveReplicationSource removes a replication source from the database
func (r *Repository) RemoveReplicationSource(ctx context.Context, replicationSourceID string) error {
	syslog.Info("anchor", "Removing replication source %s", replicationSourceID)

	query := `DELETE FROM replication_sources WHERE replication_source_id = $1`

	result, err := r.db.Pool().Exec(ctx, query, replicationSourceID)
	if err != nil {
		return fmt.Errorf("error removing replication source: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("replication source with ID %s not found", replicationSourceID)
	}

	syslog.Info("anchor", "Successfully removed replication source %s", replicationSourceID)
	return nil
}
