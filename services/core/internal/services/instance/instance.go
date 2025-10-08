package instance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/pkg/logger"
	databaseService "github.com/redbco/redb-open/services/core/internal/services/database"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
)

// System database mapping for database types that have separate system databases
var systemDatabaseMap = map[string]string{
	"clickhouse":  "default",
	"cosmosdb":    "master",
	"cockroachdb": "defaultdb",
	"cockroach":   "defaultdb", // Alternative name
	"db2":         "BLUDB",
	"edgedb":      "edgedb",
	"mariadb":     "mysql",
	"mongodb":     "admin",
	"mssql":       "master",
	"ms-sql":      "master", // Alternative name
	"mysql":       "mysql",
	"oracle":      "ORCL",
	"postgres":    "postgres",
	"postgresql":  "postgres", // Alternative name
	"snowflake":   "SNOWFLAKE",
}

// getSystemDatabaseName returns the appropriate system database name for the given database vendor/type
// For databases without separate system databases, returns the user-specified database name
func getSystemDatabaseName(vendor, userDBName string) string {
	if systemDB, exists := systemDatabaseMap[vendor]; exists {
		return systemDB
	}
	// For databases without separate system databases, use the user database name
	return userDBName
}

// Service handles instance-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new instance service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Instance represents an instance in the system
type Instance struct {
	ID                string
	TenantID          string
	WorkspaceID       string
	EnvironmentID     *string
	ConnectedToNodeID string
	Name              string
	Description       string
	Type              string
	Vendor            string
	Version           string
	UniqueIdentifier  string
	Host              string
	Port              int32
	Username          string
	Password          string
	SystemDBName      string
	Enabled           bool
	SSL               bool
	SSLMode           string
	SSLCert           *string
	SSLKey            *string
	SSLRootCert       *string
	Metadata          map[string]interface{}
	PolicyIDs         []string
	OwnerID           string
	StatusMessage     string
	Status            string
	Created           time.Time
	Updated           time.Time
}

// Create creates a new instance
// userDBNameOpt allows passing the user-specified logical database name when available
// so we can select the correct system database for vendors that require it.
func (s *Service) Create(ctx context.Context, tenantID, workspaceName, name, description, instanceType, vendor, host, username, password string, nodeID *string, port int32, enabled bool, ssl bool, sslMode string, environmentID, ownerID string, sslCert, sslKey, sslRootCert *string, userDBNameOpt *string) (*Instance, error) {
	s.logger.Infof("Creating instance in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceName, name)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Get the workspace ID from the workspace name
	var workspaceID string
	err = s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace ID: %w", err)
	}

	// Check if the workspace exists and belongs to the tenant
	var workspaceExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = $1 AND tenant_id = $2)", workspaceID, tenantID).Scan(&workspaceExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check workspace existence: %w", err)
	}
	if !workspaceExists {
		return nil, errors.New("workspace not found in tenant")
	}

	// Determine the node ID to use
	var finalNodeID string
	if nodeID != nil && *nodeID != "" {
		finalNodeID = *nodeID
	} else {
		// Default to the identity_id from localidentity table (BIGINT)
		var identityID int64
		err = s.db.Pool().QueryRow(ctx, "SELECT identity_id FROM localidentity LIMIT 1").Scan(&identityID)
		if err != nil {
			return nil, fmt.Errorf("failed to get local identity: %w", err)
		}
		finalNodeID = fmt.Sprintf("%d", identityID)
	}

	// Check if the node exists
	var nodeExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)", finalNodeID).Scan(&nodeExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check node existence: %w", err)
	}
	if !nodeExists {
		return nil, errors.New("node not found")
	}

	// Check if instance with the same name already exists in this workspace
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM instances WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3)", tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check instance existence: %w", err)
	}
	if exists {
		return nil, errors.New("instance with this name already exists in the workspace")
	}

	// Generate unique identifier
	uniqueID := fmt.Sprintf("%s_%s_%s", tenantID, workspaceID, name)

	// Set default values
	version := "1.0.0"
	if vendor != "" && vendor != "generic" {
		version = "unknown" // Use latest for specific vendors
	}

	// Determine system database name based on vendor/type
	// If a user database name is available (e.g., ConnectDatabase flow), prefer it for vendors
	// without separate system databases. Otherwise, fall back to vendor-specific system DB.
	var providedUserDB string
	if userDBNameOpt != nil && *userDBNameOpt != "" {
		providedUserDB = *userDBNameOpt
	}
	systemDBName := getSystemDatabaseName(instanceType, providedUserDB)
	if systemDBName == "" {
		// Ensure non-empty value; some connectors ignore this, but keep sensible default
		if providedUserDB != "" {
			systemDBName = providedUserDB
		} else {
			systemDBName = "system"
		}
	}

	// Initialize empty metadata and policy arrays
	emptyMetadata := "{}"
	emptyPolicyIDs := "{}"

	// Create the encrypted password (If "" is password, no encryption is done)
	var encryptedPassword string
	if password != "" {
		encryptedPassword, err = encryption.EncryptPassword(tenantID, password)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt password: %w", err)
		}
	} else {
		encryptedPassword = password
	}

	// Insert the instance into the database
	query := `
		INSERT INTO instances (tenant_id, workspace_id, environment_id, connected_to_node_id, instance_name, instance_description, instance_type, instance_vendor, instance_version, instance_unique_identifier, instance_host, instance_port, instance_username, instance_password, instance_system_db_name, instance_enabled, instance_ssl, instance_ssl_mode, instance_ssl_cert, instance_ssl_key, instance_ssl_root_cert, instance_metadata, policy_ids, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
		RETURNING instance_id, tenant_id, workspace_id, environment_id, connected_to_node_id, instance_name, instance_description, instance_type, instance_vendor, instance_version, instance_unique_identifier, instance_host, instance_port, instance_username, instance_password, instance_system_db_name, instance_enabled, instance_ssl, instance_ssl_mode, instance_ssl_cert, instance_ssl_key, instance_ssl_root_cert, instance_metadata, policy_ids, owner_id, instance_status_message, status, created, updated
	`

	var instance Instance
	var envID *string
	if environmentID != "" {
		envID = &environmentID
	}

	var metadataJSON []byte
	var policyIDsArray []string

	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, envID, finalNodeID, name, description, instanceType, vendor, version, uniqueID, host, port, username, encryptedPassword, systemDBName, enabled, ssl, sslMode, sslCert, sslKey, sslRootCert, emptyMetadata, emptyPolicyIDs, ownerID).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.WorkspaceID,
		&instance.EnvironmentID,
		&instance.ConnectedToNodeID,
		&instance.Name,
		&instance.Description,
		&instance.Type,
		&instance.Vendor,
		&instance.Version,
		&instance.UniqueIdentifier,
		&instance.Host,
		&instance.Port,
		&instance.Username,
		&instance.Password,
		&instance.SystemDBName,
		&instance.Enabled,
		&instance.SSL,
		&instance.SSLMode,
		&instance.SSLCert,
		&instance.SSLKey,
		&instance.SSLRootCert,
		&metadataJSON,
		&policyIDsArray,
		&instance.OwnerID,
		&instance.StatusMessage,
		&instance.Status,
		&instance.Created,
		&instance.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create instance: %v", err)
		return nil, err
	}

	// Parse metadata JSON
	instance.Metadata = make(map[string]interface{})
	if len(metadataJSON) > 0 && string(metadataJSON) != "{}" {
		if err := json.Unmarshal(metadataJSON, &instance.Metadata); err != nil {
			s.logger.Warnf("Failed to parse metadata JSON: %v", err)
		}
	}

	// Set policy IDs
	instance.PolicyIDs = policyIDsArray

	return &instance, nil
}

// Get retrieves an instance by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceName, name string) (*Instance, error) {
	s.logger.Infof("Retrieving instance from database with name: %s", name)

	// Get the workspace ID from the workspace name using workspace service
	s.logger.Infof("Looking up workspace: name='%s', tenant_id='%s'", workspaceName, tenantID)
	workspaceService := workspace.NewService(s.db, s.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, tenantID, workspaceName)
	if err != nil {
		s.logger.Errorf("Workspace lookup failed: name='%s', tenant_id='%s', error=%v", workspaceName, tenantID, err)
		return nil, fmt.Errorf("failed to get workspace ID: %w", err)
	}
	s.logger.Infof("Found workspace_id='%s' for name='%s'", workspaceID, workspaceName)

	query := `
		SELECT instance_id, tenant_id, workspace_id, environment_id, connected_to_node_id, instance_name, instance_description, instance_type, instance_vendor, instance_version, instance_unique_identifier, instance_host, instance_port, instance_username, instance_password, instance_system_db_name, instance_enabled, instance_ssl, instance_ssl_mode, instance_ssl_cert, instance_ssl_key, instance_ssl_root_cert, instance_metadata, policy_ids, owner_id, instance_status_message, status, created, updated
		FROM instances
		WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3
	`

	var instance Instance
	var metadataJSON []byte
	var policyIDsArray []string

	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.WorkspaceID,
		&instance.EnvironmentID,
		&instance.ConnectedToNodeID,
		&instance.Name,
		&instance.Description,
		&instance.Type,
		&instance.Vendor,
		&instance.Version,
		&instance.UniqueIdentifier,
		&instance.Host,
		&instance.Port,
		&instance.Username,
		&instance.Password,
		&instance.SystemDBName,
		&instance.Enabled,
		&instance.SSL,
		&instance.SSLMode,
		&instance.SSLCert,
		&instance.SSLKey,
		&instance.SSLRootCert,
		&metadataJSON,
		&policyIDsArray,
		&instance.OwnerID,
		&instance.StatusMessage,
		&instance.Status,
		&instance.Created,
		&instance.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("instance not found")
		}
		s.logger.Errorf("Failed to get instance: %v", err)
		return nil, err
	}

	// Parse metadata JSON
	instance.Metadata = make(map[string]interface{})
	if len(metadataJSON) > 0 && string(metadataJSON) != "{}" {
		if err := json.Unmarshal(metadataJSON, &instance.Metadata); err != nil {
			s.logger.Warnf("Failed to parse metadata JSON: %v", err)
		}
	}

	// Set policy IDs
	instance.PolicyIDs = policyIDsArray

	return &instance, nil
}

// List retrieves all instances for a tenant and workspace
func (s *Service) List(ctx context.Context, tenantID, workspaceName string) ([]*Instance, error) {
	s.logger.Infof("Listing instances from database for tenant: %s, workspace: %s", tenantID, workspaceName)

	// Get the workspace ID from the workspace name
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace ID: %w", err)
	}

	query := `
		SELECT instance_id, tenant_id, workspace_id, environment_id, connected_to_node_id, instance_name, instance_description, instance_type, instance_vendor, instance_version, instance_unique_identifier, instance_host, instance_port, instance_username, instance_password, instance_system_db_name, instance_enabled, instance_ssl, instance_ssl_mode, instance_ssl_cert, instance_ssl_key, instance_ssl_root_cert, instance_metadata, policy_ids, owner_id, instance_status_message, status, created, updated
		FROM instances
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY instance_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID)
	if err != nil {
		s.logger.Errorf("Failed to list instances: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var instances []*Instance
	for rows.Next() {
		var instance Instance
		var metadataJSON []byte
		var policyIDsArray []string

		err := rows.Scan(
			&instance.ID,
			&instance.TenantID,
			&instance.WorkspaceID,
			&instance.EnvironmentID,
			&instance.ConnectedToNodeID,
			&instance.Name,
			&instance.Description,
			&instance.Type,
			&instance.Vendor,
			&instance.Version,
			&instance.UniqueIdentifier,
			&instance.Host,
			&instance.Port,
			&instance.Username,
			&instance.Password,
			&instance.SystemDBName,
			&instance.Enabled,
			&instance.SSL,
			&instance.SSLMode,
			&instance.SSLCert,
			&instance.SSLKey,
			&instance.SSLRootCert,
			&metadataJSON,
			&policyIDsArray,
			&instance.OwnerID,
			&instance.StatusMessage,
			&instance.Status,
			&instance.Created,
			&instance.Updated,
		)
		if err != nil {
			return nil, err
		}

		// Parse metadata JSON
		instance.Metadata = make(map[string]interface{})
		if len(metadataJSON) > 0 && string(metadataJSON) != "{}" {
			if err := json.Unmarshal(metadataJSON, &instance.Metadata); err != nil {
				s.logger.Warnf("Failed to parse metadata JSON for instance %s: %v", instance.ID, err)
			}
		}

		// Set policy IDs
		instance.PolicyIDs = policyIDsArray

		instances = append(instances, &instance)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return instances, nil
}

// Update updates specific fields of an instance
func (s *Service) Update(ctx context.Context, tenantID, workspaceName, name string, updates map[string]interface{}) (*Instance, error) {
	s.logger.Infof("Updating instance in database with ID: %s, updates: %v", name, updates)

	// If no updates, just return the current instance
	if len(updates) == 0 {
		return s.Get(ctx, tenantID, workspaceName, name)
	}

	// Get the workspace ID from the workspace name
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace ID: %w", err)
	}

	// Get the instance ID from the instance name
	var instanceID string
	err = s.db.Pool().QueryRow(ctx, "SELECT instance_id FROM instances WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3", tenantID, workspaceID, name).Scan(&instanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance ID: %w", err)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE instances SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause
	query += fmt.Sprintf(" WHERE tenant_id = $%d AND workspace_id = $%d AND instance_id = $%d RETURNING instance_id, tenant_id, workspace_id, environment_id, connected_to_node_id, instance_name, instance_description, instance_type, instance_vendor, instance_version, instance_unique_identifier, instance_host, instance_port, instance_username, instance_password, instance_system_db_name, instance_enabled, instance_ssl, instance_ssl_mode, instance_ssl_cert, instance_ssl_key, instance_ssl_root_cert, owner_id, instance_status_message, status, created, updated", argIndex, argIndex+1, argIndex+2)
	args = append(args, tenantID, workspaceID, instanceID)

	var instance Instance
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&instance.ID,
		&instance.TenantID,
		&instance.WorkspaceID,
		&instance.EnvironmentID,
		&instance.ConnectedToNodeID,
		&instance.Name,
		&instance.Description,
		&instance.Type,
		&instance.Vendor,
		&instance.Version,
		&instance.UniqueIdentifier,
		&instance.Host,
		&instance.Port,
		&instance.Username,
		&instance.Password,
		&instance.SystemDBName,
		&instance.Enabled,
		&instance.SSL,
		&instance.SSLMode,
		&instance.SSLCert,
		&instance.SSLKey,
		&instance.SSLRootCert,
		&instance.OwnerID,
		&instance.StatusMessage,
		&instance.Status,
		&instance.Created,
		&instance.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("instance not found")
		}
		s.logger.Errorf("Failed to update instance: %v", err)
		return nil, err
	}

	return &instance, nil
}

// Delete deletes an instance
func (s *Service) Delete(ctx context.Context, tenantID, workspaceName, name string) error {
	s.logger.Infof("Deleting instance from database with ID: %s", name)

	// Get the workspace ID from the workspace name
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return fmt.Errorf("failed to get workspace ID: %w", err)
	}

	query := `DELETE FROM instances WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceID, name)
	if err != nil {
		s.logger.Errorf("Failed to delete instance: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("instance not found")
	}

	return nil
}

// Disable disables an instance by setting instance_enabled to false
func (s *Service) Disable(ctx context.Context, tenantID, workspaceName, name string) error {
	s.logger.Infof("Disabling instance in database with ID: %s", name)

	// Get the workspace ID from the workspace name
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return fmt.Errorf("failed to get workspace ID: %w", err)
	}

	query := `UPDATE instances SET instance_enabled = false, updated = CURRENT_TIMESTAMP WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceID, name)
	if err != nil {
		s.logger.Errorf("Failed to disable instance: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("instance not found")
	}

	return nil
}

// Enable enables an instance by setting instance_enabled to true
func (s *Service) Enable(ctx context.Context, tenantID, workspaceName, name string) error {
	s.logger.Infof("Enabling instance in database with ID: %s", name)

	// Get the workspace ID from the workspace name
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return fmt.Errorf("failed to get workspace ID: %w", err)
	}

	query := `UPDATE instances SET instance_enabled = true, updated = CURRENT_TIMESTAMP WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceID, name)
	if err != nil {
		s.logger.Errorf("Failed to enable instance: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("instance not found")
	}

	return nil
}

// CreateDatabase creates a new database on an instance
func (s *Service) CreateDatabase(ctx context.Context, tenantID, workspaceName, instanceName, databaseName, databaseDescription, dbName, nodeID, ownerID string, enabled bool, createWithUser bool, databaseUsername, databasePassword *string) (*databaseService.Database, error) {
	s.logger.Infof("Creating database on instance in database for tenant: %s, workspace: %s, instance: %s, database: %s", tenantID, workspaceName, instanceName, databaseName)

	// Get the workspace ID from the workspace name
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace ID: %w", err)
	}

	// Get the instance ID from the instance name
	var instanceID string
	err = s.db.Pool().QueryRow(ctx, "SELECT instance_id FROM instances WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3", tenantID, workspaceID, instanceName).Scan(&instanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance ID: %w", err)
	}

	// Get the node ID from the node name
	var connectedToNodeID string
	err = s.db.Pool().QueryRow(ctx, "SELECT connected_to_node_id FROM instances WHERE tenant_id = $1 AND workspace_id = $2 AND instance_name = $3", tenantID, workspaceID, instanceName).Scan(&connectedToNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get node ID: %w", err)
	}

	// Create the database
	query := `INSERT INTO databases (tenant_id, workspace_id, instance_id, database_name, database_description, db_name, node_id, owner_id, enabled, created, updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
	_, err = s.db.Pool().Exec(ctx, query, tenantID, workspaceID, instanceID, databaseName, databaseDescription, dbName, nodeID, ownerID, enabled)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	return &databaseService.Database{
		TenantID:      tenantID,
		WorkspaceID:   workspaceID,
		InstanceID:    instanceID,
		Name:          databaseName,
		Description:   databaseDescription,
		DBName:        dbName,
		Enabled:       enabled,
		OwnerID:       ownerID,
		StatusMessage: "",
		Status:        "STATUS_PENDING",
		Created:       time.Now(),
		Updated:       time.Now(),
	}, nil
}
