package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles database-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new database service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Database represents a database in the system
type Database struct {
	ID                string
	TenantID          string
	WorkspaceID       string
	EnvironmentID     *string
	ConnectedToNodeID string
	InstanceID        string
	Name              string
	Description       string
	Type              string
	Vendor            string
	Version           string
	Username          string
	Password          string
	DBName            string
	Enabled           bool
	PolicyIDs         []string
	Metadata          map[string]interface{}
	OwnerID           string
	StatusMessage     string
	Status            string
	Created           time.Time
	Updated           time.Time
	Schema            string
	Tables            string
	// Instance connection details (inherited from parent instance)
	InstanceName          string
	InstanceDescription   string
	InstanceType          string
	InstanceVendor        string
	InstanceVersion       string
	InstanceUniqueID      string
	InstanceHost          string
	InstancePort          int32
	InstanceUsername      string
	InstancePassword      string
	InstanceSystemDBName  string
	InstanceEnabled       bool
	InstanceSSL           bool
	InstanceSSLMode       string
	InstanceSSLCert       *string
	InstanceSSLKey        *string
	InstanceSSLRootCert   *string
	InstanceMetadata      map[string]interface{}
	InstancePolicyIDs     []string
	InstanceOwnerID       string
	InstanceStatusMessage string
	InstanceStatus        string
	InstanceCreated       time.Time
	InstanceUpdated       time.Time
}

// Create creates a new database
func (s *Service) Create(ctx context.Context, tenantID, workspaceID, name, description, dbType, vendor, username, password, dbName string, nodeID *string, enabled bool, environmentID, instanceID, ownerID string) (*Database, error) {
	s.logger.Infof("Creating database in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceID, name)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
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

	// Check if the instance exists (if provided)
	if instanceID != "" {
		var instExists bool
		err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM instances WHERE instance_id = $1)", instanceID).Scan(&instExists)
		if err != nil {
			return nil, fmt.Errorf("failed to check instance existence: %w", err)
		}
		if !instExists {
			return nil, errors.New("instance not found")
		}
	}

	// Check if database with the same name already exists in this workspace
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM databases WHERE tenant_id = $1 AND workspace_id = $2 AND database_name = $3)", tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check database existence: %w", err)
	}
	if exists {
		return nil, errors.New("database with this name already exists in the workspace")
	}

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

	// Insert the database into the database
	query := `
		INSERT INTO databases (tenant_id, workspace_id, environment_id, connected_to_node_id, instance_id, database_name, database_description, database_type, database_vendor, database_version, database_username, database_password, database_db_name, database_enabled, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		RETURNING database_id, tenant_id, workspace_id, environment_id, connected_to_node_id, instance_id, database_name, database_description, database_type, database_vendor, database_version, database_username, database_password, database_db_name, database_enabled, policy_ids, database_metadata, owner_id, database_status_message, status, created, updated
	`

	var database Database
	var envID *string
	if environmentID != "" {
		envID = &environmentID
	}

	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, envID, finalNodeID, instanceID, name, description, dbType, vendor, "1.0.0", username, encryptedPassword, dbName, enabled, ownerID).Scan(
		&database.ID,
		&database.TenantID,
		&database.WorkspaceID,
		&database.EnvironmentID,
		&database.ConnectedToNodeID,
		&database.InstanceID,
		&database.Name,
		&database.Description,
		&database.Type,
		&database.Vendor,
		&database.Version,
		&database.Username,
		&database.Password,
		&database.DBName,
		&database.Enabled,
		&database.PolicyIDs,
		&database.Metadata,
		&database.OwnerID,
		&database.StatusMessage,
		&database.Status,
		&database.Created,
		&database.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create database: %v", err)
		return nil, err
	}

	// Populate instance details
	if err := s.populateInstanceDetails(ctx, &database); err != nil {
		s.logger.Warnf("Failed to populate instance details for database %s: %v", database.ID, err)
		// Don't fail the creation, just log the warning
	}

	return &database, nil
}

// Get retrieves a database by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceID, name string) (*Database, error) {
	s.logger.Infof("Retrieving database from database with ID: %s", name)
	query := `
		SELECT database_id, tenant_id, workspace_id, environment_id, connected_to_node_id, 
			instance_id, database_name, database_description, database_type, database_vendor, 
			database_version, database_username, database_password, database_db_name, 
			database_enabled, policy_ids, database_metadata, owner_id, database_status_message, 
			status, created, updated, database_schema, database_tables
		FROM databases
		WHERE tenant_id = $1 AND workspace_id = $2 AND database_name = $3
	`

	var database Database
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name).Scan(
		&database.ID,
		&database.TenantID,
		&database.WorkspaceID,
		&database.EnvironmentID,
		&database.ConnectedToNodeID,
		&database.InstanceID,
		&database.Name,
		&database.Description,
		&database.Type,
		&database.Vendor,
		&database.Version,
		&database.Username,
		&database.Password,
		&database.DBName,
		&database.Enabled,
		&database.PolicyIDs,
		&database.Metadata,
		&database.OwnerID,
		&database.StatusMessage,
		&database.Status,
		&database.Created,
		&database.Updated,
		&database.Schema,
		&database.Tables,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("database not found")
		}
		s.logger.Errorf("Failed to get database: %v", err)
		return nil, err
	}

	// Populate instance details
	if err := s.populateInstanceDetails(ctx, &database); err != nil {
		s.logger.Warnf("Failed to populate instance details for database %s: %v", database.ID, err)
		// Don't fail the get operation, just log the warning
	}

	return &database, nil
}

// GetByID retrieves a database by ID
func (s *Service) GetByID(ctx context.Context, databaseID string) (*Database, error) {
	s.logger.Infof("Retrieving database from database with ID: %s", databaseID)
	query := `
		SELECT database_id, tenant_id, workspace_id, environment_id, connected_to_node_id, 
			instance_id, database_name, database_description, database_type, database_vendor, 
			database_version, database_username, database_password, database_db_name, 
			database_enabled, policy_ids, database_metadata, owner_id, database_status_message, 
			status, created, updated, database_schema, database_tables
		FROM databases
		WHERE database_id = $1
	`

	var database Database
	err := s.db.Pool().QueryRow(ctx, query, databaseID).Scan(
		&database.ID,
		&database.TenantID,
		&database.WorkspaceID,
		&database.EnvironmentID,
		&database.ConnectedToNodeID,
		&database.InstanceID,
		&database.Name,
		&database.Description,
		&database.Type,
		&database.Vendor,
		&database.Version,
		&database.Username,
		&database.Password,
		&database.DBName,
		&database.Enabled,
		&database.PolicyIDs,
		&database.Metadata,
		&database.OwnerID,
		&database.StatusMessage,
		&database.Status,
		&database.Created,
		&database.Updated,
		&database.Schema,
		&database.Tables,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("database not found")
		}
		s.logger.Errorf("Failed to get database by ID: %v", err)
		return nil, err
	}

	// Populate instance details
	if err := s.populateInstanceDetails(ctx, &database); err != nil {
		s.logger.Warnf("Failed to populate instance details for database %s: %v", database.ID, err)
		// Don't fail the get operation, just log the warning
	}

	return &database, nil
}

// populateInstanceDetails fetches and populates instance details for a database
func (s *Service) populateInstanceDetails(ctx context.Context, database *Database) error {
	if database.InstanceID == "" {
		return nil
	}

	query := `
		SELECT instance_name, instance_description, instance_type, instance_vendor, instance_version,
			instance_unique_identifier, instance_host, instance_port, instance_username, instance_password,
			instance_system_db_name, instance_enabled, instance_ssl, instance_ssl_mode, instance_ssl_cert,
			instance_ssl_key, instance_ssl_root_cert, instance_metadata, policy_ids, owner_id,
			instance_status_message, status, created, updated
		FROM instances
		WHERE instance_id = $1
	`

	err := s.db.Pool().QueryRow(ctx, query, database.InstanceID).Scan(
		&database.InstanceName,
		&database.InstanceDescription,
		&database.InstanceType,
		&database.InstanceVendor,
		&database.InstanceVersion,
		&database.InstanceUniqueID,
		&database.InstanceHost,
		&database.InstancePort,
		&database.InstanceUsername,
		&database.InstancePassword,
		&database.InstanceSystemDBName,
		&database.InstanceEnabled,
		&database.InstanceSSL,
		&database.InstanceSSLMode,
		&database.InstanceSSLCert,
		&database.InstanceSSLKey,
		&database.InstanceSSLRootCert,
		&database.InstanceMetadata,
		&database.InstancePolicyIDs,
		&database.InstanceOwnerID,
		&database.InstanceStatusMessage,
		&database.InstanceStatus,
		&database.InstanceCreated,
		&database.InstanceUpdated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logger.Warnf("Instance not found for database %s: %s", database.ID, database.InstanceID)
			return nil
		}
		return fmt.Errorf("failed to populate instance details: %w", err)
	}

	return nil
}

// List retrieves all databases for a tenant and workspace
func (s *Service) List(ctx context.Context, tenantID, workspaceID string) ([]*Database, error) {
	s.logger.Infof("Listing databases from database for tenant: %s, workspace: %s", tenantID, workspaceID)
	query := `
		SELECT database_id, tenant_id, workspace_id, environment_id, connected_to_node_id, 
			instance_id, database_name, database_description, database_type, database_vendor, 
			database_version, database_username, database_password, database_db_name, 
			database_enabled, policy_ids, database_metadata, owner_id, database_status_message, 
			status, created, updated
		FROM databases
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY database_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID)
	if err != nil {
		s.logger.Errorf("Failed to list databases: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var databases []*Database
	for rows.Next() {
		var database Database
		err := rows.Scan(
			&database.ID,
			&database.TenantID,
			&database.WorkspaceID,
			&database.EnvironmentID,
			&database.ConnectedToNodeID,
			&database.InstanceID,
			&database.Name,
			&database.Description,
			&database.Type,
			&database.Vendor,
			&database.Version,
			&database.Username,
			&database.Password,
			&database.DBName,
			&database.Enabled,
			&database.PolicyIDs,
			&database.Metadata,
			&database.OwnerID,
			&database.StatusMessage,
			&database.Status,
			&database.Created,
			&database.Updated,
		)
		if err != nil {
			return nil, err
		}

		// Populate instance details for each database
		if err := s.populateInstanceDetails(ctx, &database); err != nil {
			s.logger.Warnf("Failed to populate instance details for database %s: %v", database.ID, err)
			// Don't fail the list operation, just log the warning
		}

		databases = append(databases, &database)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return databases, nil
}

// Update updates specific fields of a database
func (s *Service) Update(ctx context.Context, tenantID, workspaceID, name string, updates map[string]interface{}) (*Database, error) {
	s.logger.Infof("Updating database in database with ID: %s, updates: %v", name, updates)

	// If no updates, just return the current database
	if len(updates) == 0 {
		return s.Get(ctx, tenantID, workspaceID, name)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE databases SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause
	query += fmt.Sprintf(" WHERE tenant_id = $%d AND workspace_id = $%d AND database_name = $%d RETURNING database_id, tenant_id, workspace_id, environment_id, connected_to_node_id, instance_id, database_name, database_description, database_type, database_vendor, database_version, database_username, database_password, database_db_name, database_enabled, policy_ids, database_metadata, owner_id, database_status_message, status, created, updated", argIndex, argIndex+1, argIndex+2)
	args = append(args, tenantID, workspaceID, name)

	var database Database
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&database.ID,
		&database.TenantID,
		&database.WorkspaceID,
		&database.EnvironmentID,
		&database.ConnectedToNodeID,
		&database.InstanceID,
		&database.Name,
		&database.Description,
		&database.Type,
		&database.Vendor,
		&database.Version,
		&database.Username,
		&database.Password,
		&database.DBName,
		&database.Enabled,
		&database.PolicyIDs,
		&database.Metadata,
		&database.OwnerID,
		&database.StatusMessage,
		&database.Status,
		&database.Created,
		&database.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("database not found")
		}
		s.logger.Errorf("Failed to update database: %v", err)
		return nil, err
	}

	// Populate instance details
	if err := s.populateInstanceDetails(ctx, &database); err != nil {
		s.logger.Warnf("Failed to populate instance details for database %s: %v", database.ID, err)
		// Don't fail the update, just log the warning
	}

	return &database, nil
}

// Delete deletes a database
func (s *Service) Delete(ctx context.Context, tenantID, workspaceID, name string) error {
	s.logger.Infof("Deleting database from database with ID: %s", name)
	query := `DELETE FROM databases WHERE tenant_id = $1 AND workspace_id = $2 AND database_name = $3`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceID, name)
	if err != nil {
		s.logger.Errorf("Failed to delete database: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("database not found")
	}

	return nil
}

// GetDatabaseConfigIdByDatabase retrieves the database config ID for a given database ID
func (s *Service) GetDatabaseConfigIdByDatabase(ctx context.Context, id string) (string, error) {
	query := `
		SELECT config_id FROM databases WHERE id = $1
	`

	var configID string
	err := s.db.Pool().QueryRow(ctx, query, id).Scan(&configID)
	if err != nil {
		return "", err
	}

	return configID, nil
}

// Disable disables a database by setting database_enabled to false
func (s *Service) Disable(ctx context.Context, tenantID, workspaceID, name string) error {
	s.logger.Infof("Disabling database in database with ID: %s", name)
	query := `UPDATE databases SET database_enabled = false, updated = CURRENT_TIMESTAMP WHERE tenant_id = $1 AND workspace_id = $2 AND database_name = $3`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceID, name)
	if err != nil {
		s.logger.Errorf("Failed to disable database: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("database not found")
	}

	return nil
}

// Enable enables a database by setting database_enabled to true
func (s *Service) Enable(ctx context.Context, tenantID, workspaceID, name string) error {
	s.logger.Infof("Enabling database in database with ID: %s", name)
	query := `UPDATE databases SET database_enabled = true, updated = CURRENT_TIMESTAMP WHERE tenant_id = $1 AND workspace_id = $2 AND database_name = $3`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceID, name)
	if err != nil {
		s.logger.Errorf("Failed to enable database: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("database not found")
	}

	return nil
}

// StoreDatabaseSchema stores the database schema in the database
func (s *Service) StoreDatabaseSchema(ctx context.Context, databaseID, schema string) error {
	s.logger.Infof("Storing database schema in database with ID: %s", databaseID)
	query := `UPDATE databases SET database_schema = $1, updated = CURRENT_TIMESTAMP WHERE database_id = $2`

	commandTag, err := s.db.Pool().Exec(ctx, query, schema, databaseID)
	if err != nil {
		s.logger.Errorf("Failed to store database schema: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("database not found")
	}

	return nil
}

// StoreDatabaseTables stores the database tables in the database
func (s *Service) StoreDatabaseTables(ctx context.Context, databaseID, tables string) error {
	s.logger.Infof("Storing database tables in database with ID: %s", databaseID)
	query := `UPDATE databases SET database_tables = $1, updated = CURRENT_TIMESTAMP WHERE database_id = $2`

	commandTag, err := s.db.Pool().Exec(ctx, query, tables, databaseID)
	if err != nil {
		s.logger.Errorf("Failed to store database tables: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("database not found")
	}

	return nil
}

// GetDatabaseSchema retrieves the database schema from the database
func (s *Service) GetDatabaseSchema(ctx context.Context, databaseID string) (string, error) {
	query := `SELECT database_schema FROM databases WHERE database_id = $1`

	var schema string
	err := s.db.Pool().QueryRow(ctx, query, databaseID).Scan(&schema)
	if err != nil {
		return "", err
	}

	return schema, nil
}

// GetDatabaseTables retrieves the database tables from the database
func (s *Service) GetDatabaseTables(ctx context.Context, databaseID string) (string, error) {
	query := `SELECT database_tables FROM databases WHERE database_id = $1`

	var tables string
	err := s.db.Pool().QueryRow(ctx, query, databaseID).Scan(&tables)
	if err != nil {
		return "", err
	}

	return tables, nil
}
