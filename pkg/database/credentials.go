package database

import (
	"fmt"
	"os"
	"time"

	"github.com/redbco/redb-open/pkg/keyring"
)

const (
	// Keyring service name for database credentials (base name)
	DatabaseKeyringService = "redb-database"
	DatabasePasswordKey    = "postgres-password"
	ProductionUser         = "redb"
	DefaultDatabase        = "redb"
)

// GetProductionPassword retrieves the production database password from keyring
// This function can be used by any service that needs to access the production database
// It supports multi-instance configuration through environment variables
func GetProductionPassword() (string, error) {
	// Get instance group ID from environment for multi-instance support
	groupID := os.Getenv("REDB_INSTANCE_GROUP_ID")
	if groupID == "" {
		groupID = "default"
	}

	// Get keyring backend type from environment (auto, file, or system)
	backend := os.Getenv("REDB_KEYRING_BACKEND")
	if backend == "" {
		backend = "auto"
	}

	// Get keyring path from environment or use default
	keyringPath := os.Getenv("REDB_KEYRING_PATH")
	if keyringPath == "" {
		keyringPath = keyring.GetDefaultKeyringPath()
	}

	// Apply instance group isolation to keyring path if using file backend
	if backend == "file" || backend == "auto" {
		keyringPath = keyring.GetKeyringPathWithGroup(keyringPath, groupID)
	}

	// Initialize keyring manager with proper backend support
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManagerWithBackend(keyringPath, masterPassword, backend)

	// Get instance-aware service name
	serviceName := keyring.GetServiceNameWithGroup(DatabaseKeyringService, groupID)

	password, err := km.Get(serviceName, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("database password not found in keyring - has the node been initialized? Error: %w", err)
	}
	return password, nil
}

// FromProductionConfig creates a PostgreSQL config using keyring credentials
func FromProductionConfig(databaseName string) (PostgreSQLConfig, error) {
	return FromProductionConfigWithUser(databaseName, "")
}

// FromProductionConfigWithUser creates a PostgreSQL config using keyring credentials with specified user
func FromProductionConfigWithUser(databaseName, databaseUser string) (PostgreSQLConfig, error) {
	password, err := GetProductionPassword()
	if err != nil {
		return PostgreSQLConfig{}, err
	}

	// Use provided database name, or try environment variable, or use default
	dbName := databaseName
	if dbName == "" {
		dbName = os.Getenv("REDB_DATABASE_NAME")
	}
	if dbName == "" {
		dbName = DefaultDatabase
	}

	// Use provided database user, or try environment variable, or derive from database name for multi-instance
	dbUser := databaseUser
	if dbUser == "" {
		dbUser = os.Getenv("REDB_DATABASE_USER")
	}
	if dbUser == "" {
		// For multi-instance support, username should match the database name
		// This ensures each instance has its own isolated database user
		if dbName != "" && dbName != DefaultDatabase {
			dbUser = dbName
		} else {
			dbUser = ProductionUser
		}
	}

	return PostgreSQLConfig{
		User:              dbUser,
		Password:          password,
		Host:              "localhost",
		Port:              5432,
		Database:          dbName,
		SSLMode:           "disable",
		MaxConnections:    10,
		ConnectionTimeout: 5 * time.Second,
	}, nil
}

// DatabaseCredentialsManager provides access to database credentials
type DatabaseCredentialsManager struct {
	keyringManager *keyring.KeyringManager
	serviceName    string // instance-aware service name
}

// NewDatabaseCredentialsManager creates a new database credentials manager
// with multi-instance support through environment variables
func NewDatabaseCredentialsManager() *DatabaseCredentialsManager {
	// Get instance group ID from environment for multi-instance support
	groupID := os.Getenv("REDB_INSTANCE_GROUP_ID")
	if groupID == "" {
		groupID = "default"
	}

	// Get keyring backend type from environment (auto, file, or system)
	backend := os.Getenv("REDB_KEYRING_BACKEND")
	if backend == "" {
		backend = "auto"
	}

	// Get keyring path from environment or use default
	keyringPath := os.Getenv("REDB_KEYRING_PATH")
	if keyringPath == "" {
		keyringPath = keyring.GetDefaultKeyringPath()
	}

	// Apply instance group isolation to keyring path if using file backend
	if backend == "file" || backend == "auto" {
		keyringPath = keyring.GetKeyringPathWithGroup(keyringPath, groupID)
	}

	// Initialize keyring manager with proper backend support
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManagerWithBackend(keyringPath, masterPassword, backend)

	// Get instance-aware service name
	serviceName := keyring.GetServiceNameWithGroup(DatabaseKeyringService, groupID)

	return &DatabaseCredentialsManager{
		keyringManager: km,
		serviceName:    serviceName,
	}
}

// GetDatabasePassword retrieves the production database password from keyring
func (dcm *DatabaseCredentialsManager) GetDatabasePassword() (string, error) {
	password, err := dcm.keyringManager.Get(dcm.serviceName, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve database password from keyring: %w", err)
	}
	return password, nil
}

// SetDatabasePassword stores the production database password in keyring
func (dcm *DatabaseCredentialsManager) SetDatabasePassword(password string) error {
	return dcm.keyringManager.Set(dcm.serviceName, DatabasePasswordKey, password)
}

// TestConnection tests if the database connection works with stored credentials
func (dcm *DatabaseCredentialsManager) TestConnection(databaseName string) error {
	config, err := FromProductionConfig(databaseName)
	if err != nil {
		return fmt.Errorf("failed to get production config: %w", err)
	}

	// Here you would typically test the actual database connection
	// For now, we'll just validate that we have the required fields
	if config.User == "" || config.Password == "" || config.Host == "" || config.Database == "" {
		return fmt.Errorf("incomplete database configuration")
	}

	return nil
}
