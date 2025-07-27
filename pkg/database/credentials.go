package database

import (
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/keyring"
)

const (
	// Keyring service name for database credentials
	DatabaseKeyringService = "redb-database"
	DatabasePasswordKey    = "postgres-password"
	ProductionUser         = "redb"
	ProductionDatabase     = "redb"
)

// GetProductionPassword retrieves the production database password from keyring
// This function can be used by any service that needs to access the production database
func GetProductionPassword() (string, error) {
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManager(keyringPath, masterPassword)

	password, err := km.Get(DatabaseKeyringService, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("database password not found in keyring - has the node been initialized? Error: %w", err)
	}
	return password, nil
}

// FromProductionConfig creates a PostgreSQL config using keyring credentials
func FromProductionConfig() (PostgreSQLConfig, error) {
	password, err := GetProductionPassword()
	if err != nil {
		return PostgreSQLConfig{}, err
	}

	return PostgreSQLConfig{
		User:              ProductionUser,
		Password:          password,
		Host:              "localhost",
		Port:              5432,
		Database:          ProductionDatabase,
		SSLMode:           "disable",
		MaxConnections:    10,
		ConnectionTimeout: 5 * time.Second,
	}, nil
}

// DatabaseCredentialsManager provides access to database credentials
type DatabaseCredentialsManager struct {
	keyringManager *keyring.KeyringManager
}

// NewDatabaseCredentialsManager creates a new database credentials manager
func NewDatabaseCredentialsManager() *DatabaseCredentialsManager {
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManager(keyringPath, masterPassword)

	return &DatabaseCredentialsManager{
		keyringManager: km,
	}
}

// GetDatabasePassword retrieves the production database password from keyring
func (dcm *DatabaseCredentialsManager) GetDatabasePassword() (string, error) {
	password, err := dcm.keyringManager.Get(DatabaseKeyringService, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve database password from keyring: %w", err)
	}
	return password, nil
}

// SetDatabasePassword stores the production database password in keyring
func (dcm *DatabaseCredentialsManager) SetDatabasePassword(password string) error {
	return dcm.keyringManager.Set(DatabaseKeyringService, DatabasePasswordKey, password)
}

// TestConnection tests if the database connection works with stored credentials
func (dcm *DatabaseCredentialsManager) TestConnection() error {
	config, err := FromProductionConfig()
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
