package initialize

import (
	"encoding/base64"
	"fmt"

	"github.com/zalando/go-keyring"
)

// DatabaseCredentialsManager handles retrieval of database credentials from keyring
type DatabaseCredentialsManager struct{}

// NewDatabaseCredentialsManager creates a new database credentials manager
func NewDatabaseCredentialsManager() *DatabaseCredentialsManager {
	return &DatabaseCredentialsManager{}
}

// GetDatabasePassword retrieves the production database password from keyring
// This function can be used by other services to get the database password
func (dcm *DatabaseCredentialsManager) GetDatabasePassword() (string, error) {
	password, err := keyring.Get(DatabaseKeyringService, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve database password from keyring: %w", err)
	}
	return password, nil
}

// GetNodeKeys retrieves the node's RSA key pair from keyring
func (dcm *DatabaseCredentialsManager) GetNodeKeys() (publicKey, privateKey string, err error) {
	publicKey, err = keyring.Get(NodeKeyringService, NodePublicKeyKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to retrieve public key from keyring: %w", err)
	}

	privateKey, err = keyring.Get(NodeKeyringService, NodePrivateKeyKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to retrieve private key from keyring: %w", err)
	}

	return publicKey, privateKey, nil
}

// GetTenantJWTSecret retrieves JWT secret for a specific tenant
func (dcm *DatabaseCredentialsManager) GetTenantJWTSecret(tenantID string) ([]byte, error) {
	secretKey := fmt.Sprintf("%s-%s", JWTSecretKeyPrefix, tenantID)
	secretString, err := keyring.Get(SecurityKeyringService, secretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve tenant JWT secret from keyring: %w", err)
	}

	// Decode base64 secret
	secretBytes, err := base64.StdEncoding.DecodeString(secretString)
	if err != nil {
		return nil, fmt.Errorf("failed to decode tenant secret: %w", err)
	}

	return secretBytes, nil
}
