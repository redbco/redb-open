package encryption

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/redbco/redb-open/pkg/keyring"
)

const (
	// Keyring service name for reDB security
	KeyringService = "redb-security"
	// Keyring key prefixes for tenant keys
	TenantPrivateKeyKeyPrefix = "tenant-private-key"
	TenantPublicKeyKeyPrefix  = "tenant-public-key"
)

// TenantEncryptionManager handles encryption and decryption using tenant-specific RSA keys
type TenantEncryptionManager struct {
	keyringManager *keyring.KeyringManager
}

// NewTenantEncryptionManager creates a new tenant encryption manager
func NewTenantEncryptionManager() *TenantEncryptionManager {
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManager(keyringPath, masterPassword)

	return &TenantEncryptionManager{
		keyringManager: km,
	}
}

// getTenantPrivateKeyName generates the keyring key name for a tenant's private key
func (tem *TenantEncryptionManager) getTenantPrivateKeyName(tenantID string) string {
	return fmt.Sprintf("%s-%s", TenantPrivateKeyKeyPrefix, tenantID)
}

// getTenantPublicKeyName generates the keyring key name for a tenant's public key
func (tem *TenantEncryptionManager) getTenantPublicKeyName(tenantID string) string {
	return fmt.Sprintf("%s-%s", TenantPublicKeyKeyPrefix, tenantID)
}

// getTenantPrivateKey retrieves the RSA private key for a specific tenant from the keyring
func (tem *TenantEncryptionManager) getTenantPrivateKey(tenantID string) (*rsa.PrivateKey, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}

	keyName := tem.getTenantPrivateKeyName(tenantID)
	privateKeyPEM, err := tem.keyringManager.Get(KeyringService, keyName)
	if err != nil {
		return nil, fmt.Errorf("tenant private key not found for tenant %s: %w", tenantID, err)
	}

	// Decode PEM block
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	// Parse private key
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return privateKey, nil
}

// getTenantPublicKey retrieves the RSA public key for a specific tenant from the keyring
func (tem *TenantEncryptionManager) getTenantPublicKey(tenantID string) (*rsa.PublicKey, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID is required")
	}

	keyName := tem.getTenantPublicKeyName(tenantID)
	publicKeyPEM, err := tem.keyringManager.Get(KeyringService, keyName)
	if err != nil {
		return nil, fmt.Errorf("tenant public key not found for tenant %s: %w", tenantID, err)
	}

	// Decode PEM block
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	// Parse public key
	publicKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	rsaPublicKey, ok := publicKey.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("public key is not an RSA key")
	}

	return rsaPublicKey, nil
}

// Encrypt encrypts a payload using the tenant's public key
// The payload is typically a password or sensitive string
func (tem *TenantEncryptionManager) Encrypt(tenantID string, payload string) (string, error) {
	if tenantID == "" {
		return "", errors.New("tenant ID is required")
	}
	if payload == "" {
		return "", errors.New("payload is required")
	}

	// Get the tenant's public key
	publicKey, err := tem.getTenantPublicKey(tenantID)
	if err != nil {
		return "", fmt.Errorf("failed to get tenant public key: %w", err)
	}

	// Convert payload to bytes
	payloadBytes := []byte(payload)

	// Encrypt the payload using RSA-OAEP
	encryptedBytes, err := rsa.EncryptOAEP(
		sha256.New(),
		rand.Reader,
		publicKey,
		payloadBytes,
		nil, // No label
	)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt payload: %w", err)
	}

	// Encode the encrypted bytes as base64 for storage
	encryptedString := base64.StdEncoding.EncodeToString(encryptedBytes)

	return encryptedString, nil
}

// Decrypt decrypts an encrypted payload using the tenant's private key
// Returns the original payload (typically a password or sensitive string)
func (tem *TenantEncryptionManager) Decrypt(tenantID string, encryptedPayload string) (string, error) {
	if tenantID == "" {
		return "", errors.New("tenant ID is required")
	}
	if encryptedPayload == "" {
		return "", errors.New("encrypted payload is required")
	}

	// Get the tenant's private key
	privateKey, err := tem.getTenantPrivateKey(tenantID)
	if err != nil {
		return "", fmt.Errorf("failed to get tenant private key: %w", err)
	}

	// Decode the base64 encrypted payload
	encryptedBytes, err := base64.StdEncoding.DecodeString(encryptedPayload)
	if err != nil {
		return "", fmt.Errorf("failed to decode encrypted payload: %w", err)
	}

	// Decrypt the payload using RSA-OAEP
	decryptedBytes, err := rsa.DecryptOAEP(
		sha256.New(),
		rand.Reader,
		privateKey,
		encryptedBytes,
		nil, // No label
	)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt payload: %w", err)
	}

	// Convert decrypted bytes back to string
	decryptedString := string(decryptedBytes)

	return decryptedString, nil
}

// EncryptPassword is a convenience function for encrypting passwords
// This is the function that should be used by other services
func EncryptPassword(tenantID string, password string) (string, error) {
	if password == "" {
		return "", errors.New("password is required")
	}

	manager := NewTenantEncryptionManager()
	return manager.Encrypt(tenantID, password)
}

// DecryptPassword is a convenience function for decrypting passwords
// This is the function that should be used by other services
func DecryptPassword(tenantID string, encryptedPassword string) (string, error) {
	if encryptedPassword == "" {
		return "", errors.New("encrypted password is required")
	}

	manager := NewTenantEncryptionManager()
	return manager.Decrypt(tenantID, encryptedPassword)
}

// ValidateTenantKeys checks if both public and private keys exist for a tenant
func (tem *TenantEncryptionManager) ValidateTenantKeys(tenantID string) error {
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}

	// Check if private key exists
	privateKeyName := tem.getTenantPrivateKeyName(tenantID)
	_, err := tem.keyringManager.Get(KeyringService, privateKeyName)
	if err != nil {
		return fmt.Errorf("tenant private key not found for tenant %s: %w", tenantID, err)
	}

	// Check if public key exists
	publicKeyName := tem.getTenantPublicKeyName(tenantID)
	_, err = tem.keyringManager.Get(KeyringService, publicKeyName)
	if err != nil {
		return fmt.Errorf("tenant public key not found for tenant %s: %w", tenantID, err)
	}

	return nil
}

// TestEncryption performs a test encryption and decryption to verify the keys work correctly
func (tem *TenantEncryptionManager) TestEncryption(tenantID string) error {
	if tenantID == "" {
		return errors.New("tenant ID is required")
	}

	// Test payload
	testPayload := "test-encryption-payload"

	// Encrypt the test payload
	encrypted, err := tem.Encrypt(tenantID, testPayload)
	if err != nil {
		return fmt.Errorf("test encryption failed: %w", err)
	}

	// Decrypt the test payload
	decrypted, err := tem.Decrypt(tenantID, encrypted)
	if err != nil {
		return fmt.Errorf("test decryption failed: %w", err)
	}

	// Verify the decrypted payload matches the original
	if decrypted != testPayload {
		return errors.New("test encryption/decryption round-trip failed: decrypted payload does not match original")
	}

	return nil
}
