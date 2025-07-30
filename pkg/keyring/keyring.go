package keyring

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zalando/go-keyring"
)

// FileKeyring implements a file-based keyring for headless servers
type FileKeyring struct {
	keyringPath string
	masterKey   []byte
}

// KeyringEntry represents a stored keyring entry
type KeyringEntry struct {
	Service string `json:"service"`
	User    string `json:"user"`
	Data    string `json:"data"` // encrypted data
}

// KeyringManager provides a unified interface for keyring operations
type KeyringManager struct {
	fileKeyring *FileKeyring
	useFile     bool
}

// NewKeyringManager creates a new keyring manager that tries system keyring first, falls back to file
func NewKeyringManager(keyringPath, masterPassword string) *KeyringManager {
	// Test if system keyring is available with timeout
	testService := "redb-test"
	testKey := "test-key"
	testValue := "test-value"

	// Try system keyring first with a timeout to prevent hanging
	done := make(chan error, 1)
	go func() {
		err := keyring.Set(testService, testKey, testValue)
		if err == nil {
			// Clean up test entry
			keyring.Delete(testService, testKey)
		}
		done <- err
	}()

	// Wait for the keyring test with a 5-second timeout
	select {
	case err := <-done:
		if err == nil {
			// System keyring is available
			return &KeyringManager{useFile: false}
		}
		// System keyring failed, fall through to file-based keyring
	case <-time.After(5 * time.Second):
		// Timeout occurred, fall back to file-based keyring
	}

	// Fall back to file-based keyring
	fk := NewFileKeyring(keyringPath, masterPassword)
	return &KeyringManager{
		fileKeyring: fk,
		useFile:     true,
	}
}

// NewFileKeyring creates a new file-based keyring
func NewFileKeyring(keyringPath, masterPassword string) *FileKeyring {
	// Create keyring directory if it doesn't exist
	os.MkdirAll(filepath.Dir(keyringPath), 0700)

	// Derive key from master password
	hash := sha256.Sum256([]byte(masterPassword))

	return &FileKeyring{
		keyringPath: keyringPath,
		masterKey:   hash[:],
	}
}

// Set stores a value in the keyring (system or file)
func (km *KeyringManager) Set(service, user, password string) error {
	if !km.useFile {
		return keyring.Set(service, user, password)
	}
	return km.fileKeyring.Set(service, user, password)
}

// Get retrieves a value from the keyring (system or file)
func (km *KeyringManager) Get(service, user string) (string, error) {
	if !km.useFile {
		return keyring.Get(service, user)
	}
	return km.fileKeyring.Get(service, user)
}

// Delete removes a value from the keyring (system or file)
func (km *KeyringManager) Delete(service, user string) error {
	if !km.useFile {
		return keyring.Delete(service, user)
	}
	return km.fileKeyring.Delete(service, user)
}

// encrypt encrypts plaintext using AES-GCM
func (fk *FileKeyring) encrypt(plaintext string) (string, error) {
	block, err := aes.NewCipher(fk.masterKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decrypt decrypts ciphertext using AES-GCM
func (fk *FileKeyring) decrypt(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(fk.masterKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce := data[:nonceSize]
	ciphertextBytes := data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// Set stores an entry in the file keyring
func (fk *FileKeyring) Set(service, user, password string) error {
	entries := make(map[string]KeyringEntry)

	// Load existing entries
	if data, err := os.ReadFile(fk.keyringPath); err == nil {
		json.Unmarshal(data, &entries)
	}

	// Encrypt password
	encryptedPassword, err := fk.encrypt(password)
	if err != nil {
		return err
	}

	// Store entry
	key := fmt.Sprintf("%s:%s", service, user)
	entries[key] = KeyringEntry{
		Service: service,
		User:    user,
		Data:    encryptedPassword,
	}

	// Save to file
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	return os.WriteFile(fk.keyringPath, data, 0600)
}

// Get retrieves an entry from the file keyring
func (fk *FileKeyring) Get(service, user string) (string, error) {
	entries := make(map[string]KeyringEntry)

	// Load entries
	data, err := os.ReadFile(fk.keyringPath)
	if err != nil {
		return "", fmt.Errorf("keyring file not found")
	}

	if err := json.Unmarshal(data, &entries); err != nil {
		return "", err
	}

	// Find entry
	key := fmt.Sprintf("%s:%s", service, user)
	entry, exists := entries[key]
	if !exists {
		return "", fmt.Errorf("entry not found")
	}

	// Decrypt password
	return fk.decrypt(entry.Data)
}

// Delete removes an entry from the file keyring
func (fk *FileKeyring) Delete(service, user string) error {
	entries := make(map[string]KeyringEntry)

	// Load entries
	data, err := os.ReadFile(fk.keyringPath)
	if err != nil {
		return nil // File doesn't exist, nothing to delete
	}

	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}

	// Remove entry
	key := fmt.Sprintf("%s:%s", service, user)
	delete(entries, key)

	// Save to file
	data, err = json.Marshal(entries)
	if err != nil {
		return err
	}

	return os.WriteFile(fk.keyringPath, data, 0600)
}

// GetMasterPasswordFromEnv gets master password from environment variable
func GetMasterPasswordFromEnv() string {
	if password := os.Getenv("REDB_KEYRING_PASSWORD"); password != "" {
		return password
	}
	// Default password for development (change this in production!)
	return "default-master-password-change-me"
}

// GetDefaultKeyringPath returns the default keyring file path
func GetDefaultKeyringPath() string {
	// Check for environment variable override first
	if path := os.Getenv("REDB_KEYRING_PATH"); path != "" {
		return path
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/redb-keyring.json"
	}
	return filepath.Join(homeDir, ".local", "share", "redb", "keyring.json")
}
