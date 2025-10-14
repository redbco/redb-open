package initialize

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/term"

	"github.com/redbco/redb-open/cmd/supervisor/internal/logger"
	"github.com/redbco/redb-open/pkg/configprovider"
	"github.com/redbco/redb-open/pkg/keyring"
)

const (
	// Keyring service names for different components
	DatabaseKeyringService = "redb-database"
	SecurityKeyringService = "redb-security"
	NodeKeyringService     = "redb-node"

	// Keyring keys
	DatabasePasswordKey       = "postgres-password"
	NodePrivateKeyKey         = "node-private-key"
	NodePublicKeyKey          = "node-public-key"
	JWTSecretKeyPrefix        = "tenant-jwt-secret"
	TenantPrivateKeyKeyPrefix = "tenant-private-key"
	TenantPublicKeyKeyPrefix  = "tenant-public-key"

	// Default database credentials to try
	DefaultPostgresUser     = "postgres"
	DefaultPostgresPassword = "postgres"
	DefaultPostgresDatabase = "postgres"
	DefaultPostgresHost     = "localhost"
	DefaultPostgresPort     = 5432

	// Environment variable names for database configuration
	EnvPostgresUser     = "REDB_POSTGRES_USER"
	EnvPostgresPassword = "REDB_POSTGRES_PASSWORD"
	EnvPostgresHost     = "REDB_POSTGRES_HOST"
	EnvPostgresPort     = "REDB_POSTGRES_PORT"
	EnvPostgresDatabase = "REDB_POSTGRES_DATABASE"

	// Environment variable names for default tenant and user
	EnvDefaultTenantName   = "REDB_DEFAULT_TENANT_NAME"
	EnvDefaultUserEmail    = "REDB_DEFAULT_USER_EMAIL"
	EnvDefaultUserPassword = "REDB_DEFAULT_USER_PASSWORD"

	// Production database configuration
	ProductionDatabase = "redb"
	ProductionUser     = "redb"
)

// getProductionDatabaseName returns the production database name from config or environment
func (i *Initializer) getProductionDatabaseName() string {
	// Try to get from config first
	if i.config != nil {
		if dbProvider, ok := i.config.(configprovider.DatabaseConfigProvider); ok {
			if dbName := dbProvider.GetDatabaseName(); dbName != "" {
				return dbName
			}
		}
	}

	// Fallback to environment variable
	if dbName := os.Getenv("REDB_DATABASE_NAME"); dbName != "" {
		return dbName
	}

	// Final fallback to default
	return ProductionDatabase
}

// getProductionDatabaseUser returns the production database user from config or defaults
func (i *Initializer) getProductionDatabaseUser() string {
	// Try to get from config first
	if i.config != nil {
		if dbProvider, ok := i.config.(configprovider.DatabaseConfigProvider); ok {
			if dbUser := dbProvider.GetDatabaseUser(); dbUser != "" {
				return dbUser
			}
		}
	}

	// Fallback to environment variable
	if dbUser := os.Getenv("REDB_DATABASE_USER"); dbUser != "" {
		return dbUser
	}

	// For multi-instance support, username should match the database name
	// This ensures each instance has its own isolated database user
	dbName := i.getProductionDatabaseName()
	if dbName != "" && dbName != ProductionDatabase {
		return dbName
	}

	// Final fallback to default (for backward compatibility)
	return ProductionUser
}

type Initializer struct {
	logger         logger.LoggerInterface
	reader         io.Reader
	keyringManager *keyring.KeyringManager
	config         interface{} // Store config for instance-aware service names
	version        string      // Application version
}

type DatabaseCredentials struct {
	User     string
	Password string
	Host     string
	Port     int
	Database string
}

type NodeInfo struct {
	NodeID     string
	NodeName   string
	IPAddress  string
	Port       int
	PublicKey  string
	PrivateKey string
	Platform   string
	Version    string
}

type TenantInfo struct {
	TenantID   string
	TenantName string
	TenantURL  string
}

type UserInfo struct {
	UserID       string
	Email        string
	Name         string
	PasswordHash string
}

// New creates a new initializer instance
func New(logger logger.LoggerInterface) *Initializer {
	return NewWithConfig(logger, nil)
}

// NewWithVersion creates a new initializer instance with version information
func NewWithVersion(logger logger.LoggerInterface, version string) *Initializer {
	return NewWithConfigAndVersion(logger, nil, version)
}

// NewWithConfig creates a new initializer instance with configuration
func NewWithConfig(logger logger.LoggerInterface, config interface{}) *Initializer {
	return NewWithConfigAndVersion(logger, config, "unknown")
}

// NewWithConfigAndVersion creates a new initializer instance with configuration and version
func NewWithConfigAndVersion(logger logger.LoggerInterface, config interface{}, version string) *Initializer {
	// Initialize keyring manager with configuration if available
	var keyringPath string
	var masterPassword string
	var backend string = "auto"
	var groupID string = "default"

	// Try to extract keyring configuration using proper interfaces
	if config != nil {
		// Use proper interface-based type assertions
		if keyringProvider, ok := config.(configprovider.KeyringConfigProvider); ok {
			backend = keyringProvider.GetKeyringBackend()
			if path := keyringProvider.GetKeyringPath(); path != "" {
				keyringPath = path
			}
			if key := keyringProvider.GetKeyringMasterKey(); key != "" {
				masterPassword = key
			}
		}

		if instanceProvider, ok := config.(configprovider.InstanceConfigProvider); ok {
			groupID = instanceProvider.GetInstanceGroupID()
		}
	}

	// Fallback to defaults if not configured
	if keyringPath == "" {
		keyringPath = keyring.GetKeyringPathWithGroup(keyring.GetDefaultKeyringPath(), groupID)
	}
	if masterPassword == "" {
		masterPassword = keyring.GetMasterPasswordFromEnv()
	}
	if backend == "" {
		backend = "auto"
	}

	logger.Info("Initializing keyring manager with multi-instance support...")
	logger.Infof("Keyring path: %s", keyringPath)
	logger.Infof("Keyring backend: %s", backend)
	logger.Infof("Instance group: %s", groupID)

	km := keyring.NewKeyringManagerWithBackend(keyringPath, masterPassword, backend)
	logger.Info("Keyring manager initialized successfully")

	return &Initializer{
		logger:         logger,
		reader:         os.Stdin,
		keyringManager: km,
		config:         config,
		version:        version,
	}
}

// getKeyringServiceName returns the instance-aware keyring service name
func (i *Initializer) getKeyringServiceName(service string) string {
	if i.config != nil {
		// Use proper interface-based type assertion
		if serviceNameProvider, ok := i.config.(configprovider.ServiceNameProvider); ok {
			return serviceNameProvider.GetKeyringServiceName(service)
		}
	}

	// Fallback to default naming
	return fmt.Sprintf("redb-%s", service)
}

// getMeshExternalPort returns the mesh external port from configuration
func (i *Initializer) getMeshExternalPort() int {
	defaultPort := 10001 // Default mesh external port

	if i.config == nil {
		i.logger.Warnf("No config available, using default mesh external port %d", defaultPort)
		return defaultPort
	}

	// Try to use the ServiceConfigProvider interface
	if serviceConfigProvider, ok := i.config.(interface {
		GetServiceExternalPort(serviceName string) int
	}); ok {
		port := serviceConfigProvider.GetServiceExternalPort("mesh")
		if port > 0 {
			i.logger.Infof("Using mesh external port from configuration: %d", port)
			return port
		}
	}

	// Fallback to default port
	i.logger.Warnf("Could not read mesh external port from configuration, using default: %d", defaultPort)
	return defaultPort
}

// getPlatform returns the platform string (OS/Architecture)
func (i *Initializer) getPlatform() string {
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

// getVersion returns the application version
func (i *Initializer) getVersion() string {
	if i.version == "" || i.version == "unknown" {
		return "v0.0.1" // Default version
	}
	return i.version
}

// Initialize performs the complete node initialization process
func (i *Initializer) Initialize(ctx context.Context) error {
	i.logger.Info("Starting reDB node initialization...")

	// Step 1: Check database connectivity
	defaultCreds := &DatabaseCredentials{
		User:     DefaultPostgresUser,
		Password: DefaultPostgresPassword,
		Host:     DefaultPostgresHost,
		Port:     DefaultPostgresPort,
		Database: DefaultPostgresDatabase,
	}

	workingCreds, err := i.checkDatabaseConnectivity(ctx, defaultCreds)
	if err != nil {
		return fmt.Errorf("failed to establish database connectivity: %w", err)
	}

	// Step 2: Generate secure password for production database
	prodPassword, err := i.generateSecurePassword(32)
	if err != nil {
		return fmt.Errorf("failed to generate secure password: %w", err)
	}

	// Step 3: Store production password in keyring
	if err := i.storeProductionPassword(prodPassword); err != nil {
		return fmt.Errorf("failed to store production password: %w", err)
	}

	// Step 4: Create production database and user
	if err := i.createProductionDatabase(ctx, workingCreds, prodPassword); err != nil {
		return fmt.Errorf("failed to create production database: %w", err)
	}

	// Step 5: Create database schema
	prodCreds := &DatabaseCredentials{
		User:     i.getProductionDatabaseUser(),
		Password: prodPassword,
		Host:     workingCreds.Host,
		Port:     workingCreds.Port,
		Database: i.getProductionDatabaseName(),
	}

	if err := i.createDatabaseSchema(ctx, prodCreds); err != nil {
		return fmt.Errorf("failed to create database schema: %w", err)
	}

	// Step 6: Generate and store node keys
	nodeInfo, err := i.generateNodeKeys()
	if err != nil {
		return fmt.Errorf("failed to generate node keys: %w", err)
	}

	// Step 7: Insert local node details and set localidentity
	if err := i.createLocalNode(ctx, prodCreds, nodeInfo); err != nil {
		return fmt.Errorf("failed to create local node: %w", err)
	}

	// Step 8: Optionally create default tenant and user
	if i.promptYesNo("Would you like to create a default tenant and user?") {
		tenantInfo, userInfo, err := i.createTenantAndUser(ctx, prodCreds)
		if err != nil {
			return fmt.Errorf("failed to create tenant and user: %w", err)
		}

		// Step 9: Generate and store JWT secrets for the tenant
		if err := i.generateTenantJWTSecret(tenantInfo.TenantID); err != nil {
			return fmt.Errorf("failed to generate tenant JWT secret: %w", err)
		}

		// Step 10: Generate and store RSA keys for the tenant
		if err := i.generateTenantKeys(tenantInfo.TenantID); err != nil {
			return fmt.Errorf("failed to generate tenant RSA keys: %w", err)
		}

		i.logger.Infof("Successfully created tenant '%s' with user '%s'", tenantInfo.TenantName, userInfo.Email)
	}

	i.logger.Info("Node initialization completed successfully!")
	i.logger.Info("You can now start the supervisor service normally.")

	return nil
}

// AutoInitialize performs headless initialization without any user prompts
// This is designed for Docker/containerized environments where interactive prompts are not possible
func (i *Initializer) AutoInitialize(ctx context.Context) error {
	i.logger.Info("Starting reDB node auto-initialization (headless mode)...")

	// Step 1: Check database connectivity with credentials from environment or defaults
	defaultCreds := getDatabaseCredentialsFromEnv()

	workingCreds, err := i.checkDatabaseConnectivityHeadless(ctx, defaultCreds)
	if err != nil {
		return fmt.Errorf("failed to establish database connectivity: %w", err)
	}

	// Step 2: Generate secure password for production database
	prodPassword, err := i.generateSecurePassword(32)
	if err != nil {
		return fmt.Errorf("failed to generate secure password: %w", err)
	}

	// Step 3: Store production password in keyring
	if err := i.storeProductionPassword(prodPassword); err != nil {
		return fmt.Errorf("failed to store production password: %w", err)
	}

	// Step 4: Create production database and user
	if err := i.createProductionDatabase(ctx, workingCreds, prodPassword); err != nil {
		return fmt.Errorf("failed to create production database: %w", err)
	}

	// Step 5: Create database schema
	prodCreds := &DatabaseCredentials{
		User:     i.getProductionDatabaseUser(),
		Password: prodPassword,
		Host:     workingCreds.Host,
		Port:     workingCreds.Port,
		Database: i.getProductionDatabaseName(),
	}

	// Check if initialization is already complete
	isComplete, err := i.isInitializationComplete(ctx, prodCreds)
	if err != nil {
		return fmt.Errorf("failed to check initialization status: %w", err)
	}

	if isComplete {
		i.logger.Info("System is already fully initialized, skipping initialization steps")
		i.logger.Info("Node auto-initialization completed successfully!")
		i.logger.Info("Database schema and node setup completed.")
		i.logger.Info("Use the API to create the initial tenant and user.")
		i.logger.Info("You can now start the supervisor service normally.")
		return nil
	}

	// Step 6: Create database schema (idempotent)
	if err := i.createDatabaseSchema(ctx, prodCreds); err != nil {
		return fmt.Errorf("failed to create database schema: %w", err)
	}

	// Step 7: Generate and store node keys (idempotent)
	nodeInfo, err := i.generateNodeKeys()
	if err != nil {
		return fmt.Errorf("failed to generate node keys: %w", err)
	}

	// Step 8: Insert local node details and set localidentity (idempotent)
	if err := i.createLocalNode(ctx, prodCreds, nodeInfo); err != nil {
		return fmt.Errorf("failed to create local node: %w", err)
	}

	i.logger.Info("Node auto-initialization completed successfully!")
	i.logger.Info("Database schema and node setup completed.")
	i.logger.Info("Use the API to create the initial tenant and user.")
	i.logger.Info("You can now start the supervisor service normally.")

	return nil
}

// isInitializationComplete checks if the system has been fully initialized
func (i *Initializer) isInitializationComplete(ctx context.Context, creds *DatabaseCredentials) (bool, error) {
	// Connect to database
	connConfig, err := pgx.ParseConfig("")
	if err != nil {
		return false, fmt.Errorf("failed to create connection config: %w", err)
	}

	connConfig.Host = creds.Host
	connConfig.Port = uint16(creds.Port)
	connConfig.Database = creds.Database
	connConfig.User = creds.User
	connConfig.Password = creds.Password
	connConfig.ConnectTimeout = 30 * time.Second

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return false, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Check if key tables exist
	var schemaExists bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name IN ('localidentity', 'tenants', 'users', 'nodes')
		)
	`).Scan(&schemaExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if schema exists: %w", err)
	}

	if !schemaExists {
		return false, nil
	}

	// Check if local node exists
	var localNodeExists bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM localidentity li
			JOIN nodes n ON n.node_id = li.identity_id
		)
	`).Scan(&localNodeExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if local node exists: %w", err)
	}

	if !localNodeExists {
		return false, nil
	}

	// Check if node keys exist in keyring
	nodeServiceName := i.getKeyringServiceName("node")
	_, err = i.keyringManager.Get(nodeServiceName, NodePrivateKeyKey)
	if err != nil {
		return false, nil
	}

	_, err = i.keyringManager.Get(nodeServiceName, NodePublicKeyKey)
	if err != nil {
		return false, nil
	}

	// Check if production password exists in keyring
	databaseServiceName := i.getKeyringServiceName("database")
	_, err = i.keyringManager.Get(databaseServiceName, DatabasePasswordKey)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// storeProductionPassword stores the production database password in keyring
func (i *Initializer) storeProductionPassword(password string) error {
	i.logger.Info("Storing production database password in keyring...")

	serviceName := i.getKeyringServiceName("database")

	// Check if password already exists
	existingPassword, err := i.keyringManager.Get(serviceName, DatabasePasswordKey)
	if err == nil && existingPassword != "" {
		i.logger.Info("Production database password already exists in keyring")
		return nil
	}

	return i.keyringManager.Set(serviceName, DatabasePasswordKey, password)
}

// generateNodeKeys generates RSA key pair for the node and stores them in keyring
func (i *Initializer) generateNodeKeys() (*NodeInfo, error) {
	i.logger.Info("Generating node RSA key pair...")

	nodeServiceName := i.getKeyringServiceName("node")

	// Check if keys already exist in keyring
	existingPrivateKey, err := i.keyringManager.Get(nodeServiceName, NodePrivateKeyKey)
	if err == nil && existingPrivateKey != "" {
		existingPublicKey, err := i.keyringManager.Get(nodeServiceName, NodePublicKeyKey)
		if err == nil && existingPublicKey != "" {
			i.logger.Info("Node keys already exist in keyring, using existing keys")

			// Generate node information with existing keys
			nodeInfo := &NodeInfo{
				PublicKey:  existingPublicKey,
				PrivateKey: existingPrivateKey,
				Port:       i.getMeshExternalPort(),
				Platform:   i.getPlatform(),
				Version:    i.getVersion(),
			}

			// Get local IP address
			nodeInfo.IPAddress, err = i.getLocalIPAddress()
			if err != nil {
				i.logger.Warnf("Failed to detect local IP address: %v", err)
				nodeInfo.IPAddress = "127.0.0.1" // Fallback to localhost
			}

			return nodeInfo, nil
		}
	}

	// Generate new RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Encode private key
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	// Encode public key
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	// Store keys in keyring
	if err := i.keyringManager.Set(nodeServiceName, NodePrivateKeyKey, string(privateKeyPEM)); err != nil {
		return nil, fmt.Errorf("failed to store private key: %w", err)
	}

	if err := i.keyringManager.Set(nodeServiceName, NodePublicKeyKey, string(publicKeyPEM)); err != nil {
		return nil, fmt.Errorf("failed to store public key: %w", err)
	}

	// Generate node information
	nodeInfo := &NodeInfo{
		PublicKey:  string(publicKeyPEM),
		PrivateKey: string(privateKeyPEM),
		Port:       i.getMeshExternalPort(),
		Platform:   i.getPlatform(),
		Version:    i.getVersion(),
	}

	// Get local IP address
	nodeInfo.IPAddress, err = i.getLocalIPAddress()
	if err != nil {
		i.logger.Warnf("Failed to detect local IP address: %v", err)
		nodeInfo.IPAddress = "127.0.0.1" // Fallback to localhost
	}

	i.logger.Info("Successfully generated and stored node keys")
	return nodeInfo, nil
}

// generateTenantJWTSecret generates and stores JWT secret for the tenant
func (i *Initializer) generateTenantJWTSecret(tenantID string) error {
	i.logger.Info("Generating JWT secret for tenant...")

	// Generate random secret (64 bytes)
	secretBytes := make([]byte, 64)
	if _, err := rand.Read(secretBytes); err != nil {
		return fmt.Errorf("failed to generate random secret: %w", err)
	}

	// Encode secret as base64 for storage
	secretString := base64.StdEncoding.EncodeToString(secretBytes)

	// Store in keyring using the same pattern as the security service
	securityServiceName := i.getKeyringServiceName("security")
	secretKey := fmt.Sprintf("%s-%s", JWTSecretKeyPrefix, tenantID)
	err := i.keyringManager.Set(securityServiceName, secretKey, secretString)
	if err != nil {
		return fmt.Errorf("failed to store tenant JWT secret: %w", err)
	}

	i.logger.Info("Successfully generated and stored tenant JWT secret")
	return nil
}

// generateTenantKeys generates and stores RSA public and private keys for the tenant
func (i *Initializer) generateTenantKeys(tenantID string) error {
	i.logger.Info("Generating RSA public and private keys for tenant...")

	// Generate RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	// Encode private key
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	// Encode public key
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return fmt.Errorf("failed to marshal public key: %w", err)
	}

	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	// Create tenant-specific key names
	privateKeyName := fmt.Sprintf("%s-%s", TenantPrivateKeyKeyPrefix, tenantID)
	publicKeyName := fmt.Sprintf("%s-%s", TenantPublicKeyKeyPrefix, tenantID)

	// Store keys in keyring
	securityServiceName := i.getKeyringServiceName("security")
	if err := i.keyringManager.Set(securityServiceName, privateKeyName, string(privateKeyPEM)); err != nil {
		return fmt.Errorf("failed to store private key: %w", err)
	}

	if err := i.keyringManager.Set(securityServiceName, publicKeyName, string(publicKeyPEM)); err != nil {
		return fmt.Errorf("failed to store public key: %w", err)
	}

	i.logger.Info("Successfully generated and stored tenant RSA keys")
	return nil
}

// GetDatabasePassword retrieves the production database password from keyring
func (i *Initializer) GetDatabasePassword() (string, error) {
	serviceName := i.getKeyringServiceName("database")
	password, err := i.keyringManager.Get(serviceName, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve database password from keyring: %w", err)
	}
	return password, nil
}

// GetNodeKeys retrieves the node's RSA key pair from keyring
func (i *Initializer) GetNodeKeys() (publicKey, privateKey string, err error) {
	nodeServiceName := i.getKeyringServiceName("node")

	publicKey, err = i.keyringManager.Get(nodeServiceName, NodePublicKeyKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to retrieve public key from keyring: %w", err)
	}

	privateKey, err = i.keyringManager.Get(nodeServiceName, NodePrivateKeyKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to retrieve private key from keyring: %w", err)
	}

	return publicKey, privateKey, nil
}

// GetTenantJWTSecret retrieves JWT secret for a specific tenant
func (i *Initializer) GetTenantJWTSecret(tenantID string) ([]byte, error) {
	securityServiceName := i.getKeyringServiceName("security")
	secretKey := fmt.Sprintf("%s-%s", JWTSecretKeyPrefix, tenantID)
	secretString, err := i.keyringManager.Get(securityServiceName, secretKey)
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

// Rest of the initialize.go methods remain the same...
// (checkDatabaseConnectivity, testDatabaseConnection, generateSecurePassword, etc.)

// checkDatabaseConnectivity verifies database connection with default or prompted credentials
func (i *Initializer) checkDatabaseConnectivity(ctx context.Context, defaultCreds *DatabaseCredentials) (*DatabaseCredentials, error) {
	i.logger.Info("Checking database connectivity...")

	// Try default credentials first
	if err := i.testDatabaseConnection(ctx, defaultCreds); err == nil {
		i.logger.Info("Successfully connected to database with default credentials")
		return defaultCreds, nil
	}

	i.logger.Warn("Could not connect with default credentials, prompting for custom credentials...")

	// Prompt for custom credentials
	creds := &DatabaseCredentials{
		Host:     defaultCreds.Host,
		Port:     defaultCreds.Port,
		Database: defaultCreds.Database,
	}

	fmt.Printf("Enter PostgreSQL username [%s]: ", DefaultPostgresUser)
	if username := i.readInput(); username != "" {
		creds.User = username
	} else {
		creds.User = DefaultPostgresUser
	}

	fmt.Print("Enter PostgreSQL password: ")
	password, err := i.readPassword()
	if err != nil {
		return nil, fmt.Errorf("failed to read password: %w", err)
	}
	creds.Password = password

	fmt.Printf("Enter PostgreSQL host [%s]: ", DefaultPostgresHost)
	if host := i.readInput(); host != "" {
		creds.Host = host
	}

	fmt.Printf("Enter PostgreSQL port [%d]: ", DefaultPostgresPort)
	if portStr := i.readInput(); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			creds.Port = port
		}
	}

	if err := i.testDatabaseConnection(ctx, creds); err != nil {
		return nil, fmt.Errorf("failed to connect with provided credentials: %w", err)
	}

	i.logger.Info("Successfully connected to database with provided credentials")
	return creds, nil
}

// checkDatabaseConnectivityHeadless performs headless database connectivity check
// In headless mode, we only try default credentials and fail if they don't work
func (i *Initializer) checkDatabaseConnectivityHeadless(ctx context.Context, defaultCreds *DatabaseCredentials) (*DatabaseCredentials, error) {
	i.logger.Info("Checking database connectivity (headless mode)...")

	// Try default credentials first
	if err := i.testDatabaseConnection(ctx, defaultCreds); err == nil {
		i.logger.Info("Successfully connected to database with default credentials")
		return defaultCreds, nil
	}

	// In headless mode, we don't prompt for credentials - we fail if defaults don't work
	return nil, fmt.Errorf("failed to connect with database credentials in headless mode. Please ensure PostgreSQL is running and configure database connection via environment variables: %s, %s, %s, %s, %s", EnvPostgresUser, EnvPostgresPassword, EnvPostgresHost, EnvPostgresPort, EnvPostgresDatabase)
}

// testDatabaseConnection tests if we can connect to the database
func (i *Initializer) testDatabaseConnection(ctx context.Context, creds *DatabaseCredentials) error {
	// Use pgxpool.ParseConfig to handle special characters in passwords
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return err
	}

	poolConfig.ConnConfig.Host = creds.Host
	poolConfig.ConnConfig.Port = uint16(creds.Port)
	poolConfig.ConnConfig.Database = creds.Database
	poolConfig.ConnConfig.User = creds.User
	poolConfig.ConnConfig.Password = creds.Password
	poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()

	return pool.Ping(ctx)
}

// generateSecurePassword generates a cryptographically secure random password
func (i *Initializer) generateSecurePassword(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}

	return string(b), nil
}

// createProductionDatabase creates the production database and user
func (i *Initializer) createProductionDatabase(ctx context.Context, adminCreds *DatabaseCredentials, prodPassword string) error {
	i.logger.Info("Creating production database and user...")

	// Get the production database name and user
	prodDatabaseName := i.getProductionDatabaseName()
	prodDatabaseUser := i.getProductionDatabaseUser()

	// Use pgx.ParseConfig to handle special characters in passwords
	connConfig, err := pgx.ParseConfig("")
	if err != nil {
		return fmt.Errorf("failed to create connection config: %w", err)
	}

	connConfig.Host = adminCreds.Host
	connConfig.Port = uint16(adminCreds.Port)
	connConfig.Database = adminCreds.Database
	connConfig.User = adminCreds.User
	connConfig.Password = adminCreds.Password
	connConfig.ConnectTimeout = 30 * time.Second

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Check if database already exists
	var exists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", prodDatabaseName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	}

	if exists {
		i.logger.Warnf("Database '%s' already exists, skipping database creation", prodDatabaseName)
	} else {
		// Create database
		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", prodDatabaseName))
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		i.logger.Infof("Created database '%s'", prodDatabaseName)
	}

	// Check if user already exists
	err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_user WHERE usename = $1)", prodDatabaseUser).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if user exists: %w", err)
	}

	if exists {
		i.logger.Warnf("User '%s' already exists, updating password", prodDatabaseUser)
		// Update password for existing user
		_, err = conn.Exec(ctx, fmt.Sprintf("ALTER USER %s WITH ENCRYPTED PASSWORD '%s'", prodDatabaseUser, prodPassword))
		if err != nil {
			return fmt.Errorf("failed to update user password: %w", err)
		}
	} else {
		// Create user
		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE USER %s WITH ENCRYPTED PASSWORD '%s'", prodDatabaseUser, prodPassword))
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}
		i.logger.Infof("Created user '%s'", prodDatabaseUser)
	}

	// Grant the production user to admin so admin can set ownership
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT %s TO %s", prodDatabaseUser, adminCreds.User))
	if err != nil {
		return fmt.Errorf("failed to grant user to admin: %w", err)
	}

	// Grant privileges and set ownership
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", prodDatabaseName, prodDatabaseUser))
	if err != nil {
		return fmt.Errorf("failed to grant privileges: %w", err)
	}

	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", prodDatabaseName, prodDatabaseUser))
	if err != nil {
		return fmt.Errorf("failed to set database owner: %w", err)
	}

	i.logger.Info("Successfully created production database and user")
	return nil
}

// createDatabaseSchema creates the database schema using the provided SQL
func (i *Initializer) createDatabaseSchema(ctx context.Context, creds *DatabaseCredentials) error {
	i.logger.Info("Creating database schema from embedded schema definition...")

	// Connect to production database using pgx.ParseConfig to handle special characters
	connConfig, err := pgx.ParseConfig("")
	if err != nil {
		return fmt.Errorf("failed to create connection config: %w", err)
	}

	connConfig.Host = creds.Host
	connConfig.Port = uint16(creds.Port)
	connConfig.Database = creds.Database
	connConfig.User = creds.User
	connConfig.Password = creds.Password
	connConfig.ConnectTimeout = 30 * time.Second

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Check if schema is already initialized by looking for key tables
	var schemaExists bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name IN ('localidentity', 'tenants', 'users', 'nodes')
		)
	`).Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("failed to check if schema exists: %w", err)
	}

	if schemaExists {
		i.logger.Info("Database schema already exists, skipping schema creation")
		return nil
	}

	// Create the ulid domain separately (may fail on shared PostgreSQL instances)
	if err := i.createUlidDomainSafely(ctx, conn); err != nil {
		return fmt.Errorf("failed to create ulid domain: %w", err)
	}

	// Execute the rest of the schema (everything else should work fine)
	_, err = conn.Exec(ctx, DatabaseSchema)
	if err != nil {
		return fmt.Errorf("failed to create database schema: %w", err)
	}

	_, err = conn.Exec(ctx, DatabaseIndexes)
	if err != nil {
		return fmt.Errorf("failed to create database indexes: %w", err)
	}

	i.logger.Info("Successfully created database schema and indexes")
	return nil
}

// createUlidDomainSafely creates the ulid domain, handling conflicts gracefully
// This is the only schema object that can conflict on shared PostgreSQL instances
func (i *Initializer) createUlidDomainSafely(ctx context.Context, conn *pgx.Conn) error {
	ulidDomainSQL := `CREATE DOMAIN ulid AS TEXT
CHECK (
    -- Check overall format
    VALUE ~ '^[a-z]{2,10}_[0-9A-HJKMNP-TV-Z]{26}$'
    AND
    -- Ensure prefix is from allowed list
    substring(VALUE from '^([a-z]+)_') IN (
        'mesh', 'node', 'route', 'region', 'tenant', 'user', 'group', 'role', 'perm', 'pol', 'ws', 'env', 'instance', 'db', 'repo', 'branch', 'commit', 'map', 'maprule','rel', 'transform', 'mcpserver', 'mcpres', 'mcptool', 'mcpprompt', 'audit', 'satellite', 'anchor', 'template', 'apitoken', 'cdcs', 'integration', 'intjob'
    )
);`

	_, err := conn.Exec(ctx, ulidDomainSQL)
	if err != nil {
		// Check if it's a "already exists" error
		if strings.Contains(err.Error(), "already exists") {
			i.logger.Warnf("ULID domain already exists (shared PostgreSQL instance), continuing with initialization")
			return nil // Not an error, just means another instance created it first
		}
		// For other errors, fail
		return fmt.Errorf("failed to create ulid domain: %w", err)
	}

	i.logger.Info("Successfully created ulid domain")
	return nil
}

// generateUniqueNodeID generates a unique node ID using timestamp and randomness
// This ensures different nodes initialized independently have different IDs
func (i *Initializer) generateUniqueNodeID() int64 {
	// Use current time in milliseconds since epoch as base
	now := time.Now().UnixMilli() // Returns milliseconds since 1970

	// Generate 4 random bytes for additional uniqueness
	randomBytes := make([]byte, 4)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp + process ID if random fails
		randomBytes = []byte{
			byte(os.Getpid() >> 24),
			byte(os.Getpid() >> 16),
			byte(os.Getpid() >> 8),
			byte(os.Getpid()),
		}
	}

	// Combine: use lower 40 bits of timestamp + 24 bits of random
	// This gives us unique IDs while staying in positive int64 range
	// Format: [timestamp(40 bits)][random(24 bits)]
	timestamp := uint64(now) & 0xFFFFFFFFFF                                                   // 40 bits
	random := uint64(randomBytes[0])<<16 | uint64(randomBytes[1])<<8 | uint64(randomBytes[2]) // 24 bits

	nodeID := int64((timestamp << 24) | random)

	// Ensure it's positive (though it should be with our bit masking)
	if nodeID < 0 {
		nodeID = -nodeID
	}

	// Ensure minimum value (avoid very small IDs)
	if nodeID < 1000 {
		nodeID = 1000 + nodeID
	}

	i.logger.Infof("Generated unique node ID: %d (timestamp: %d, random: %d)", nodeID, timestamp, random)
	return nodeID
}

// getLocalIPAddress attempts to get the local machine's IP address
func (i *Initializer) getLocalIPAddress() (string, error) {
	// Use a timeout to prevent hanging when there's no internet connection
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
	}

	conn, err := dialer.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

// createLocalNode inserts the local node into the database and sets localidentity
func (i *Initializer) createLocalNode(ctx context.Context, creds *DatabaseCredentials, nodeInfo *NodeInfo) error {
	i.logger.Info("Creating local node entry in database...")

	// Use pgxpool.ParseConfig to handle special characters in passwords
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return fmt.Errorf("failed to create connection config: %w", err)
	}

	poolConfig.ConnConfig.Host = creds.Host
	poolConfig.ConnConfig.Port = uint16(creds.Port)
	poolConfig.ConnConfig.Database = creds.Database
	poolConfig.ConnConfig.User = creds.User
	poolConfig.ConnConfig.Password = creds.Password
	poolConfig.ConnConfig.ConnectTimeout = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Check if local node already exists
	var existingNodeID int64
	var existingNodeName string
	err = pool.QueryRow(ctx, `
		SELECT li.identity_id, n.node_name
		FROM localidentity li
		JOIN nodes n ON n.node_id = li.identity_id
		LIMIT 1
	`).Scan(&existingNodeID, &existingNodeName)
	if err == nil {
		// Local node already exists
		i.logger.Infof("Local node already exists: '%s' with ID '%d'", existingNodeName, existingNodeID)
		nodeInfo.NodeID = fmt.Sprintf("%d", existingNodeID)
		nodeInfo.NodeName = existingNodeName
		return nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("failed to check for existing local node: %w", err)
	}

	// No existing local node, create new one
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Generate a unique node ID using timestamp + random
	// This ensures different nodes initialized at different times or on different systems have unique IDs
	nodeID := i.generateUniqueNodeID()

	// Create a readable node name from the generated ID
	nodeName := fmt.Sprintf("node-%d", nodeID)

	// Insert node into nodes table with explicit node_id
	err = tx.QueryRow(ctx, `
		INSERT INTO nodes (node_id, node_name, node_description, node_public_key, ip_address, port, node_platform, node_version, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING node_id
	`, nodeID, nodeName, "Local node", []byte(nodeInfo.PublicKey), nodeInfo.IPAddress, nodeInfo.Port, nodeInfo.Platform, nodeInfo.Version, "STATUS_ACTIVE").Scan(&nodeID)
	if err != nil {
		return fmt.Errorf("failed to insert node: %w", err)
	}

	// Set localidentity
	_, err = tx.Exec(ctx, `
		INSERT INTO localidentity (identity_id) VALUES ($1)
		ON CONFLICT (identity_id) DO NOTHING
	`, nodeID)
	if err != nil {
		return fmt.Errorf("failed to set local identity: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	nodeInfo.NodeID = fmt.Sprintf("%d", nodeID)
	nodeInfo.NodeName = nodeName

	i.logger.Infof("Successfully created local node '%s' with ID '%d'", nodeName, nodeID)
	return nil
}

// createTenantAndUser prompts for and creates a default tenant and user
func (i *Initializer) createTenantAndUser(ctx context.Context, creds *DatabaseCredentials) (*TenantInfo, *UserInfo, error) {
	i.logger.Info("Creating default tenant and user...")

	// Prompt for tenant information
	fmt.Print("Enter tenant name: ")
	tenantName := i.readInput()
	if tenantName == "" {
		return nil, nil, fmt.Errorf("tenant name is required")
	}

	// Generate the tenant URL by taking the name and removing spaces and non-alphanumeric characters
	tenantURL := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return r
		}
		return -1
	}, tenantName)

	// Prompt for user information
	fmt.Print("Enter admin user email: ")
	userEmail := i.readInput()
	if userEmail == "" {
		return nil, nil, fmt.Errorf("user email is required")
	}

	// Set the username as email, do not prompt for it
	userName := userEmail

	userPassword, err := i.readPasswordWithConfirmation("Enter admin user password: ")
	if err != nil {
		return nil, nil, err
	}

	// Hash password
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(userPassword), bcrypt.DefaultCost)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// Connect to database using pgxpool.ParseConfig to handle special characters
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create connection config: %w", err)
	}

	poolConfig.ConnConfig.Host = creds.Host
	poolConfig.ConnConfig.Port = uint16(creds.Port)
	poolConfig.ConnConfig.Database = creds.Database
	poolConfig.ConnConfig.User = creds.User
	poolConfig.ConnConfig.Password = creds.Password
	poolConfig.ConnConfig.ConnectTimeout = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Generate tenant ID
	var tenantID string
	err = tx.QueryRow(ctx, "SELECT generate_ulid('tenant')").Scan(&tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate tenant ULID: %w", err)
	}

	// Insert tenant
	_, err = tx.Exec(ctx, `
		INSERT INTO tenants (tenant_id, tenant_name, tenant_description, tenant_url, status)
		VALUES ($1, $2, $3, $4, $5)
	`, tenantID, tenantName, "Default tenant", tenantURL, "STATUS_ACTIVE")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to insert tenant: %w", err)
	}

	// Generate user ID
	var userID string
	err = tx.QueryRow(ctx, "SELECT generate_ulid('user')").Scan(&userID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate user ULID: %w", err)
	}

	// Insert user
	_, err = tx.Exec(ctx, `
		INSERT INTO users (user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, userID, tenantID, userEmail, userName, string(passwordHash), true)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to insert user: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	tenantInfo := &TenantInfo{
		TenantID:   tenantID,
		TenantName: tenantName,
		TenantURL:  tenantURL,
	}

	userInfo := &UserInfo{
		UserID:       userID,
		Email:        userEmail,
		Name:         userName,
		PasswordHash: string(passwordHash),
	}

	i.logger.Infof("Successfully created tenant '%s' and user '%s'", tenantName, userEmail)
	return tenantInfo, userInfo, nil
}

// createTenantAndUserHeadless creates a default tenant and user without prompting
func (i *Initializer) createTenantAndUserHeadless(ctx context.Context, creds *DatabaseCredentials) (*TenantInfo, *UserInfo, error) {
	i.logger.Info("Creating default tenant and user (headless mode)...")

	// Get tenant and user information from environment variables or use defaults
	tenantName := os.Getenv(EnvDefaultTenantName)
	if tenantName == "" {
		tenantName = "default-tenant"
	}

	// Generate the tenant URL by taking the name and removing spaces and non-alphanumeric characters
	tenantURL := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return r
		}
		return -1
	}, tenantName)

	// Generate user information
	userEmail := os.Getenv(EnvDefaultUserEmail)
	if userEmail == "" {
		userEmail = "admin@example.com"
	}
	userName := userEmail

	// Get password from environment variable or generate a secure one
	userPassword := os.Getenv(EnvDefaultUserPassword)
	if userPassword == "" {
		var err error
		userPassword, err = i.generateSecurePassword(16) // Shorter password for headless mode
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate secure password for headless user: %w", err)
		}
		// Log the generated password so user can see it
		i.logger.Infof("Generated default user password: %s", userPassword)
		i.logger.Info("IMPORTANT: Save this password! It will not be shown again.")
	} else {
		i.logger.Info("Using password from environment variable")
	}

	// Hash password
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(userPassword), bcrypt.DefaultCost)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to hash password for headless user: %w", err)
	}

	// Connect to database using pgxpool.ParseConfig to handle special characters
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create connection config for headless user: %w", err)
	}

	poolConfig.ConnConfig.Host = creds.Host
	poolConfig.ConnConfig.Port = uint16(creds.Port)
	poolConfig.ConnConfig.Database = creds.Database
	poolConfig.ConnConfig.User = creds.User
	poolConfig.ConnConfig.Password = creds.Password
	poolConfig.ConnConfig.ConnectTimeout = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to database for headless user: %w", err)
	}
	defer pool.Close()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction for headless user: %w", err)
	}
	defer tx.Rollback(ctx)

	// Generate tenant ID
	var tenantID string
	err = tx.QueryRow(ctx, "SELECT generate_ulid('tenant')").Scan(&tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate tenant ULID for headless user: %w", err)
	}

	// Insert tenant
	_, err = tx.Exec(ctx, `
		INSERT INTO tenants (tenant_id, tenant_name, tenant_description, tenant_url, status)
		VALUES ($1, $2, $3, $4, $5)
	`, tenantID, tenantName, "Default tenant", tenantURL, "STATUS_ACTIVE")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to insert tenant for headless user: %w", err)
	}

	// Generate user ID
	var userID string
	err = tx.QueryRow(ctx, "SELECT generate_ulid('user')").Scan(&userID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate user ULID for headless user: %w", err)
	}

	// Insert user
	_, err = tx.Exec(ctx, `
		INSERT INTO users (user_id, tenant_id, user_email, user_name, user_password_hash, user_enabled)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, userID, tenantID, userEmail, userName, string(passwordHash), true)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to insert user for headless user: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction for headless user: %w", err)
	}

	tenantInfo := &TenantInfo{
		TenantID:   tenantID,
		TenantName: tenantName,
		TenantURL:  tenantURL,
	}

	userInfo := &UserInfo{
		UserID:       userID,
		Email:        userEmail,
		Name:         userName,
		PasswordHash: string(passwordHash),
	}

	i.logger.Infof("Successfully created tenant '%s' and user '%s' (headless mode)", tenantName, userEmail)
	return tenantInfo, userInfo, nil
}

// promptYesNo prompts the user for a yes/no answer
func (i *Initializer) promptYesNo(question string) bool {
	fmt.Printf("%s (y/N): ", question)
	response := strings.ToLower(strings.TrimSpace(i.readInput()))
	return response == "y" || response == "yes"
}

// readInput reads a line of input from the reader
func (i *Initializer) readInput() string {
	scanner := bufio.NewScanner(i.reader)
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text())
	}
	return ""
}

// readPassword reads a password from stdin with masking
func (i *Initializer) readPassword() (string, error) {
	// Check if we're reading from stdin (not a test reader)
	if i.reader != os.Stdin {
		// For testing, fall back to regular input
		return i.readInput(), nil
	}

	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println() // Print newline after password input
	return string(bytePassword), nil
}

// readPasswordWithConfirmation reads a password twice and ensures they match
func (i *Initializer) readPasswordWithConfirmation(prompt string) (string, error) {
	for {
		fmt.Print(prompt)
		password1, err := i.readPassword()
		if err != nil {
			return "", fmt.Errorf("failed to read password: %w", err)
		}

		if password1 == "" {
			return "", fmt.Errorf("password cannot be empty")
		}

		fmt.Print("Confirm password: ")
		password2, err := i.readPassword()
		if err != nil {
			return "", fmt.Errorf("failed to read password confirmation: %w", err)
		}

		if password1 == password2 {
			return password1, nil
		}

		fmt.Println("Passwords do not match. Please try again.")
	}
}

// getDatabaseCredentialsFromEnv returns database credentials from environment variables
// with fallback to default values
func getDatabaseCredentialsFromEnv() *DatabaseCredentials {
	creds := &DatabaseCredentials{
		User:     DefaultPostgresUser,
		Password: DefaultPostgresPassword,
		Host:     DefaultPostgresHost,
		Port:     DefaultPostgresPort,
		Database: DefaultPostgresDatabase,
	}

	// Override with environment variables if they exist
	if user := os.Getenv(EnvPostgresUser); user != "" {
		creds.User = user
	}
	if password := os.Getenv(EnvPostgresPassword); password != "" {
		creds.Password = password
	}
	if host := os.Getenv(EnvPostgresHost); host != "" {
		creds.Host = host
	}
	if portStr := os.Getenv(EnvPostgresPort); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			creds.Port = port
		}
	}
	if database := os.Getenv(EnvPostgresDatabase); database != "" {
		creds.Database = database
	}

	return creds
}

// InitializeWithSingleTenant performs initialization with automatic single-tenant setup
func (i *Initializer) InitializeWithSingleTenant(ctx context.Context, tenantID, tenantName, tenantURL string) error {
	i.logger.Info("Starting reDB node initialization in single-tenant mode...")

	// Step 1: Check database connectivity
	defaultCreds := &DatabaseCredentials{
		User:     DefaultPostgresUser,
		Password: DefaultPostgresPassword,
		Host:     DefaultPostgresHost,
		Port:     DefaultPostgresPort,
		Database: DefaultPostgresDatabase,
	}

	workingCreds, err := i.checkDatabaseConnectivity(ctx, defaultCreds)
	if err != nil {
		return fmt.Errorf("failed to establish database connectivity: %w", err)
	}

	// Step 2: Generate secure password for production database
	prodPassword, err := i.generateSecurePassword(32)
	if err != nil {
		return fmt.Errorf("failed to generate secure password: %w", err)
	}

	// Step 3: Store production password in keyring
	if err := i.storeProductionPassword(prodPassword); err != nil {
		return fmt.Errorf("failed to store production password: %w", err)
	}

	// Step 4: Create production database and user
	if err := i.createProductionDatabase(ctx, workingCreds, prodPassword); err != nil {
		return fmt.Errorf("failed to create production database: %w", err)
	}

	// Step 5: Create database schema
	prodCreds := &DatabaseCredentials{
		User:     i.getProductionDatabaseUser(),
		Password: prodPassword,
		Host:     workingCreds.Host,
		Port:     workingCreds.Port,
		Database: i.getProductionDatabaseName(),
	}

	if err := i.createDatabaseSchema(ctx, prodCreds); err != nil {
		return fmt.Errorf("failed to create database schema: %w", err)
	}

	// Step 6: Generate and store node keys
	nodeInfo, err := i.generateNodeKeys()
	if err != nil {
		return fmt.Errorf("failed to generate node keys: %w", err)
	}

	// Step 7: Insert local node details and set localidentity
	if err := i.createLocalNode(ctx, prodCreds, nodeInfo); err != nil {
		return fmt.Errorf("failed to create local node: %w", err)
	}

	// Step 8: Create default tenant automatically for single-tenant mode
	tenantInfo, err := i.createDefaultTenant(ctx, prodCreds, tenantID, tenantName, tenantURL)
	if err != nil {
		return fmt.Errorf("failed to create default tenant: %w", err)
	}

	// Step 9: Generate and store JWT secrets for the tenant
	if err := i.generateTenantJWTSecret(tenantInfo.TenantID); err != nil {
		return fmt.Errorf("failed to generate tenant JWT secret: %w", err)
	}

	// Step 10: Generate and store RSA keys for the tenant
	if err := i.generateTenantKeys(tenantInfo.TenantID); err != nil {
		return fmt.Errorf("failed to generate tenant RSA keys: %w", err)
	}

	i.logger.Infof("Successfully created default tenant '%s' for single-tenant mode", tenantInfo.TenantName)
	i.logger.Info("Node initialization completed successfully!")
	i.logger.Info("You can now start the supervisor service normally.")
	i.logger.Info("Use the API to create the initial user for the default tenant.")

	return nil
}

// createDefaultTenant creates the default tenant for single-tenant mode
func (i *Initializer) createDefaultTenant(ctx context.Context, creds *DatabaseCredentials, tenantID, tenantName, tenantURL string) (*TenantInfo, error) {
	i.logger.Info("Creating default tenant for single-tenant mode...")

	// Connect to database using pgxpool.ParseConfig to handle special characters
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("failed to create connection config: %w", err)
	}

	poolConfig.ConnConfig.Host = creds.Host
	poolConfig.ConnConfig.Port = uint16(creds.Port)
	poolConfig.ConnConfig.Database = creds.Database
	poolConfig.ConnConfig.User = creds.User
	poolConfig.ConnConfig.Password = creds.Password
	poolConfig.ConnConfig.ConnectTimeout = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	// Check if tenant already exists by URL (since we'll generate a new ID)
	var existingTenantID string
	err = pool.QueryRow(ctx, "SELECT tenant_id FROM tenants WHERE tenant_url = $1 LIMIT 1", tenantURL).Scan(&existingTenantID)
	if err == nil {
		i.logger.Infof("Default tenant already exists with ID: %s", existingTenantID)
		return &TenantInfo{
			TenantID:   existingTenantID,
			TenantName: tenantName,
			TenantURL:  tenantURL,
		}, nil
	}

	// Generate a proper ULID for the tenant
	var generatedTenantID string
	err = pool.QueryRow(ctx, "SELECT generate_ulid('tenant')").Scan(&generatedTenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate tenant ULID: %w", err)
	}

	// Create new tenant with the generated ULID
	_, err = pool.Exec(ctx, `
		INSERT INTO tenants (tenant_id, tenant_name, tenant_description, tenant_url, status)
		VALUES ($1, $2, $3, $4, $5)
	`, generatedTenantID, tenantName, "Default tenant for single-tenant mode", tenantURL, "STATUS_ACTIVE")
	if err != nil {
		return nil, fmt.Errorf("failed to insert default tenant: %w", err)
	}

	tenantInfo := &TenantInfo{
		TenantID:   generatedTenantID,
		TenantName: tenantName,
		TenantURL:  tenantURL,
	}

	i.logger.Infof("Successfully created default tenant '%s' with ID '%s'", tenantName, generatedTenantID)
	return tenantInfo, nil
}
