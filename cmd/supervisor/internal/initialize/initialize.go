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

// getProductionDatabaseName returns the production database name from environment or defaults
func getProductionDatabaseName() string {
	if dbName := os.Getenv("REDB_DATABASE_NAME"); dbName != "" {
		return dbName
	}
	return ProductionDatabase
}

type Initializer struct {
	logger         logger.LoggerInterface
	reader         io.Reader
	keyringManager *keyring.KeyringManager
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
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()

	logger.Info("Initializing keyring manager...")
	km := keyring.NewKeyringManager(keyringPath, masterPassword)
	logger.Info("Keyring manager initialized successfully")

	return &Initializer{
		logger:         logger,
		reader:         os.Stdin,
		keyringManager: km,
	}
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
		User:     ProductionUser,
		Password: prodPassword,
		Host:     workingCreds.Host,
		Port:     workingCreds.Port,
		Database: getProductionDatabaseName(),
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
		User:     ProductionUser,
		Password: prodPassword,
		Host:     workingCreds.Host,
		Port:     workingCreds.Port,
		Database: getProductionDatabaseName(),
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
	_, err = i.keyringManager.Get(NodeKeyringService, NodePrivateKeyKey)
	if err != nil {
		return false, nil
	}

	_, err = i.keyringManager.Get(NodeKeyringService, NodePublicKeyKey)
	if err != nil {
		return false, nil
	}

	// Check if production password exists in keyring
	_, err = i.keyringManager.Get(DatabaseKeyringService, DatabasePasswordKey)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// storeProductionPassword stores the production database password in keyring
func (i *Initializer) storeProductionPassword(password string) error {
	i.logger.Info("Storing production database password in keyring...")

	// Check if password already exists
	existingPassword, err := i.keyringManager.Get(DatabaseKeyringService, DatabasePasswordKey)
	if err == nil && existingPassword != "" {
		i.logger.Info("Production database password already exists in keyring")
		return nil
	}

	return i.keyringManager.Set(DatabaseKeyringService, DatabasePasswordKey, password)
}

// generateNodeKeys generates RSA key pair for the node and stores them in keyring
func (i *Initializer) generateNodeKeys() (*NodeInfo, error) {
	i.logger.Info("Generating node RSA key pair...")

	// Check if keys already exist in keyring
	existingPrivateKey, err := i.keyringManager.Get(NodeKeyringService, NodePrivateKeyKey)
	if err == nil && existingPrivateKey != "" {
		existingPublicKey, err := i.keyringManager.Get(NodeKeyringService, NodePublicKeyKey)
		if err == nil && existingPublicKey != "" {
			i.logger.Info("Node keys already exist in keyring, using existing keys")

			// Generate node information with existing keys
			nodeInfo := &NodeInfo{
				PublicKey:  existingPublicKey,
				PrivateKey: existingPrivateKey,
				Port:       8443, // Default port
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
	if err := i.keyringManager.Set(NodeKeyringService, NodePrivateKeyKey, string(privateKeyPEM)); err != nil {
		return nil, fmt.Errorf("failed to store private key: %w", err)
	}

	if err := i.keyringManager.Set(NodeKeyringService, NodePublicKeyKey, string(publicKeyPEM)); err != nil {
		return nil, fmt.Errorf("failed to store public key: %w", err)
	}

	// Generate node information
	nodeInfo := &NodeInfo{
		PublicKey:  string(publicKeyPEM),
		PrivateKey: string(privateKeyPEM),
		Port:       8443, // Default port
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
	secretKey := fmt.Sprintf("%s-%s", JWTSecretKeyPrefix, tenantID)
	err := i.keyringManager.Set(SecurityKeyringService, secretKey, secretString)
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
	if err := i.keyringManager.Set(SecurityKeyringService, privateKeyName, string(privateKeyPEM)); err != nil {
		return fmt.Errorf("failed to store private key: %w", err)
	}

	if err := i.keyringManager.Set(SecurityKeyringService, publicKeyName, string(publicKeyPEM)); err != nil {
		return fmt.Errorf("failed to store public key: %w", err)
	}

	i.logger.Info("Successfully generated and stored tenant RSA keys")
	return nil
}

// GetDatabasePassword retrieves the production database password from keyring
func (i *Initializer) GetDatabasePassword() (string, error) {
	password, err := i.keyringManager.Get(DatabaseKeyringService, DatabasePasswordKey)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve database password from keyring: %w", err)
	}
	return password, nil
}

// GetNodeKeys retrieves the node's RSA key pair from keyring
func (i *Initializer) GetNodeKeys() (publicKey, privateKey string, err error) {
	publicKey, err = i.keyringManager.Get(NodeKeyringService, NodePublicKeyKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to retrieve public key from keyring: %w", err)
	}

	privateKey, err = i.keyringManager.Get(NodeKeyringService, NodePrivateKeyKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to retrieve private key from keyring: %w", err)
	}

	return publicKey, privateKey, nil
}

// GetTenantJWTSecret retrieves JWT secret for a specific tenant
func (i *Initializer) GetTenantJWTSecret(tenantID string) ([]byte, error) {
	secretKey := fmt.Sprintf("%s-%s", JWTSecretKeyPrefix, tenantID)
	secretString, err := i.keyringManager.Get(SecurityKeyringService, secretKey)
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

	// Get the production database name
	prodDatabaseName := getProductionDatabaseName()

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
	err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_user WHERE usename = $1)", ProductionUser).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if user exists: %w", err)
	}

	if exists {
		i.logger.Warnf("User '%s' already exists, updating password", ProductionUser)
		// Update password for existing user
		_, err = conn.Exec(ctx, fmt.Sprintf("ALTER USER %s WITH ENCRYPTED PASSWORD '%s'", ProductionUser, prodPassword))
		if err != nil {
			return fmt.Errorf("failed to update user password: %w", err)
		}
	} else {
		// Create user
		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE USER %s WITH ENCRYPTED PASSWORD '%s'", ProductionUser, prodPassword))
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}
		i.logger.Infof("Created user '%s'", ProductionUser)
	}

	// Grant the production user to admin so admin can set ownership
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT %s TO %s", ProductionUser, adminCreds.User))
	if err != nil {
		return fmt.Errorf("failed to grant user to admin: %w", err)
	}

	// Grant privileges and set ownership
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", prodDatabaseName, ProductionUser))
	if err != nil {
		return fmt.Errorf("failed to grant privileges: %w", err)
	}

	_, err = conn.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", prodDatabaseName, ProductionUser))
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

	// Execute the main schema creation
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
	var existingNodeID string
	err = pool.QueryRow(ctx, `
		SELECT li.identity_id 
		FROM localidentity li
		JOIN nodes n ON n.node_id = li.identity_id
		LIMIT 1
	`).Scan(&existingNodeID)
	if err == nil {
		// Local node already exists, use existing node info
		var existingNodeName string
		err = pool.QueryRow(ctx, `
			SELECT node_name FROM nodes WHERE node_id = $1
		`, existingNodeID).Scan(&existingNodeName)
		if err != nil {
			return fmt.Errorf("failed to get existing node info: %w", err)
		}

		nodeInfo.NodeID = existingNodeID
		nodeInfo.NodeName = existingNodeName
		i.logger.Infof("Local node already exists: '%s' with ID '%s'", existingNodeName, existingNodeID)
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

	// Generate node ID and derive name
	var nodeID string
	err = tx.QueryRow(ctx, "SELECT generate_ulid('node')").Scan(&nodeID)
	if err != nil {
		return fmt.Errorf("failed to generate node ULID: %w", err)
	}

	// Create node name from ULID (take last 8 characters)
	nodeIDSuffix := nodeID[len(nodeID)-8:]
	nodeName := fmt.Sprintf("node-%s", nodeIDSuffix)

	// Insert node into nodes table
	_, err = tx.Exec(ctx, `
		INSERT INTO nodes (node_id, node_name, node_description, ip_address, port, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, nodeID, nodeName, "Local node", nodeInfo.IPAddress, nodeInfo.Port, "STATUS_ACTIVE")
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

	nodeInfo.NodeID = nodeID
	nodeInfo.NodeName = nodeName

	i.logger.Infof("Successfully created local node '%s' with ID '%s'", nodeName, nodeID)
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
