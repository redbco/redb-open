# reDB Shared Packages Documentation

This document provides comprehensive documentation for all shared packages in the `/pkg` directory. These packages provide core functionality that can be imported and used across all microservices in the reDB ecosystem.

## Table of Contents

1. [Service Framework (`/pkg/service`)](#service-framework)
2. [Configuration Management (`/pkg/config`)](#configuration-management)
3. [Database Access (`/pkg/database`)](#database-access)
4. [Structured Logging (`/pkg/logger`)](#structured-logging)
5. [Health Checks (`/pkg/health`)](#health-checks)
6. [Encryption (`/pkg/encryption`)](#encryption)
7. [Data Models (`/pkg/models`)](#data-models)
8. [Secure Storage (`/pkg/keyring`)](#secure-storage)
9. [gRPC Utilities (`/pkg/grpc`)](#grpc-utilities)
10. [System Logging (`/pkg/syslog`)](#system-logging)

---

## Service Framework

**Package:** `github.com/redbco/redb-open/pkg/service`

The service framework provides a standardized base for building microservices with common functionality like lifecycle management, gRPC integration, health checks, and supervisor registration.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/service"

// Create a service implementation
type MyService struct {
    // Your service fields
}

// Implement the Service interface
func (s *MyService) Initialize(ctx context.Context, cfg *config.Config) error {
    // Initialize your service
    return nil
}

func (s *MyService) Start(ctx context.Context) error {
    // Start your service
    return nil
}

func (s *MyService) Stop(ctx context.Context, gracePeriod time.Duration) error {
    // Stop your service
    return nil
}

func (s *MyService) GetCapabilities() *supervisorv1.ServiceCapabilities {
    return &supervisorv1.ServiceCapabilities{
        SupportsHotReload:        true,
        SupportsGracefulShutdown: true,
        Dependencies:             []string{"database"},
        RequiredConfig: map[string]string{
            "database.url": "Database connection URL",
        },
    }
}

// In your main.go
func main() {
    impl := &MyService{}
    
    svc := service.NewBaseService(
        "my-service",        // Service name
        "1.0.0",            // Version
        8080,               // Port
        "supervisor:8081",  // Supervisor address
        impl,               // Your service implementation
    )
    
    if err := svc.Run(); err != nil {
        log.Fatal(err)
    }
}
```

### Advanced Features

#### Logger Integration
```go
type MyService struct {
    logger *logger.Logger
}

// Implement LoggerAware interface
func (s *MyService) SetLogger(logger *logger.Logger) {
    s.logger = logger
}
```

#### gRPC Server Integration
```go
type MyService struct {
    grpcServer *grpc.Server
}

// Implement GRPCServerAware interface
func (s *MyService) SetGRPCServer(server *grpc.Server) {
    s.grpcServer = server
    
    // Register your gRPC services
    myv1.RegisterMyServiceServer(server, s)
}
```

#### Health Checks
```go
func (s *MyService) HealthChecks() map[string]health.CheckFunc {
    return map[string]health.CheckFunc{
        "database":    s.checkDatabase,
        "grpc_server": s.checkGRPCServer,
    }
}

func (s *MyService) checkDatabase() error {
    // Implement database health check
    return nil
}
```

#### Metrics Collection
```go
func (s *MyService) CollectMetrics() map[string]int64 {
    return map[string]int64{
        "requests_processed": s.requestCount,
        "active_connections": s.activeConnections,
    }
}
```

---

## Configuration Management

**Package:** `github.com/redbco/redb-open/pkg/config`

Provides dynamic configuration management with support for hot reloading and hierarchical configuration keys.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/config"

// Create a new configuration instance
cfg := config.New()

// Set configuration values
cfg.Update(map[string]string{
    "database.url":    "postgres://user:pass@localhost/db",
    "server.port":     "8080",
    "feature.enabled": "true",
})

// Get configuration values
dbURL := cfg.Get("database.url")
port := cfg.Get("server.port")

// Get with default value
timeout := cfg.GetWithDefault("server.timeout", "30s")
```

### Service Integration

```go
type MyService struct {
    config *config.Config
}

func (s *MyService) Initialize(ctx context.Context, cfg *config.Config) error {
    s.config = cfg
    
    // Define configuration keys that require service restart when changed
    cfg.SetRestartKeys([]string{
        "database.url",
        "server.port",
        "grpc.address",
    })
    
    return nil
}

// Access configuration in your service methods
func (s *MyService) connectToDatabase() error {
    dbURL := s.config.Get("database.url")
    if dbURL == "" {
        return fmt.Errorf("database.url not configured")
    }
    
    // Use the configuration value
    return s.db.Connect(dbURL)
}
```

### Configuration Patterns

```go
// Service-specific configuration with namespacing
type ServiceConfig struct {
    DatabaseURL string
    GRPCPort    int
    Timeout     time.Duration
}

func loadServiceConfig(cfg *config.Config) (*ServiceConfig, error) {
    return &ServiceConfig{
        DatabaseURL: cfg.Get("services.myservice.database_url"),
        GRPCPort:    parseInt(cfg.Get("services.myservice.grpc_port")),
        Timeout:     parseDuration(cfg.GetWithDefault("services.myservice.timeout", "30s")),
    }, nil
}
```

---

## Database Access

**Package:** `github.com/redbco/redb-open/pkg/database`

Provides PostgreSQL database connection management with connection pooling, health checks, and automatic credential management.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/database"

// Create database configuration from global config
dbConfig := database.FromGlobalConfig(cfg)

// Or create custom configuration
dbConfig := &database.Config{
    Host:           "localhost",
    Port:           5432,
    Username:       "myuser",
    Password:       "mypass",
    DatabaseName:   "mydb",
    SSLMode:        "require",
    MaxConnections: 25,
}

// Initialize database connection
ctx := context.Background()
db, err := database.New(ctx, dbConfig)
if err != nil {
    return fmt.Errorf("failed to connect to database: %w", err)
}
```

### Service Integration

```go
type MyService struct {
    db     *database.PostgreSQL
    logger *logger.Logger
}

func (s *MyService) Initialize(ctx context.Context, cfg *config.Config) error {
    // Initialize database
    dbConfig := database.FromGlobalConfig(cfg)
    db, err := database.New(ctx, dbConfig)
    if err != nil {
        return fmt.Errorf("failed to initialize database: %w", err)
    }
    
    s.db = db
    return nil
}

func (s *MyService) Stop(ctx context.Context, gracePeriod time.Duration) error {
    if s.db != nil {
        s.db.Close()
    }
    return nil
}
```

### Database Operations

```go
// Query single row
func (s *MyService) GetUser(ctx context.Context, userID string) (*User, error) {
    var user User
    
    err := s.db.Pool().QueryRow(ctx,
        "SELECT id, name, email FROM users WHERE id = $1",
        userID,
    ).Scan(&user.ID, &user.Name, &user.Email)
    
    if err != nil {
        if err == pgx.ErrNoRows {
            return nil, fmt.Errorf("user not found")
        }
        return nil, fmt.Errorf("failed to get user: %w", err)
    }
    
    return &user, nil
}

// Query multiple rows
func (s *MyService) ListUsers(ctx context.Context, limit int) ([]*User, error) {
    rows, err := s.db.Pool().Query(ctx,
        "SELECT id, name, email FROM users LIMIT $1",
        limit,
    )
    if err != nil {
        return nil, fmt.Errorf("failed to query users: %w", err)
    }
    defer rows.Close()
    
    var users []*User
    for rows.Next() {
        var user User
        if err := rows.Scan(&user.ID, &user.Name, &user.Email); err != nil {
            return nil, fmt.Errorf("failed to scan user: %w", err)
        }
        users = append(users, &user)
    }
    
    return users, nil
}

// Execute transaction
func (s *MyService) CreateUserWithProfile(ctx context.Context, user *User, profile *Profile) error {
    tx, err := s.db.Pool().Begin(ctx)
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback(ctx)
    
    // Insert user
    err = tx.QueryRow(ctx,
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
        user.Name, user.Email,
    ).Scan(&user.ID)
    if err != nil {
        return fmt.Errorf("failed to create user: %w", err)
    }
    
    // Insert profile
    _, err = tx.Exec(ctx,
        "INSERT INTO profiles (user_id, bio) VALUES ($1, $2)",
        user.ID, profile.Bio,
    )
    if err != nil {
        return fmt.Errorf("failed to create profile: %w", err)
    }
    
    return tx.Commit(ctx)
}
```

### Health Check Integration

```go
func (s *MyService) checkDatabase() error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return s.db.Ping(ctx)
}
```

---

## Structured Logging

**Package:** `github.com/redbco/redb-open/pkg/logger`

Provides structured logging with different levels, colored output, and integration with the supervisor for log streaming.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/logger"

// Create a logger for your service
logger := logger.NewLogger("my-service", "1.0.0")

// Log at different levels
logger.Info("Service started")
logger.Infof("Processing request for user: %s", userID)

logger.Warn("Retrying failed operation")
logger.Warnf("Connection timeout after %v", timeout)

logger.Error("Failed to process request")
logger.Errorf("Database error: %v", err)

logger.Debug("Debug information")
logger.Debugf("Request details: %+v", request)

// Fatal logging (exits the application)
logger.Fatal("Critical error occurred")
logger.Fatalf("Cannot start service: %v", err)
```

### Service Integration

```go
type MyService struct {
    logger *logger.Logger
}

// Implement LoggerAware interface
func (s *MyService) SetLogger(logger *logger.Logger) {
    s.logger = logger
}

// Use logger in service methods
func (s *MyService) ProcessRequest(ctx context.Context, req *Request) (*Response, error) {
    s.logger.Infof("Processing request: %s", req.ID)
    
    // Process request...
    
    if err != nil {
        s.logger.Errorf("Failed to process request %s: %v", req.ID, err)
        return nil, err
    }
    
    s.logger.Infof("Successfully processed request: %s", req.ID)
    return response, nil
}
```

### Advanced Logging Features

```go
// Logging with structured fields
logger.WithFields(map[string]string{
    "user_id":    userID,
    "request_id": requestID,
    "operation":  "create_user",
}).Info("User created successfully")

// Subscribe to log entries (useful for supervisor integration)
logChan := logger.Subscribe()
go func() {
    for entry := range logChan {
        // Process log entry
        fmt.Printf("[%s] %s: %s\n", entry.Time, entry.Level, entry.Message)
    }
}()

// Disable console output (when streaming to supervisor)
logger.DisableConsoleOutput()

// Re-enable console output
logger.EnableConsoleOutput()
```

### Error Handling Patterns

```go
func (s *MyService) performOperation() error {
    s.logger.Debug("Starting operation")
    
    if err := s.step1(); err != nil {
        s.logger.Errorf("Step 1 failed: %v", err)
        return fmt.Errorf("operation failed at step 1: %w", err)
    }
    
    if err := s.step2(); err != nil {
        s.logger.Errorf("Step 2 failed: %v", err)
        return fmt.Errorf("operation failed at step 2: %w", err)
    }
    
    s.logger.Info("Operation completed successfully")
    return nil
}
```

---

## Health Checks

**Package:** `github.com/redbco/redb-open/pkg/health`

Provides a framework for implementing health checks that can be monitored by the supervisor and other monitoring systems.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/health"

// Define health check functions
func checkDatabase() error {
    // Check database connectivity
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    if err := db.Ping(ctx); err != nil {
        return fmt.Errorf("database health check failed: %w", err)
    }
    return nil
}

func checkExternalAPI() error {
    // Check external service connectivity
    client := &http.Client{Timeout: 10 * time.Second}
    resp, err := client.Get("https://api.example.com/health")
    if err != nil {
        return fmt.Errorf("external API health check failed: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("external API returned status: %d", resp.StatusCode)
    }
    return nil
}
```

### Service Integration

```go
type MyService struct {
    db         *database.PostgreSQL
    httpClient *http.Client
    logger     *logger.Logger
}

// Implement health checks in your service
func (s *MyService) HealthChecks() map[string]health.CheckFunc {
    return map[string]health.CheckFunc{
        "database":     s.checkDatabase,
        "grpc_server":  s.checkGRPCServer,
        "external_api": s.checkExternalAPI,
        "memory":       s.checkMemoryUsage,
    }
}

func (s *MyService) checkDatabase() error {
    if s.db == nil {
        return fmt.Errorf("database not initialized")
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return s.db.Ping(ctx)
}

func (s *MyService) checkGRPCServer() error {
    if s.grpcServer == nil {
        return fmt.Errorf("gRPC server not initialized")
    }
    
    // Check if server is serving
    // Implementation depends on your gRPC server setup
    return nil
}

func (s *MyService) checkExternalAPI() error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    req, err := http.NewRequestWithContext(ctx, "GET", "https://api.example.com/health", nil)
    if err != nil {
        return err
    }
    
    resp, err := s.httpClient.Do(req)
    if err != nil {
        return fmt.Errorf("failed to reach external API: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("external API unhealthy, status: %d", resp.StatusCode)
    }
    
    return nil
}

func (s *MyService) checkMemoryUsage() error {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    
    // Check if memory usage is too high (example: 1GB)
    if m.Alloc > 1024*1024*1024 {
        return fmt.Errorf("memory usage too high: %d bytes", m.Alloc)
    }
    
    return nil
}
```

### Health Check Best Practices

```go
// Health checks should be fast and non-blocking
func (s *MyService) checkQuick() error {
    // This should complete in < 1 second
    select {
    case <-time.After(500 * time.Millisecond):
        return fmt.Errorf("health check timeout")
    default:
        // Perform quick check
        return nil
    }
}

// Health checks should not have side effects
func (s *MyService) checkReadOnly() error {
    // Only perform read operations, never write operations
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    
    var count int
    err := s.db.Pool().QueryRow(ctx, "SELECT 1").Scan(&count)
    return err
}

// Health checks should handle errors gracefully
func (s *MyService) checkGraceful() error {
    defer func() {
        if r := recover(); r != nil {
            s.logger.Errorf("Health check panic recovered: %v", r)
        }
    }()
    
    // Perform health check that might panic
    return s.riskyOperation()
}
```

---

## Encryption

**Package:** `github.com/redbco/redb-open/pkg/encryption`

Provides tenant-based encryption for sensitive data using RSA-OAEP encryption with tenant-specific keys.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/encryption"

// Encrypt password for a specific tenant
tenantID := "tenant-123"
password := "sensitive-password"

encryptedPassword, err := encryption.EncryptPassword(tenantID, password)
if err != nil {
    return fmt.Errorf("failed to encrypt password: %w", err)
}

// Decrypt password
decryptedPassword, err := encryption.DecryptPassword(tenantID, encryptedPassword)
if err != nil {
    return fmt.Errorf("failed to decrypt password: %w", err)
}
```

### Advanced Encryption Usage

```go
// Encrypt arbitrary data (not just passwords)
tenantID := "tenant-456"
sensitiveData := []byte("sensitive configuration data")

encryptedData, err := encryption.EncryptData(tenantID, sensitiveData)
if err != nil {
    return fmt.Errorf("failed to encrypt data: %w", err)
}

// Decrypt the data
decryptedData, err := encryption.DecryptData(tenantID, encryptedData)
if err != nil {
    return fmt.Errorf("failed to decrypt data: %w", err)
}
```

### Service Integration

```go
type MyService struct {
    logger *logger.Logger
}

// Store encrypted configuration
func (s *MyService) storeConfig(tenantID, configValue string) error {
    encrypted, err := encryption.EncryptPassword(tenantID, configValue)
    if err != nil {
        s.logger.Errorf("Failed to encrypt config for tenant %s: %v", tenantID, err)
        return err
    }
    
    // Store encrypted value in database
    return s.db.StoreEncryptedConfig(tenantID, encrypted)
}

// Retrieve and decrypt configuration
func (s *MyService) getConfig(tenantID string) (string, error) {
    encrypted, err := s.db.GetEncryptedConfig(tenantID)
    if err != nil {
        return "", err
    }
    
    decrypted, err := encryption.DecryptPassword(tenantID, encrypted)
    if err != nil {
        s.logger.Errorf("Failed to decrypt config for tenant %s: %v", tenantID, err)
        return "", err
    }
    
    return decrypted, nil
}
```

### Key Management

The encryption package automatically handles:
- **Key Generation**: RSA key pairs are generated per tenant as needed
- **Key Storage**: Keys are stored securely in the system keyring
- **Key Retrieval**: Keys are automatically retrieved for encryption/decryption operations
- **Cross-Platform Support**: Works on macOS, Windows, and Linux

### Security Considerations

```go
// Always use tenant-specific encryption
func (s *MyService) secureStorage(tenantID, data string) error {
    // ✅ Good: Tenant-specific encryption
    encrypted, err := encryption.EncryptPassword(tenantID, data)
    
    // ❌ Bad: Don't use a hardcoded or shared tenant ID
    // encrypted, err := encryption.EncryptPassword("shared-tenant", data)
    
    return s.storeData(tenantID, encrypted)
}

// Handle encryption errors appropriately
func (s *MyService) handleEncryption(tenantID, data string) error {
    encrypted, err := encryption.EncryptPassword(tenantID, data)
    if err != nil {
        // Log the error but don't log the sensitive data
        s.logger.Errorf("Encryption failed for tenant %s: %v", tenantID, err)
        return fmt.Errorf("failed to secure data for tenant %s", tenantID)
    }
    
    return s.storeData(tenantID, encrypted)
}
```

---

## Data Models

**Package:** `github.com/redbco/redb-open/pkg/models`

Provides shared data structures used across microservices with consistent JSON and database field mappings.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/models"

// Use predefined models
user := &models.User{
    ID:    "user-123",
    Name:  "John Doe",
    Email: "john@example.com",
}

instance := &models.Instance{
    ID:        "instance-456",
    TenantID:  "tenant-789",
    DatabaseType: "postgresql",
    Status:    "running",
}

database := &models.Database{
    ID:          "db-321",
    InstanceID:  "instance-456",
    Name:        "myapp_production",
    Owner:       "john_doe",
}
```

### JSON Serialization

```go
// Models have JSON tags for API responses
func (s *MyService) getUserAPI(w http.ResponseWriter, r *http.Request) {
    user := &models.User{
        ID:    "user-123",
        Name:  "John Doe",
        Email: "john@example.com",
    }
    
    // Serialize to JSON
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(user)
    // Output: {"id":"user-123","name":"John Doe","email":"john@example.com"}
}
```

### Database Integration

```go
// Models have database tags for scanning
func (s *MyService) getUser(ctx context.Context, userID string) (*models.User, error) {
    var user models.User
    
    err := s.db.Pool().QueryRow(ctx,
        "SELECT id, name, email, created_at FROM users WHERE id = $1",
        userID,
    ).Scan(&user.ID, &user.Name, &user.Email, &user.CreatedAt)
    
    if err != nil {
        return nil, err
    }
    
    return &user, nil
}

// Bulk operations with models
func (s *MyService) listUsers(ctx context.Context) ([]*models.User, error) {
    rows, err := s.db.Pool().Query(ctx,
        "SELECT id, name, email, created_at FROM users",
    )
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var users []*models.User
    for rows.Next() {
        var user models.User
        if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.CreatedAt); err != nil {
            return nil, err
        }
        users = append(users, &user)
    }
    
    return users, nil
}
```

### Model Validation

```go
// Implement validation methods for your models
func validateUser(user *models.User) error {
    if user.ID == "" {
        return fmt.Errorf("user ID is required")
    }
    
    if user.Email == "" {
        return fmt.Errorf("user email is required")
    }
    
    if !isValidEmail(user.Email) {
        return fmt.Errorf("invalid email format")
    }
    
    return nil
}

// Use in service methods
func (s *MyService) createUser(ctx context.Context, user *models.User) error {
    if err := validateUser(user); err != nil {
        return fmt.Errorf("user validation failed: %w", err)
    }
    
    // Insert into database
    _, err := s.db.Pool().Exec(ctx,
        "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
        user.ID, user.Name, user.Email,
    )
    
    return err
}
```

### Model Extensions

```go
// Extend models with additional methods
type ExtendedUser struct {
    models.User
    Permissions []string `json:"permissions"`
}

func (u *ExtendedUser) HasPermission(permission string) bool {
    for _, p := range u.Permissions {
        if p == permission {
            return true
        }
    }
    return false
}

// Use extended models in services
func (s *MyService) getUserWithPermissions(ctx context.Context, userID string) (*ExtendedUser, error) {
    // Get base user data
    baseUser, err := s.getUser(ctx, userID)
    if err != nil {
        return nil, err
    }
    
    // Get permissions
    permissions, err := s.getUserPermissions(ctx, userID)
    if err != nil {
        return nil, err
    }
    
    return &ExtendedUser{
        User:        *baseUser,
        Permissions: permissions,
    }, nil
}
```

---

## Secure Storage

**Package:** `github.com/redbco/redb-open/pkg/keyring`

Provides secure storage for credentials and encryption keys using the system keyring with file-based fallback.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/keyring"

// Store a credential
err := keyring.Set("my-service", "database-password", "secret-password")
if err != nil {
    return fmt.Errorf("failed to store credential: %w", err)
}

// Retrieve a credential
password, err := keyring.Get("my-service", "database-password")
if err != nil {
    return fmt.Errorf("failed to retrieve credential: %w", err)
}

// Delete a credential
err = keyring.Delete("my-service", "database-password")
if err != nil {
    return fmt.Errorf("failed to delete credential: %w", err)
}
```

### Service Integration

```go
type MyService struct {
    config *config.Config
    logger *logger.Logger
}

// Store database credentials securely
func (s *MyService) storeDBCredentials(username, password string) error {
    serviceName := "my-service"
    
    // Store username
    if err := keyring.Set(serviceName, "db-username", username); err != nil {
        s.logger.Errorf("Failed to store database username: %v", err)
        return err
    }
    
    // Store password
    if err := keyring.Set(serviceName, "db-password", password); err != nil {
        s.logger.Errorf("Failed to store database password: %v", err)
        return err
    }
    
    s.logger.Info("Database credentials stored successfully")
    return nil
}

// Retrieve database credentials
func (s *MyService) getDBCredentials() (string, string, error) {
    serviceName := "my-service"
    
    username, err := keyring.Get(serviceName, "db-username")
    if err != nil {
        return "", "", fmt.Errorf("failed to get database username: %w", err)
    }
    
    password, err := keyring.Get(serviceName, "db-password")
    if err != nil {
        return "", "", fmt.Errorf("failed to get database password: %w", err)
    }
    
    return username, password, nil
}
```

### Configuration Integration

```go
// Use keyring as fallback for configuration
func (s *MyService) getDatabaseURL() (string, error) {
    // First try configuration
    if dbURL := s.config.Get("database.url"); dbURL != "" {
        return dbURL, nil
    }
    
    // Fallback to keyring
    username, password, err := s.getDBCredentials()
    if err != nil {
        return "", err
    }
    
    host := s.config.GetWithDefault("database.host", "localhost")
    port := s.config.GetWithDefault("database.port", "5432")
    dbname := s.config.GetWithDefault("database.name", "myapp")
    
    dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
        username, password, host, port, dbname)
    
    return dbURL, nil
}
```

### Encryption Key Management

```go
// Store encryption keys
func (s *MyService) storeEncryptionKey(tenantID string, key []byte) error {
    keyName := fmt.Sprintf("encryption-key-%s", tenantID)
    keyValue := base64.StdEncoding.EncodeToString(key)
    
    return keyring.Set("encryption-service", keyName, keyValue)
}

// Retrieve encryption keys
func (s *MyService) getEncryptionKey(tenantID string) ([]byte, error) {
    keyName := fmt.Sprintf("encryption-key-%s", tenantID)
    
    keyValue, err := keyring.Get("encryption-service", keyName)
    if err != nil {
        return nil, err
    }
    
    return base64.StdEncoding.DecodeString(keyValue)
}
```

### Platform Considerations

The keyring package provides:
- **macOS**: Uses the macOS Keychain
- **Windows**: Uses the Windows Credential Manager
- **Linux**: Uses the Secret Service API (GNOME/KDE)
- **Fallback**: File-based storage when system keyring is unavailable

```go
// Check if system keyring is available
func (s *MyService) initializeSecureStorage() error {
    // Test keyring availability
    testKey := fmt.Sprintf("test-key-%d", time.Now().Unix())
    testValue := "test-value"
    
    if err := keyring.Set("test-service", testKey, testValue); err != nil {
        s.logger.Warnf("System keyring unavailable, using file fallback: %v", err)
        // Service will still work with file-based storage
    } else {
        s.logger.Info("System keyring available")
        // Clean up test key
        keyring.Delete("test-service", testKey)
    }
    
    return nil
}
```

---

## gRPC Utilities

**Package:** `github.com/redbco/redb-open/pkg/grpc`

Provides utilities for creating and configuring gRPC servers and clients with standard settings for the reDB ecosystem.

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/grpc"

// Create a gRPC server with default configuration
server := grpc.NewServer()

// Create a gRPC client connection
conn, err := grpc.NewClientConnection("localhost:8080")
if err != nil {
    return fmt.Errorf("failed to connect: %w", err)
}
defer conn.Close()
```

### Server Configuration

```go
import (
    "github.com/redbco/redb-open/pkg/grpc"
    "google.golang.org/grpc"
)

// Create server with custom options
server := grpc.NewServerWithOptions(
    grpc.MaxRecvMsgSize(4 * 1024 * 1024), // 4MB
    grpc.MaxSendMsgSize(4 * 1024 * 1024), // 4MB
)

// Register your services
myv1.RegisterMyServiceServer(server, &MyServiceImpl{})

// Start serving
lis, err := net.Listen("tcp", ":8080")
if err != nil {
    log.Fatal(err)
}

if err := server.Serve(lis); err != nil {
    log.Fatal(err)
}
```

### Client Configuration

```go
// Create client with timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

conn, err := grpc.NewClientConnectionWithContext(ctx, "service:8080")
if err != nil {
    return fmt.Errorf("failed to connect: %w", err)
}
defer conn.Close()

// Create service client
client := myv1.NewMyServiceClient(conn)

// Make request
response, err := client.MyMethod(ctx, &myv1.MyRequest{
    Field: "value",
})
if err != nil {
    return fmt.Errorf("request failed: %w", err)
}
```

### Service Integration

```go
type MyService struct {
    grpcServer *grpc.Server
    clients    map[string]*grpc.ClientConn
    logger     *logger.Logger
}

// Implement GRPCServerAware interface
func (s *MyService) SetGRPCServer(server *grpc.Server) {
    s.grpcServer = server
    
    // Register your gRPC services
    myv1.RegisterMyServiceServer(server, s)
}

// Initialize client connections
func (s *MyService) initializeClients() error {
    s.clients = make(map[string]*grpc.ClientConn)
    
    // Connect to other services
    coreConn, err := grpc.NewClientConnection("core-service:8080")
    if err != nil {
        return fmt.Errorf("failed to connect to core service: %w", err)
    }
    s.clients["core"] = coreConn
    
    authConn, err := grpc.NewClientConnection("auth-service:8081")
    if err != nil {
        return fmt.Errorf("failed to connect to auth service: %w", err)
    }
    s.clients["auth"] = authConn
    
    return nil
}

// Clean up connections
func (s *MyService) Stop(ctx context.Context, gracePeriod time.Duration) error {
    // Close client connections
    for name, conn := range s.clients {
        if err := conn.Close(); err != nil {
            s.logger.Errorf("Failed to close connection to %s: %v", name, err)
        }
    }
    
    return nil
}
```

### Service Discovery Integration

```go
// Connect to services using configuration
func (s *MyService) connectToServices() error {
    services := map[string]string{
        "core": s.config.Get("services.core.grpc_address"),
        "auth": s.config.Get("services.auth.grpc_address"),
        "data": s.config.Get("services.data.grpc_address"),
    }
    
    s.clients = make(map[string]*grpc.ClientConn)
    
    for name, address := range services {
        if address == "" {
            s.logger.Warnf("No address configured for service: %s", name)
            continue
        }
        
        conn, err := grpc.NewClientConnection(address)
        if err != nil {
            s.logger.Errorf("Failed to connect to %s at %s: %v", name, address, err)
            continue
        }
        
        s.clients[name] = conn
        s.logger.Infof("Connected to %s service at %s", name, address)
    }
    
    return nil
}

// Get client for a service
func (s *MyService) getCoreClient() (corev1.CoreServiceClient, error) {
    conn, exists := s.clients["core"]
    if !exists {
        return nil, fmt.Errorf("core service not connected")
    }
    
    return corev1.NewCoreServiceClient(conn), nil
}
```

---

## System Logging

**Package:** `github.com/redbco/redb-open/pkg/syslog`

Provides system-level logging integration for sending logs to system log facilities (syslog on Unix, Event Log on Windows).

### Import and Basic Usage

```go
import "github.com/redbco/redb-open/pkg/syslog"

// Create syslog writer
writer, err := syslog.NewWriter("my-service")
if err != nil {
    return fmt.Errorf("failed to create syslog writer: %w", err)
}
defer writer.Close()

// Log messages
writer.Info("Service started")
writer.Error("An error occurred")
writer.Warning("Warning message")
```

### Service Integration

```go
type MyService struct {
    logger    *logger.Logger
    syslogger *syslog.Writer
}

func (s *MyService) Initialize(ctx context.Context, cfg *config.Config) error {
    // Initialize syslog if enabled
    if cfg.Get("logging.syslog.enabled") == "true" {
        serviceName := cfg.GetWithDefault("service.name", "my-service")
        
        syslogger, err := syslog.NewWriter(serviceName)
        if err != nil {
            s.logger.Warnf("Failed to initialize syslog: %v", err)
        } else {
            s.syslogger = syslogger
            s.logger.Info("Syslog integration enabled")
        }
    }
    
    return nil
}

// Bridge regular logging to syslog
func (s *MyService) logToSyslog(level, message string) {
    if s.syslogger == nil {
        return
    }
    
    switch level {
    case "INFO":
        s.syslogger.Info(message)
    case "WARN":
        s.syslogger.Warning(message)
    case "ERROR":
        s.syslogger.Error(message)
    default:
        s.syslogger.Info(message)
    }
}

func (s *MyService) Stop(ctx context.Context, gracePeriod time.Duration) error {
    if s.syslogger != nil {
        s.syslogger.Close()
    }
    return nil
}
```

### Configuration Options

```go
// Configure syslog based on environment
func (s *MyService) configureSyslog(cfg *config.Config) error {
    if !cfg.GetBool("logging.syslog.enabled") {
        return nil
    }
    
    facility := cfg.GetWithDefault("logging.syslog.facility", "daemon")
    priority := cfg.GetWithDefault("logging.syslog.priority", "info")
    
    config := &syslog.Config{
        ServiceName: cfg.GetWithDefault("service.name", "redb-service"),
        Facility:    facility,
        Priority:    priority,
    }
    
    writer, err := syslog.NewWriterWithConfig(config)
    if err != nil {
        return fmt.Errorf("failed to configure syslog: %w", err)
    }
    
    s.syslogger = writer
    return nil
}
```

---

## Common Integration Patterns

### Complete Service Example

Here's a complete example showing how to integrate multiple shared packages in a microservice:

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/redbco/redb-open/pkg/config"
    "github.com/redbco/redb-open/pkg/database"
    "github.com/redbco/redb-open/pkg/health"
    "github.com/redbco/redb-open/pkg/logger"
    "github.com/redbco/redb-open/pkg/models"
    "github.com/redbco/redb-open/pkg/service"
    myv1 "github.com/redbco/redb-open/api/proto/my/v1"
    supervisorv1 "github.com/redbco/redb-open/api/proto/supervisor/v1"
    "google.golang.org/grpc"
)

type MyService struct {
    config *config.Config
    logger *logger.Logger
    db     *database.PostgreSQL
    grpcServer *grpc.Server
}

func NewMyService() *MyService {
    return &MyService{}
}

// Implement service.Service interface
func (s *MyService) Initialize(ctx context.Context, cfg *config.Config) error {
    s.config = cfg
    
    // Set restart-required configuration keys
    cfg.SetRestartKeys([]string{
        "database.url",
        "server.port",
    })
    
    // Initialize database
    dbConfig := database.FromGlobalConfig(cfg)
    db, err := database.New(ctx, dbConfig)
    if err != nil {
        return fmt.Errorf("failed to initialize database: %w", err)
    }
    s.db = db
    
    return nil
}

func (s *MyService) Start(ctx context.Context) error {
    s.logger.Info("Starting my service")
    
    // Start any background processes
    go s.backgroundTask(ctx)
    
    return nil
}

func (s *MyService) Stop(ctx context.Context, gracePeriod time.Duration) error {
    s.logger.Info("Stopping my service")
    
    if s.db != nil {
        s.db.Close()
    }
    
    return nil
}

func (s *MyService) GetCapabilities() *supervisorv1.ServiceCapabilities {
    return &supervisorv1.ServiceCapabilities{
        SupportsHotReload:        true,
        SupportsGracefulShutdown: true,
        Dependencies:             []string{"database"},
        RequiredConfig: map[string]string{
            "database.url": "Database connection URL",
        },
    }
}

// Implement service.LoggerAware interface
func (s *MyService) SetLogger(logger *logger.Logger) {
    s.logger = logger
}

// Implement service.GRPCServerAware interface
func (s *MyService) SetGRPCServer(server *grpc.Server) {
    s.grpcServer = server
    myv1.RegisterMyServiceServer(server, s)
}

// Implement health checks
func (s *MyService) HealthChecks() map[string]health.CheckFunc {
    return map[string]health.CheckFunc{
        "database": s.checkDatabase,
        "grpc":     s.checkGRPC,
    }
}

func (s *MyService) checkDatabase() error {
    if s.db == nil {
        return fmt.Errorf("database not initialized")
    }
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return s.db.Ping(ctx)
}

func (s *MyService) checkGRPC() error {
    if s.grpcServer == nil {
        return fmt.Errorf("gRPC server not initialized")
    }
    return nil
}

// Implement metrics collection
func (s *MyService) CollectMetrics() map[string]int64 {
    return map[string]int64{
        "requests_processed": s.getRequestCount(),
        "active_connections": s.getActiveConnections(),
    }
}

// Background task example
func (s *MyService) backgroundTask(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            s.performMaintenance()
        case <-ctx.Done():
            s.logger.Info("Background task stopping")
            return
        }
    }
}

func (s *MyService) performMaintenance() {
    s.logger.Debug("Performing maintenance tasks")
    // Implement maintenance logic
}

// gRPC service implementation
func (s *MyService) CreateUser(ctx context.Context, req *myv1.CreateUserRequest) (*myv1.CreateUserResponse, error) {
    s.logger.Infof("Creating user: %s", req.Name)
    
    user := &models.User{
        ID:    generateID(),
        Name:  req.Name,
        Email: req.Email,
    }
    
    // Insert into database
    err := s.db.Pool().QueryRow(ctx,
        "INSERT INTO users (id, name, email) VALUES ($1, $2, $3) RETURNING created_at",
        user.ID, user.Name, user.Email,
    ).Scan(&user.CreatedAt)
    
    if err != nil {
        s.logger.Errorf("Failed to create user: %v", err)
        return nil, fmt.Errorf("failed to create user: %w", err)
    }
    
    s.logger.Infof("User created successfully: %s", user.ID)
    
    return &myv1.CreateUserResponse{
        User: &myv1.User{
            Id:    user.ID,
            Name:  user.Name,
            Email: user.Email,
        },
    }, nil
}

// Helper functions
func (s *MyService) getRequestCount() int64 {
    // Implement request counting
    return 0
}

func (s *MyService) getActiveConnections() int64 {
    // Implement connection counting
    return 0
}

func generateID() string {
    // Implement ID generation
    return fmt.Sprintf("user_%d", time.Now().UnixNano())
}

// Main function
func main() {
    impl := NewMyService()
    
    svc := service.NewBaseService(
        "my-service",
        "1.0.0",
        8080,
        "supervisor:8081",
        impl,
    )
    
    if err := svc.Run(); err != nil {
        panic(err)
    }
}
```

This example demonstrates how to:
1. ✅ **Initialize all shared packages** in the correct order
2. ✅ **Implement required interfaces** for the service framework
3. ✅ **Handle configuration and hot-reloading** properly
4. ✅ **Integrate database operations** with proper error handling
5. ✅ **Implement health checks** for all components
6. ✅ **Use structured logging** throughout the service
7. ✅ **Register gRPC services** correctly
8. ✅ **Collect and expose metrics** for monitoring
9. ✅ **Handle graceful shutdown** and resource cleanup

Each shared package provides essential functionality that, when combined, creates a robust, production-ready microservice that integrates seamlessly with the codebase.