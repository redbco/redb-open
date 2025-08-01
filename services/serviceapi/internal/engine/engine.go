package engine

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/keyring"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Keyring service names for different components
	SecurityKeyringService = "redb-security"
	JWTSecretKeyPrefix     = "tenant-jwt-secret"
)

type Engine struct {
	config          *config.Config
	server          *http.Server
	securityClient  securityv1.SecurityServiceClient
	tenantClient    corev1.TenantServiceClient
	workspaceClient corev1.WorkspaceServiceClient
	meshClient      corev1.MeshServiceClient
	userClient      corev1.UserServiceClient
	logger          *logger.Logger
	state           struct {
		sync.Mutex
		isRunning         bool
		ongoingOperations int32
	}
	metrics struct {
		requestsProcessed int64
		errors            int64
	}
}

func NewEngine(cfg *config.Config) *Engine {
	return &Engine{
		config: cfg,
	}
}

// SetLogger sets the logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger
}

func (e *Engine) Start(ctx context.Context) error {
	e.state.Lock()
	if e.state.isRunning {
		e.state.Unlock()
		return fmt.Errorf("engine is already running")
	}
	e.state.isRunning = true
	e.state.Unlock()

	if e.logger != nil {
		e.logger.Infof("Starting ServiceAPI engine...")
	}

	// Connect to core service
	coreAddr := e.config.Get("services.core.grpc_address")
	if coreAddr == "" {
		coreAddr = "localhost:50062"
	}

	if e.logger != nil {
		e.logger.Infof("Connecting to core service at: %s", coreAddr)
	}

	coreConn, err := grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to core service at %s: %v", coreAddr, err)
		}
		return fmt.Errorf("failed to connect to core service: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to core service at: %s", coreAddr)
	}

	e.tenantClient = corev1.NewTenantServiceClient(coreConn)
	e.meshClient = corev1.NewMeshServiceClient(coreConn)
	e.workspaceClient = corev1.NewWorkspaceServiceClient(coreConn)
	e.userClient = corev1.NewUserServiceClient(coreConn)

	// Connect to security service
	securityAddr := e.config.Get("services.security.grpc_address")
	if securityAddr == "" {
		securityAddr = "localhost:50051"
	}

	if e.logger != nil {
		e.logger.Infof("Connecting to security service at: %s", securityAddr)
	}

	securityConn, err := grpc.Dial(securityAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to security service at %s: %v", securityAddr, err)
		}
		return fmt.Errorf("failed to connect to security service: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to security service at: %s", securityAddr)
	}

	e.securityClient = securityv1.NewSecurityServiceClient(securityConn)

	if e.logger != nil {
		e.logger.Infof("Security client initialized successfully")
	}

	// Initialize HTTP server
	portStr := e.config.Get("services.serviceapi.http_port")
	if portStr == "" {
		portStr = "8081"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Invalid HTTP port configuration: %v", err)
		}
		return fmt.Errorf("invalid port configuration: %v", err)
	}

	e.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: NewServer(e),
	}

	if e.logger != nil {
		e.logger.Infof("Starting HTTP server on port: %d", port)
	}

	// Start HTTP server
	go func() {
		if err := e.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if e.logger != nil {
				e.logger.Errorf("HTTP server error: %v", err)
			}
			atomic.AddInt64(&e.metrics.errors, 1)
		}
	}()

	if e.logger != nil {
		e.logger.Infof("ServiceAPI engine started successfully")
	}

	return nil
}

func (e *Engine) Stop(ctx context.Context) error {
	e.state.Lock()
	if !e.state.isRunning {
		e.state.Unlock()
		return nil
	}
	e.state.Unlock()

	if e.server != nil {
		return e.server.Shutdown(ctx)
	}

	return nil
}

func (e *Engine) GetMetrics() map[string]int64 {
	return map[string]int64{
		"requests_processed": atomic.LoadInt64(&e.metrics.requestsProcessed),
		"errors":             atomic.LoadInt64(&e.metrics.errors),
	}
}

func (e *Engine) CheckGRPCServer() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	if e.server == nil {
		return fmt.Errorf("HTTP server not initialized")
	}

	return nil
}

func (e *Engine) CheckHealth() error {
	e.state.Lock()
	defer e.state.Unlock()

	if !e.state.isRunning {
		return fmt.Errorf("service not initialized")
	}

	return nil
}

func (e *Engine) TrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, 1)
}

func (e *Engine) UntrackOperation() {
	atomic.AddInt32(&e.state.ongoingOperations, -1)
}

// Query forwards the request to the core service
func (e *Engine) Query(ctx context.Context, req interface{}) (interface{}, error) {
	// TODO: Implement query handling based on the request type
	return nil, fmt.Errorf("query not implemented")
}

// GetSecurityClient returns the security service client
func (e *Engine) GetSecurityClient() securityv1.SecurityServiceClient {
	return e.securityClient
}

// GetTenantClient returns the tenant service client
func (e *Engine) GetTenantClient() corev1.TenantServiceClient {
	return e.tenantClient
}

// PerformInitialSetup handles the initial setup of tenant, user, and workspace
// This endpoint is only accessible when no tenants exist in the system
func (e *Engine) PerformInitialSetup(ctx context.Context, req interface{}) (interface{}, error) {
	// Type assertion to get the request data
	setupReq, ok := req.(struct {
		TenantName        string `json:"tenant_name"`
		TenantURL         string `json:"tenant_url"`
		TenantDescription string `json:"tenant_description"`
		UserEmail         string `json:"user_email"`
		UserPassword      string `json:"user_password"`
		WorkspaceName     string `json:"workspace_name"`
	})
	if !ok {
		return nil, fmt.Errorf("invalid request type for initial setup")
	}

	// Check if any tenants already exist
	tenantsReq := &corev1.ListTenantsRequest{}
	tenantsResp, err := e.tenantClient.ListTenants(ctx, tenantsReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to check existing tenants: %v", err)
		}
		return nil, fmt.Errorf("failed to check existing tenants: %v", err)
	}

	// If tenants already exist, reject the setup request
	if len(tenantsResp.Tenants) > 0 {
		if e.logger != nil {
			e.logger.Warnf("Initial setup rejected: tenants already exist")
		}
		return nil, fmt.Errorf("initial setup not allowed: tenants already exist in the system")
	}

	// Create the tenant with user
	tenantReq := &corev1.AddTenantRequest{
		TenantName:        setupReq.TenantName,
		TenantUrl:         setupReq.TenantURL,
		TenantDescription: setupReq.TenantDescription,
		UserEmail:         setupReq.UserEmail,
		UserPassword:      setupReq.UserPassword,
	}

	tenantResp, err := e.tenantClient.AddTenant(ctx, tenantReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to create tenant: %v", err)
		}
		return nil, fmt.Errorf("failed to create tenant: %v", err)
	}

	// Generate JWT secret for the tenant
	if err := e.generateTenantJWTSecret(tenantResp.Tenant.TenantId); err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to generate JWT secret for tenant: %v", err)
		}
		return nil, fmt.Errorf("failed to generate JWT secret for tenant: %v", err)
	}

	// Get the user id from the tenant by listing users for the tenant
	usersReq := &corev1.ListUsersRequest{
		TenantId: tenantResp.Tenant.TenantId,
	}
	usersResp, err := e.userClient.ListUsers(ctx, usersReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to list users: %v", err)
		}
		return nil, fmt.Errorf("failed to list users: %v", err)
	}

	// Get the user id from the users response
	userId := usersResp.Users[0].UserId

	workspaceDescription := "Default workspace"

	// Create workspace using the workspace service
	workspaceReq := &corev1.AddWorkspaceRequest{
		TenantId:             tenantResp.Tenant.TenantId,
		WorkspaceName:        setupReq.WorkspaceName,
		WorkspaceDescription: &workspaceDescription,
		OwnerId:              userId,
	}
	workspaceResp, err := e.workspaceClient.AddWorkspace(ctx, workspaceReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to create workspace: %v", err)
		}
		return nil, fmt.Errorf("failed to create workspace: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Initial setup completed successfully for tenant: %s", setupReq.TenantName)
	}

	return map[string]interface{}{
		"success": true,
		"message": "Initial setup completed successfully",
		"tenant": map[string]interface{}{
			"tenant_id":          tenantResp.Tenant.TenantId,
			"tenant_name":        tenantResp.Tenant.TenantName,
			"tenant_description": tenantResp.Tenant.TenantDescription,
			"tenant_url":         tenantResp.Tenant.TenantUrl,
		},
		"workspace": map[string]interface{}{
			"workspace_id":          workspaceResp.Workspace.WorkspaceId,
			"workspace_name":        workspaceResp.Workspace.WorkspaceName,
			"workspace_description": workspaceResp.Workspace.WorkspaceDescription,
		},
	}, nil
}

func (e *Engine) generateTenantJWTSecret(tenantId string) error {
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	km := keyring.NewKeyringManager(keyringPath, masterPassword)

	// Generate random secret (64 bytes)
	secretBytes := make([]byte, 64)
	if _, err := rand.Read(secretBytes); err != nil {
		return fmt.Errorf("failed to generate random secret: %w", err)
	}

	// Encode secret as base64 for storage
	secretString := base64.StdEncoding.EncodeToString(secretBytes)

	// Store in keyring using the same pattern as the security service
	secretKey := fmt.Sprintf("%s-%s", JWTSecretKeyPrefix, tenantId)
	err := km.Set(SecurityKeyringService, secretKey, secretString)
	if err != nil {
		return fmt.Errorf("failed to store tenant JWT secret: %w", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully generated and stored JWT secret for tenant: %s", tenantId)
	}

	return nil
}
