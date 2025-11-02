package engine

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/pkg/keyring"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// Keyring service names for different components
	SecurityKeyringService = "redb-security"
	JWTSecretKeyPrefix     = "tenant-jwt-secret"
)

type Engine struct {
	config               *config.Config
	server               *http.Server
	securityClient       securityv1.SecurityServiceClient
	meshClient           corev1.MeshServiceClient
	workspaceClient      corev1.WorkspaceServiceClient
	satelliteClient      corev1.SatelliteServiceClient
	anchorClient         corev1.AnchorServiceClient
	regionClient         corev1.RegionServiceClient
	environmentClient    corev1.EnvironmentServiceClient
	instanceClient       corev1.InstanceServiceClient
	databaseClient       corev1.DatabaseServiceClient
	repoClient           corev1.RepoServiceClient
	branchClient         corev1.BranchServiceClient
	commitClient         corev1.CommitServiceClient
	mappingClient        corev1.MappingServiceClient
	relationshipClient   corev1.RelationshipServiceClient
	transformationClient corev1.TransformationServiceClient
	policyClient         corev1.PolicyServiceClient
	mcpClient            corev1.MCPServiceClient
	tenantClient         corev1.TenantServiceClient
	userClient           corev1.UserServiceClient
	tokenClient          corev1.TokenServiceClient
	groupClient          corev1.GroupServiceClient
	roleClient           corev1.RoleServiceClient
	permissionClient     corev1.PermissionServiceClient
	assignmentClient     corev1.AssignmentServiceClient
	authorizationClient  corev1.AuthorizationServiceClient
	templateClient       corev1.TemplateServiceClient
	auditClient          corev1.AuditServiceClient
	importExportClient   corev1.ImportExportServiceClient
	logger               *logger.Logger
	state                struct {
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
		e.logger.Infof("Starting ClientAPI engine...")
	}

	// Connect to core service using dynamic address resolution
	coreAddr := grpcconfig.GetServiceAddress(e.config, "core")

	if e.logger != nil {
		e.logger.Infof("Connecting to core service at: %s", coreAddr)
	}

	// Create connection with proper keepalive and connection pool settings
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	coreDialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be established
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second, // Send keepalive pings every 10 seconds
			Timeout:             3 * time.Second,  // Wait 3 seconds for ping ack before considering connection dead
			PermitWithoutStream: true,             // Send keepalive pings even when there are no active streams
		}),
		grpc.WithDefaultCallOptions(
			grpc.WaitForReady(true), // Wait for connection to be ready before sending RPCs
		),
	}

	coreConn, err := grpc.DialContext(dialCtx, coreAddr, coreDialOpts...)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to core service at %s: %v", coreAddr, err)
		}
		return fmt.Errorf("failed to connect to core service: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to core service at: %s", coreAddr)
	}

	// Initialize all clients for core v2
	e.meshClient = corev1.NewMeshServiceClient(coreConn)
	e.workspaceClient = corev1.NewWorkspaceServiceClient(coreConn)
	e.satelliteClient = corev1.NewSatelliteServiceClient(coreConn)
	e.anchorClient = corev1.NewAnchorServiceClient(coreConn)
	e.regionClient = corev1.NewRegionServiceClient(coreConn)
	e.environmentClient = corev1.NewEnvironmentServiceClient(coreConn)
	e.instanceClient = corev1.NewInstanceServiceClient(coreConn)
	e.databaseClient = corev1.NewDatabaseServiceClient(coreConn)
	e.repoClient = corev1.NewRepoServiceClient(coreConn)
	e.branchClient = corev1.NewBranchServiceClient(coreConn)
	e.commitClient = corev1.NewCommitServiceClient(coreConn)
	e.mappingClient = corev1.NewMappingServiceClient(coreConn)
	e.relationshipClient = corev1.NewRelationshipServiceClient(coreConn)
	e.transformationClient = corev1.NewTransformationServiceClient(coreConn)
	e.policyClient = corev1.NewPolicyServiceClient(coreConn)
	e.mcpClient = corev1.NewMCPServiceClient(coreConn)
	e.tenantClient = corev1.NewTenantServiceClient(coreConn)
	e.userClient = corev1.NewUserServiceClient(coreConn)
	e.tokenClient = corev1.NewTokenServiceClient(coreConn)
	e.groupClient = corev1.NewGroupServiceClient(coreConn)
	e.roleClient = corev1.NewRoleServiceClient(coreConn)
	e.permissionClient = corev1.NewPermissionServiceClient(coreConn)
	e.assignmentClient = corev1.NewAssignmentServiceClient(coreConn)
	e.authorizationClient = corev1.NewAuthorizationServiceClient(coreConn)
	e.templateClient = corev1.NewTemplateServiceClient(coreConn)
	e.auditClient = corev1.NewAuditServiceClient(coreConn)
	e.importExportClient = corev1.NewImportExportServiceClient(coreConn)

	// Connect to security service using dynamic address resolution
	securityAddr := grpcconfig.GetServiceAddress(e.config, "security")

	if e.logger != nil {
		e.logger.Infof("Connecting to security service at: %s", securityAddr)
	}

	// Create connection with proper keepalive and connection pool settings
	// This is critical for handling concurrent authentication requests
	securityDialCtx, securityCancel := context.WithTimeout(ctx, 30*time.Second)
	defer securityCancel()

	securityDialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be established
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second, // Send keepalive pings every 10 seconds
			Timeout:             3 * time.Second,  // Wait 3 seconds for ping ack before considering connection dead
			PermitWithoutStream: true,             // Send keepalive pings even when there are no active streams
		}),
		grpc.WithDefaultCallOptions(
			grpc.WaitForReady(true), // Wait for connection to be ready before sending RPCs
		),
	}

	securityConn, err := grpc.DialContext(securityDialCtx, securityAddr, securityDialOpts...)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to security service at %s: %v", securityAddr, err)
		}
		return fmt.Errorf("failed to connect to security service: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to security service at: %s", securityAddr)
	}

	// Initialize the security client
	e.securityClient = securityv1.NewSecurityServiceClient(securityConn)

	if e.logger != nil {
		e.logger.Infof("Security client initialized successfully")
	}

	// Initialize HTTP server
	// Check for REST_API_PORT from environment first (set by supervisor with port offset)
	portStr := os.Getenv("REST_API_PORT")
	if portStr == "" {
		// Fallback to EXTERNAL_PORT for backward compatibility
		portStr = os.Getenv("EXTERNAL_PORT")
	}
	if portStr == "" {
		// Fallback to legacy config key
		portStr = e.config.Get("services.clientapi.http_port")
	}
	if portStr == "" {
		portStr = "8080" // Default HTTP port
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Invalid REST API port configuration: %v", err)
		}
		return fmt.Errorf("invalid REST API port configuration: %v", err)
	}

	e.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: NewServer(e),
	}

	if e.logger != nil {
		e.logger.Infof("Starting REST API server on port: %d", port)
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
		e.logger.Infof("ClientAPI engine started successfully")
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

// PerformUserSetup creates a user and workspace for an existing tenant (single-tenant mode)
func (e *Engine) PerformUserSetup(ctx context.Context, tenantURL string, req interface{}) (interface{}, error) {
	// Type assertion to get the request data
	setupReq, ok := req.(struct {
		UserEmail     string `json:"user_email"`
		UserPassword  string `json:"user_password"`
		WorkspaceName string `json:"workspace_name"`
	})
	if !ok {
		return nil, fmt.Errorf("invalid request type for user setup")
	}

	// Get tenant by URL
	tenantReq := &corev1.ListTenantsRequest{}
	tenantsResp, err := e.tenantClient.ListTenants(ctx, tenantReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to list tenants: %v", err)
		}
		return nil, fmt.Errorf("failed to list tenants: %v", err)
	}

	// Find the tenant by URL
	var targetTenant *corev1.Tenant
	for _, tenant := range tenantsResp.Tenants {
		if tenant.TenantUrl == tenantURL {
			targetTenant = tenant
			break
		}
	}

	if targetTenant == nil {
		if e.logger != nil {
			e.logger.Warnf("Tenant not found with URL: %s", tenantURL)
		}
		return nil, fmt.Errorf("tenant not found with URL: %s", tenantURL)
	}

	// Check if any users already exist in the tenant (this should only create the first user)
	usersReq := &corev1.ListUsersRequest{
		TenantId: targetTenant.TenantId,
	}
	usersResp, err := e.userClient.ListUsers(ctx, usersReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to check existing users: %v", err)
		}
		return nil, fmt.Errorf("failed to check existing users: %v", err)
	}

	// If users already exist, reject the setup request
	if len(usersResp.Users) > 0 {
		if e.logger != nil {
			e.logger.Warnf("User setup rejected: users already exist in tenant %s", tenantURL)
		}
		return nil, fmt.Errorf("user setup not allowed: users already exist in the tenant")
	}

	// Generate JWT secret for the tenant if it doesn't exist
	if err := e.generateTenantJWTSecret(targetTenant.TenantId); err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to generate JWT secret for tenant: %v", err)
		}
		return nil, fmt.Errorf("failed to generate JWT secret for tenant: %v", err)
	}

	// Create the user
	userReq := &corev1.AddUserRequest{
		TenantId:     targetTenant.TenantId,
		UserName:     setupReq.UserEmail, // Use email as name for initial user
		UserEmail:    setupReq.UserEmail,
		UserPassword: setupReq.UserPassword,
	}

	userResp, err := e.userClient.AddUser(ctx, userReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to create user: %v", err)
		}
		return nil, fmt.Errorf("failed to create user: %v", err)
	}

	workspaceDescription := "Default workspace"

	// Create workspace using the workspace service
	workspaceReq := &corev1.AddWorkspaceRequest{
		TenantId:             targetTenant.TenantId,
		WorkspaceName:        setupReq.WorkspaceName,
		WorkspaceDescription: &workspaceDescription,
		OwnerId:              userResp.User.UserId,
	}
	workspaceResp, err := e.workspaceClient.AddWorkspace(ctx, workspaceReq)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to create workspace: %v", err)
		}
		return nil, fmt.Errorf("failed to create workspace: %v", err)
	}

	if e.logger != nil {
		e.logger.Infof("User setup completed successfully for tenant: %s, user: %s", tenantURL, setupReq.UserEmail)
	}

	return map[string]interface{}{
		"success": true,
		"message": "User setup completed successfully",
		"user": map[string]interface{}{
			"user_id":    userResp.User.UserId,
			"user_name":  userResp.User.UserName,
			"user_email": userResp.User.UserEmail,
		},
		"workspace": map[string]interface{}{
			"workspace_id":          workspaceResp.Workspace.WorkspaceId,
			"workspace_name":        workspaceResp.Workspace.WorkspaceName,
			"workspace_description": workspaceResp.Workspace.WorkspaceDescription,
		},
	}, nil
}

// GetNodeStatus returns the current initialization status of the node
func (e *Engine) GetNodeStatus(ctx context.Context) (interface{}, error) {
	// Check if tenants exist (indicates initialization has been run)
	tenantsReq := &corev1.ListTenantsRequest{}
	tenantsResp, err := e.tenantClient.ListTenants(ctx, tenantsReq)
	if err != nil {
		// If we can't connect to core service, node is not initialized
		if e.logger != nil {
			e.logger.Debugf("Failed to connect to core service for status check: %v", err)
		}
		return map[string]interface{}{
			"status":      "not_initialized",
			"description": "Node not initialized",
			"reachable":   true, // Client API is responding
		}, nil
	}

	// If no tenants exist, node is initialized but no users created yet
	if len(tenantsResp.Tenants) == 0 {
		return map[string]interface{}{
			"status":      "initialized_no_users",
			"description": "Initial user not created",
			"reachable":   true,
		}, nil
	}

	// Check if users exist in any tenant
	hasUsers := false
	for _, tenant := range tenantsResp.Tenants {
		usersReq := &corev1.ListUsersRequest{
			TenantId: tenant.TenantId,
		}
		usersResp, err := e.userClient.ListUsers(ctx, usersReq)
		if err != nil {
			continue // Skip this tenant if we can't check users
		}
		if len(usersResp.Users) > 0 {
			hasUsers = true
			break
		}
	}

	if !hasUsers {
		return map[string]interface{}{
			"status":      "initialized_no_users",
			"description": "Initial user not created",
			"reachable":   true,
		}, nil
	}

	// Check if node is part of a mesh using the mesh client
	meshStatus := "not_connected"
	meshDescription := "no mesh"

	if e.meshClient != nil {
		meshReq := &corev1.ShowMeshRequest{}
		meshResp, err := e.meshClient.ShowMesh(ctx, meshReq)
		if err == nil && meshResp.Mesh != nil {
			// Node is part of a mesh
			meshStatus = "connected"
			meshDescription = fmt.Sprintf("('%s' - %d nodes)",
				meshResp.Mesh.MeshId, meshResp.Mesh.NodeCount)

			return map[string]interface{}{
				"status":      "ready_with_mesh",
				"description": fmt.Sprintf("Ready, %s", meshDescription),
				"reachable":   true,
				"mesh_status": meshStatus,
				"mesh_info": map[string]interface{}{
					"mesh_id":    meshResp.Mesh.MeshId,
					"mesh_name":  meshResp.Mesh.MeshName,
					"node_count": meshResp.Mesh.NodeCount,
				},
			}, nil
		}
	}

	return map[string]interface{}{
		"status":      "ready_no_mesh",
		"description": fmt.Sprintf("Ready, %s", meshDescription),
		"reachable":   true,
		"mesh_status": meshStatus,
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
