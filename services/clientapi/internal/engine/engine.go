package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// Connect to core service
	coreAddr := e.config.Get("services.core.grpc_address")
	if coreAddr == "" {
		coreAddr = "localhost:50062" // Fixed: Use correct core service port (matches core service default)
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

	// Connect to security service
	securityAddr := e.config.Get("services.security.grpc_address")
	if securityAddr == "" {
		securityAddr = "localhost:50051" // Default security service port
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

	// Initialize the security client
	e.securityClient = securityv1.NewSecurityServiceClient(securityConn)

	if e.logger != nil {
		e.logger.Infof("Security client initialized successfully")
	}

	// Initialize HTTP server
	// Check for external_port from environment first, then fall back to config
	portStr := os.Getenv("EXTERNAL_PORT")
	if portStr == "" {
		portStr = e.config.Get("services.clientapi.http_port") // Fallback to config
	}
	if portStr == "" {
		portStr = "8080" // Default HTTP port
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
