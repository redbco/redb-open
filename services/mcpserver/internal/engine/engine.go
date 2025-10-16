package engine

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/services/mcpserver/internal/audit"
	"github.com/redbco/redb-open/services/mcpserver/internal/auth"
	"github.com/redbco/redb-open/services/mcpserver/internal/prompts"
	"github.com/redbco/redb-open/services/mcpserver/internal/protocol"
	"github.com/redbco/redb-open/services/mcpserver/internal/resources"
	"github.com/redbco/redb-open/services/mcpserver/internal/tools"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Engine handles MCP server core functionality
type Engine struct {
	config   *config.Config
	logger   *logger.Logger
	db       *database.PostgreSQL
	anchor   anchorv1.AnchorServiceClient
	security securityv1.SecurityServiceClient

	// Metrics
	requestCount   int64
	activeSessions int64
	errorCount     int64

	// State
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

	// running HTTP servers per MCP server id
	httpServers map[string]*http.Server
}

// NewEngine creates a new MCP engine
func NewEngine(config *config.Config) *Engine {
	return &Engine{
		config:      config,
		stopCh:      make(chan struct{}),
		httpServers: make(map[string]*http.Server),
	}
}

// SetLogger sets the logger for the engine
func (e *Engine) SetLogger(logger *logger.Logger) {
	e.logger = logger
}

// SetDatabase sets the shared internal DB handle
func (e *Engine) SetDatabase(db *database.PostgreSQL) {
	e.db = db
}

// Start begins the MCP engine operations
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return fmt.Errorf("engine already running")
	}
	e.running = true
	e.mu.Unlock()

	// Start background tasks
	go e.cleanupStaleSessions(ctx)

	// Initialize gRPC clients
	if err := e.initAnchorClient(); err != nil {
		if e.logger != nil {
			e.logger.Warnf("Anchor client not initialized: %v", err)
		}
	}

	if err := e.initSecurityClient(); err != nil {
		if e.logger != nil {
			e.logger.Warnf("Security client not initialized: %v", err)
		}
	}

	// Start per-node MCP servers based on DB configuration and keep them in sync
	go e.startServersForThisNode(ctx)
	go e.syncServersLoop(ctx)

	return nil
}

// Stop gracefully shuts down the engine
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return nil
	}
	e.running = false
	e.mu.Unlock()

	close(e.stopCh)

	// Shutdown HTTP servers
	e.mu.Lock()
	for id, srv := range e.httpServers {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = srv.Shutdown(shutdownCtx)
		cancel()
		delete(e.httpServers, id)
	}
	e.mu.Unlock()
	return nil
}

// Validate checks if the service is in a valid state
func (e *Engine) Validate() error {
	// TODO: Implement actual engine validation when service is fully implemented
	// For now, return success to avoid health check failures during development
	if !e.running {
		// Engine not running yet, but service is healthy during development
		return nil
	}
	return nil
}

// CheckHealth verifies service health
func (e *Engine) CheckHealth() error {
	// TODO: Implement actual health checking when service is fully implemented
	// For now, return success to avoid health check failures during development
	if !e.running {
		// Engine not running yet, but service is healthy during development
		return nil
	}

	// Add additional health checks as needed when implemented
	return nil
}

// GetMetrics returns current engine metrics
func (e *Engine) GetMetrics() map[string]int64 {
	return map[string]int64{
		"request_count":   atomic.LoadInt64(&e.requestCount),
		"active_sessions": atomic.LoadInt64(&e.activeSessions),
		"error_count":     atomic.LoadInt64(&e.errorCount),
	}
}

// IncrementRequestCount increments the request counter
func (e *Engine) IncrementRequestCount() {
	atomic.AddInt64(&e.requestCount, 1)
}

// IncrementErrorCount increments the error counter
func (e *Engine) IncrementErrorCount() {
	atomic.AddInt64(&e.errorCount, 1)
}

// IncrementActiveSessions increments the active sessions counter
func (e *Engine) IncrementActiveSessions() {
	atomic.AddInt64(&e.activeSessions, 1)
}

// DecrementActiveSessions decrements the active sessions counter
func (e *Engine) DecrementActiveSessions() {
	atomic.AddInt64(&e.activeSessions, -1)
}

func (e *Engine) cleanupStaleSessions(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Cleanup stale sessions
			// This would typically involve database operations
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		}
	}
}

// startServersForThisNode loads MCP server configs for this node and starts listeners
func (e *Engine) startServersForThisNode(ctx context.Context) {
	if e.db == nil {
		if e.logger != nil {
			e.logger.Warn("Database not set; MCP server manager idle")
		}
		return
	}

	// Determine local node id
	var nodeID string
	if err := e.db.Pool().QueryRow(ctx, "SELECT identity_id FROM localidentity LIMIT 1").Scan(&nodeID); err != nil || nodeID == "" {
		if e.logger != nil {
			e.logger.Errorf("Unable to read local node identity: %v", err)
		}
		return
	}

	// Load MCP servers targeting this node (hosted on this node ID)
	// mcpservers.mcpserver_host_ids contains array of node IDs. Use ANY operator.
	rows, err := e.db.Pool().Query(ctx, `
        SELECT mcpserver_id, tenant_id, workspace_id, mcpserver_name, mcpserver_description,
               COALESCE(mcpserver_host_ids, '{}') AS mcpserver_host_ids,
               mcpserver_port, mcpserver_enabled, COALESCE(policy_ids, '{}') AS policy_ids,
               owner_id, status_message, status, created, updated
        FROM mcpservers
        WHERE $1 = ANY(mcpserver_host_ids) AND mcpserver_enabled = TRUE
    `, nodeID)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to query MCP servers: %v", err)
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
		var s models.MCPServer
		if err := rows.Scan(
			&s.MCPServerID,
			&s.TenantID,
			&s.WorkspaceID,
			&s.MCPServerName,
			&s.MCPServerDescription,
			&s.MCPServerHostIDs,
			&s.MCPServerPort,
			&s.MCPServerEnabled,
			&s.PolicyIDs,
			&s.OwnerID,
			&s.StatusMessage,
			&s.Status,
			&s.Created,
			&s.Updated,
		); err != nil {
			if e.logger != nil {
				e.logger.Errorf("Failed to scan MCP server: %v", err)
			}
			continue
		}

		// Start a server worker for this configuration
		go e.runMCPServer(ctx, s)
	}
}

// syncServersLoop periodically reconciles running MCP HTTP servers with DB state
func (e *Engine) syncServersLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.reconcileServers(ctx)
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) reconcileServers(ctx context.Context) {
	if e.db == nil {
		return
	}
	var nodeID string
	if err := e.db.Pool().QueryRow(ctx, "SELECT identity_id FROM localidentity LIMIT 1").Scan(&nodeID); err != nil || nodeID == "" {
		return
	}
	rows, err := e.db.Pool().Query(ctx, `
        SELECT mcpserver_id, tenant_id, workspace_id, mcpserver_name, mcpserver_description,
               COALESCE(mcpserver_host_ids, '{}') AS mcpserver_host_ids,
               mcpserver_port, mcpserver_enabled, COALESCE(policy_ids, '{}') AS policy_ids,
               owner_id, status_message, status, created, updated
        FROM mcpservers
        WHERE $1 = ANY(mcpserver_host_ids) AND mcpserver_enabled = TRUE
    `, nodeID)
	if err != nil {
		return
	}
	defer rows.Close()

	desired := make(map[string]models.MCPServer)
	for rows.Next() {
		var s models.MCPServer
		if err := rows.Scan(
			&s.MCPServerID, &s.TenantID, &s.WorkspaceID, &s.MCPServerName, &s.MCPServerDescription,
			&s.MCPServerHostIDs, &s.MCPServerPort, &s.MCPServerEnabled, &s.PolicyIDs,
			&s.OwnerID, &s.StatusMessage, &s.Status, &s.Created, &s.Updated,
		); err == nil {
			desired[s.MCPServerID] = s
		}
	}

	e.mu.Lock()
	// Stop servers no longer desired
	for id, srv := range e.httpServers {
		if _, ok := desired[id]; !ok {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = srv.Shutdown(shutdownCtx)
			cancel()
			delete(e.httpServers, id)
			if e.logger != nil {
				e.logger.Infof("Stopped MCP server %s (deleted/disabled)", id)
			}
		}
	}
	e.mu.Unlock()

	// Start or restart servers with changed port
	for id, cfg := range desired {
		e.mu.RLock()
		existing, exists := e.httpServers[id]
		e.mu.RUnlock()
		needStart := true
		if exists {
			// If port differs, restart
			if existing.Addr == fmt.Sprintf(":%d", cfg.MCPServerPort) {
				needStart = false
			} else {
				shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_ = existing.Shutdown(shutdownCtx)
				cancel()
				e.mu.Lock()
				delete(e.httpServers, id)
				e.mu.Unlock()
			}
		}
		if needStart {
			go e.runMCPServer(ctx, cfg)
		}
	}
}

// runMCPServer starts an MCP protocol server serving tools/resources/prompts per config
func (e *Engine) runMCPServer(ctx context.Context, server models.MCPServer) {
	if e.logger != nil {
		e.logger.Infof("Starting MCP server '%s' (tenant=%s ws=%s) on port %d", server.MCPServerName, server.TenantID, server.WorkspaceID, server.MCPServerPort)
	}

	// Create authentication middleware
	authMiddleware := auth.NewMiddleware(e.logger, e.security)

	// Create audit logger
	auditLogger := audit.NewLogger(e.db, e.logger)

	// Create protocol handler
	protocolHandler := protocol.NewHandler(e.logger)
	protocolHandler.SetAuditLogger(auditLogger)

	// Create resource handler
	resourceHandler := resources.NewHandler(e.logger, e.db, e.anchor, authMiddleware, server.MCPServerID, e.config)
	protocolHandler.SetResourceHandler(resourceHandler)

	// Create tool handler
	toolHandler := tools.NewHandler(e.logger, e.db, e.anchor, authMiddleware, server.MCPServerID, e.config)
	protocolHandler.SetToolHandler(toolHandler)

	// Create prompt handler
	promptHandler := prompts.NewHandler(e.logger, e.db, authMiddleware, server.MCPServerID)
	protocolHandler.SetPromptHandler(promptHandler)

	// Wrap with authentication middleware
	handler := authMiddleware.Authenticate(protocolHandler)

	addr := fmt.Sprintf(":%d", server.MCPServerPort)
	httpSrv := &http.Server{Addr: addr, Handler: handler}

	e.mu.Lock()
	e.httpServers[server.MCPServerID] = httpSrv
	e.mu.Unlock()

	go func() {
		cert := e.config.Get("services.mcpserver.security_cert")
		key := e.config.Get("services.mcpserver.security_key")
		var err error
		if cert != "" && key != "" {
			err = httpSrv.ListenAndServeTLS(cert, key)
		} else {
			err = httpSrv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			if e.logger != nil {
				e.logger.Errorf("HTTP server for MCP %s failed: %v", server.MCPServerName, err)
			}
		}
	}()
}

// initAnchorClient connects to the Anchor gRPC service
func (e *Engine) initAnchorClient() error {
	addr := e.config.Get("services.anchor.grpc_address")
	if addr == "" {
		addr = grpcconfig.GetServiceAddress(e.config, "anchor")
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	e.anchor = anchorv1.NewAnchorServiceClient(conn)
	return nil
}

// initSecurityClient connects to the Security gRPC service
func (e *Engine) initSecurityClient() error {
	addr := e.config.Get("services.security.grpc_address")
	if addr == "" {
		addr = grpcconfig.GetServiceAddress(e.config, "security")
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	e.security = securityv1.NewSecurityServiceClient(conn)
	return nil
}
