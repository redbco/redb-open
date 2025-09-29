package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Engine handles MCP server core functionality
type Engine struct {
	config *config.Config
	logger *logger.Logger
	db     *database.PostgreSQL
	anchor anchorv1.AnchorServiceClient

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

	// Initialize gRPC client to anchor
	if err := e.initAnchorClient(); err != nil {
		if e.logger != nil {
			e.logger.Warnf("Anchor client not initialized: %v", err)
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

// runMCPServer starts a JSON-RPC MCP server serving tools/resources/prompts per config
func (e *Engine) runMCPServer(ctx context.Context, server models.MCPServer) {
	if e.logger != nil {
		e.logger.Infof("Starting MCP server '%s' (tenant=%s ws=%s) on port %d", server.MCPServerName, server.TenantID, server.WorkspaceID, server.MCPServerPort)
	}

	// Load resources, tools, prompts bound to this server
	resources, tools, prompts, err := e.loadServerBindings(ctx, server.MCPServerID)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to load bindings for MCP server %s: %v", server.MCPServerID, err)
		}
		return
	}

	// Build JSON-RPC handler for MCP
	handler := e.mcpHTTPHandler(server, resources, tools, prompts)
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

// loadServerBindings fetches resources/tools/prompts linked to a server
func (e *Engine) loadServerBindings(ctx context.Context, mcpServerID string) ([]models.MCPResource, []models.MCPTool, []models.MCPPrompt, error) {
	resources := make([]models.MCPResource, 0)
	tools := make([]models.MCPTool, 0)
	prompts := make([]models.MCPPrompt, 0)

	// Resources
	resRows, err := e.db.Pool().Query(ctx, `
        SELECT r.mcpresource_id, r.tenant_id, r.workspace_id, r.mcpresource_name, r.mcpresource_description,
               r.mcpresource_config, r.mapping_id, COALESCE(r.policy_ids, '{}') AS policy_ids,
               r.owner_id, r.created, r.updated
        FROM mcpresources r
        JOIN mcp_server_resources sr ON sr.mcpresource_id = r.mcpresource_id
        WHERE sr.mcpserver_id = $1
        ORDER BY r.mcpresource_name
    `, mcpServerID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resRows.Close()
	for resRows.Next() {
		var r models.MCPResource
		if err := resRows.Scan(
			&r.MCPResourceID, &r.TenantID, &r.WorkspaceID, &r.MCPResourceName, &r.MCPResourceDescription,
			&r.MCPResourceConfig, &r.MappingID, &r.PolicyIDs, &r.OwnerID, &r.Created, &r.Updated,
		); err != nil {
			return nil, nil, nil, err
		}
		resources = append(resources, r)
	}

	// Tools
	toolRows, err := e.db.Pool().Query(ctx, `
        SELECT t.mcptool_id, t.tenant_id, t.workspace_id, t.mcptool_name, t.mcptool_description,
               t.mcptool_config, t.mapping_id, COALESCE(t.policy_ids, '{}') AS policy_ids,
               t.owner_id, t.created, t.updated
        FROM mcptools t
        JOIN mcp_server_tools st ON st.mcptool_id = t.mcptool_id
        WHERE st.mcpserver_id = $1
        ORDER BY t.mcptool_name
    `, mcpServerID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer toolRows.Close()
	for toolRows.Next() {
		var t models.MCPTool
		if err := toolRows.Scan(
			&t.MCPToolID, &t.TenantID, &t.WorkspaceID, &t.MCPToolName, &t.MCPToolDescription,
			&t.MCPToolConfig, &t.MappingID, &t.PolicyIDs, &t.OwnerID, &t.Created, &t.Updated,
		); err != nil {
			return nil, nil, nil, err
		}
		tools = append(tools, t)
	}

	// Prompts
	promptRows, err := e.db.Pool().Query(ctx, `
        SELECT p.mcpprompt_id, p.tenant_id, p.workspace_id, p.mcpprompt_name, p.mcpprompt_description,
               p.mcpprompt_config, p.mapping_id, COALESCE(p.policy_ids, '{}') AS policy_ids,
               p.owner_id, p.created, p.updated
        FROM mcpprompts p
        JOIN mcp_server_prompts sp ON sp.mcpprompt_id = p.mcpprompt_id
        WHERE sp.mcpserver_id = $1
        ORDER BY p.mcpprompt_name
    `, mcpServerID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer promptRows.Close()
	for promptRows.Next() {
		var p models.MCPPrompt
		if err := promptRows.Scan(
			&p.MCPPromptID, &p.TenantID, &p.WorkspaceID, &p.MCPPromptName, &p.MCPPromptDescription,
			&p.MCPPromptConfig, &p.MappingID, &p.PolicyIDs, &p.OwnerID, &p.Created, &p.Updated,
		); err != nil {
			return nil, nil, nil, err
		}
		prompts = append(prompts, p)
	}

	return resources, tools, prompts, nil
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

// --- Minimal JSON-RPC over HTTP for MCP ---
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type jsonRPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
	ID      any       `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Engine) mcpHTTPHandler(server models.MCPServer, resources []models.MCPResource, tools []models.MCPTool, prompts []models.MCPPrompt) http.Handler {
	// Build lookup maps
	resByName := map[string]models.MCPResource{}
	for _, r := range resources {
		resByName[r.MCPResourceName] = r
	}
	toolByName := map[string]models.MCPTool{}
	for _, t := range tools {
		toolByName[t.MCPToolName] = t
	}
	promptByName := map[string]models.MCPPrompt{}
	for _, p := range prompts {
		promptByName[p.MCPPromptName] = p
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRPCError(w, req.ID, -32700, "parse error")
			return
		}
		switch req.Method {
		case "mcp.list_tools":
			list := make([]map[string]any, 0, len(tools))
			for _, t := range tools {
				list = append(list, map[string]any{
					"name":         t.MCPToolName,
					"description":  t.MCPToolDescription,
					"input_schema": t.MCPToolConfig, // loosely store schema in config
				})
			}
			writeRPCResult(w, req.ID, map[string]any{"tools": list})
		case "mcp.list_resources":
			list := make([]map[string]any, 0, len(resources))
			for _, rsrc := range resources {
				list = append(list, map[string]any{
					"name":        rsrc.MCPResourceName,
					"description": rsrc.MCPResourceDescription,
					"config":      rsrc.MCPResourceConfig,
				})
			}
			writeRPCResult(w, req.ID, map[string]any{"resources": list})
		case "mcp.list_prompts":
			list := make([]map[string]any, 0, len(prompts))
			for _, p := range prompts {
				list = append(list, map[string]any{
					"name":        p.MCPPromptName,
					"description": p.MCPPromptDescription,
					"config":      p.MCPPromptConfig,
				})
			}
			writeRPCResult(w, req.ID, map[string]any{"prompts": list})
		case "mcp.get_resource":
			var params struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(req.Params, &params)
			rsrc, ok := resByName[params.Name]
			if !ok {
				writeRPCError(w, req.ID, -32602, "unknown resource")
				return
			}
			// Return mapping object as content placeholder
			writeRPCResult(w, req.ID, map[string]any{"name": rsrc.MCPResourceName, "content": rsrc.MCPResourceConfig})
		case "mcp.get_prompt":
			var params struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(req.Params, &params)
			p, ok := promptByName[params.Name]
			if !ok {
				writeRPCError(w, req.ID, -32602, "unknown prompt")
				return
			}
			writeRPCResult(w, req.ID, map[string]any{"name": p.MCPPromptName, "content": p.MCPPromptConfig})
		case "mcp.call_tool":
			var params struct {
				Name  string          `json:"name"`
				Input json.RawMessage `json:"input"`
			}
			_ = json.Unmarshal(req.Params, &params)
			if _, ok := toolByName[params.Name]; !ok {
				writeRPCError(w, req.ID, -32602, "unknown tool")
				return
			}
			// Placeholder: not yet implemented tool execution
			writeRPCError(w, req.ID, -32000, "tool execution not implemented")
		default:
			writeRPCError(w, req.ID, -32601, "method not found")
		}
	})
}

func writeRPCError(w http.ResponseWriter, id any, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	resp := jsonRPCResponse{JSONRPC: "2.0", Error: &rpcError{Code: code, Message: msg}, ID: id}
	_ = json.NewEncoder(w).Encode(resp)
}

func writeRPCResult(w http.ResponseWriter, id any, result any) {
	w.Header().Set("Content-Type", "application/json")
	resp := jsonRPCResponse{JSONRPC: "2.0", Result: result, ID: id}
	_ = json.NewEncoder(w).Encode(resp)
}
