package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/redbco/redb-open/pkg/logger"
)

// Handler handles MCP protocol requests
type Handler struct {
	logger       *logger.Logger
	initialized  bool
	capabilities ServerCapabilities
	serverInfo   ImplementationInfo

	// Method handlers
	resourceHandler ResourceHandler
	toolHandler     ToolHandler
	promptHandler   PromptHandler

	// Audit logger (optional)
	auditLogger AuditLogger
}

// AuditLogger interface for audit logging
type AuditLogger interface {
	LogInitialize(ctx context.Context, clientName string, clientVersion string, success bool) error
	LogResourceRead(ctx context.Context, resourceID string, uri string, success bool) error
	LogToolCall(ctx context.Context, toolID string, toolName string, args map[string]interface{}, success bool) error
	LogPromptGet(ctx context.Context, promptID string, promptName string, args map[string]interface{}, success bool) error
}

// ResourceHandler handles resource operations
type ResourceHandler interface {
	List(ctx context.Context, req *ListResourcesRequest) (*ListResourcesResult, error)
	Read(ctx context.Context, req *ReadResourceRequest) (*ReadResourceResult, error)
	Subscribe(ctx context.Context, req *SubscribeRequest) error
	Unsubscribe(ctx context.Context, req *UnsubscribeRequest) error
}

// ToolHandler handles tool operations
type ToolHandler interface {
	List(ctx context.Context, req *ListToolsRequest) (*ListToolsResult, error)
	Call(ctx context.Context, req *CallToolRequest) (*CallToolResult, error)
}

// PromptHandler handles prompt operations
type PromptHandler interface {
	List(ctx context.Context, req *ListPromptsRequest) (*ListPromptsResult, error)
	Get(ctx context.Context, req *GetPromptRequest) (*GetPromptResult, error)
}

// NewHandler creates a new MCP protocol handler
func NewHandler(logger *logger.Logger) *Handler {
	return &Handler{
		logger: logger,
		capabilities: ServerCapabilities{
			Resources: &ResourcesCapability{
				Subscribe:   true,
				ListChanged: false,
			},
			Tools: &ToolsCapability{
				ListChanged: false,
			},
			Prompts: &PromptsCapability{
				ListChanged: false,
			},
		},
		serverInfo: ImplementationInfo{
			Name:    "redb-mcp-server",
			Version: "1.0.0",
		},
	}
}

// SetResourceHandler sets the resource handler
func (h *Handler) SetResourceHandler(handler ResourceHandler) {
	h.resourceHandler = handler
}

// SetToolHandler sets the tool handler
func (h *Handler) SetToolHandler(handler ToolHandler) {
	h.toolHandler = handler
}

// SetPromptHandler sets the prompt handler
func (h *Handler) SetPromptHandler(handler PromptHandler) {
	h.promptHandler = handler
}

// SetAuditLogger sets the audit logger
func (h *Handler) SetAuditLogger(logger AuditLogger) {
	h.auditLogger = logger
}

// ServeHTTP implements http.Handler for MCP protocol
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, nil, ParseError, "Failed to read request body")
		return
	}
	defer r.Body.Close()

	// Parse JSON-RPC request
	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, nil, ParseError, "Invalid JSON")
		return
	}

	// Validate JSON-RPC version
	if req.JSONRPC != "2.0" {
		h.writeError(w, req.ID, InvalidRequest, "Invalid JSON-RPC version")
		return
	}

	// Handle the request
	ctx := r.Context()
	result, err := h.handleMethod(ctx, req.Method, req.Params)
	if err != nil {
		if rpcErr, ok := err.(*RPCError); ok {
			h.writeError(w, req.ID, rpcErr.Code, rpcErr.Message)
		} else {
			h.writeError(w, req.ID, InternalError, err.Error())
		}
		return
	}

	// Write response
	h.writeResult(w, req.ID, result)
}

// handleMethod routes the method to appropriate handler
func (h *Handler) handleMethod(ctx context.Context, method string, params interface{}) (interface{}, error) {
	switch method {
	case "initialize":
		return h.handleInitialize(ctx, params)
	case "initialized":
		// Notification that initialization is complete
		return nil, nil
	case "resources/list":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handleResourcesList(ctx, params)
	case "resources/read":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handleResourcesRead(ctx, params)
	case "resources/subscribe":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handleResourcesSubscribe(ctx, params)
	case "resources/unsubscribe":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handleResourcesUnsubscribe(ctx, params)
	case "tools/list":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handleToolsList(ctx, params)
	case "tools/call":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handleToolsCall(ctx, params)
	case "prompts/list":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handlePromptsList(ctx, params)
	case "prompts/get":
		if !h.initialized {
			return nil, &RPCError{Code: InvalidRequest, Message: "Not initialized"}
		}
		return h.handlePromptsGet(ctx, params)
	default:
		return nil, &RPCError{Code: MethodNotFound, Message: fmt.Sprintf("Method not found: %s", method)}
	}
}

// handleInitialize handles the initialize method
func (h *Handler) handleInitialize(ctx context.Context, params interface{}) (interface{}, error) {
	var req InitializeRequest
	if err := h.unmarshalParams(params, &req); err != nil {
		return nil, &RPCError{Code: InvalidParams, Message: "Invalid initialize params"}
	}

	h.logger.Infof("MCP client connecting: %s v%s", req.ClientInfo.Name, req.ClientInfo.Version)

	h.initialized = true

	// Audit log initialization
	if h.auditLogger != nil {
		h.auditLogger.LogInitialize(ctx, req.ClientInfo.Name, req.ClientInfo.Version, true)
	}

	return InitializeResult{
		ProtocolVersion: "2024-11-05",
		Capabilities:    h.capabilities,
		ServerInfo:      h.serverInfo,
	}, nil
}

// handleResourcesList handles the resources/list method
func (h *Handler) handleResourcesList(ctx context.Context, params interface{}) (interface{}, error) {
	if h.resourceHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Resource handler not configured"}
	}

	var req ListResourcesRequest
	if params != nil {
		if err := h.unmarshalParams(params, &req); err != nil {
			return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
		}
	}

	return h.resourceHandler.List(ctx, &req)
}

// handleResourcesRead handles the resources/read method
func (h *Handler) handleResourcesRead(ctx context.Context, params interface{}) (interface{}, error) {
	if h.resourceHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Resource handler not configured"}
	}

	var req ReadResourceRequest
	if err := h.unmarshalParams(params, &req); err != nil {
		return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
	}

	return h.resourceHandler.Read(ctx, &req)
}

// handleResourcesSubscribe handles the resources/subscribe method
func (h *Handler) handleResourcesSubscribe(ctx context.Context, params interface{}) (interface{}, error) {
	if h.resourceHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Resource handler not configured"}
	}

	var req SubscribeRequest
	if err := h.unmarshalParams(params, &req); err != nil {
		return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
	}

	return nil, h.resourceHandler.Subscribe(ctx, &req)
}

// handleResourcesUnsubscribe handles the resources/unsubscribe method
func (h *Handler) handleResourcesUnsubscribe(ctx context.Context, params interface{}) (interface{}, error) {
	if h.resourceHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Resource handler not configured"}
	}

	var req UnsubscribeRequest
	if err := h.unmarshalParams(params, &req); err != nil {
		return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
	}

	return nil, h.resourceHandler.Unsubscribe(ctx, &req)
}

// handleToolsList handles the tools/list method
func (h *Handler) handleToolsList(ctx context.Context, params interface{}) (interface{}, error) {
	if h.toolHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Tool handler not configured"}
	}

	var req ListToolsRequest
	if params != nil {
		if err := h.unmarshalParams(params, &req); err != nil {
			return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
		}
	}

	return h.toolHandler.List(ctx, &req)
}

// handleToolsCall handles the tools/call method
func (h *Handler) handleToolsCall(ctx context.Context, params interface{}) (interface{}, error) {
	if h.toolHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Tool handler not configured"}
	}

	var req CallToolRequest
	if err := h.unmarshalParams(params, &req); err != nil {
		return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
	}

	return h.toolHandler.Call(ctx, &req)
}

// handlePromptsList handles the prompts/list method
func (h *Handler) handlePromptsList(ctx context.Context, params interface{}) (interface{}, error) {
	if h.promptHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Prompt handler not configured"}
	}

	var req ListPromptsRequest
	if params != nil {
		if err := h.unmarshalParams(params, &req); err != nil {
			return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
		}
	}

	return h.promptHandler.List(ctx, &req)
}

// handlePromptsGet handles the prompts/get method
func (h *Handler) handlePromptsGet(ctx context.Context, params interface{}) (interface{}, error) {
	if h.promptHandler == nil {
		return nil, &RPCError{Code: InternalError, Message: "Prompt handler not configured"}
	}

	var req GetPromptRequest
	if err := h.unmarshalParams(params, &req); err != nil {
		return nil, &RPCError{Code: InvalidParams, Message: "Invalid parameters"}
	}

	return h.promptHandler.Get(ctx, &req)
}

// unmarshalParams unmarshals params into the target struct
func (h *Handler) unmarshalParams(params interface{}, target interface{}) error {
	// Re-marshal and unmarshal to convert map to struct
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// writeResult writes a JSON-RPC success response
func (h *Handler) writeResult(w http.ResponseWriter, id interface{}, result interface{}) {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		Result:  result,
		ID:      id,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// writeError writes a JSON-RPC error response
func (h *Handler) writeError(w http.ResponseWriter, id interface{}, code int, message string) {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		Error: &RPCError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
