package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// MCPHandlers provides HTTP handlers for MCP management
type MCPHandlers struct {
	engine *Engine
}

// NewMCPHandlers creates a new MCP handlers instance
func NewMCPHandlers(engine *Engine) *MCPHandlers {
	return &MCPHandlers{engine: engine}
}

// ============================================================================
// MCP Server Handlers
// ============================================================================

// ListMCPServers handles GET /mcpservers
func (h *MCPHandlers) ListMCPServers(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ListMCPServersRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	resp, err := h.engine.mcpClient.ListMCPServers(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to list MCP servers")
		return
	}

	// Convert protobuf responses to JSON-friendly format
	servers := make([]map[string]interface{}, len(resp.McpServers))
	for i, server := range resp.McpServers {
		servers[i] = h.mcpServerToJSON(server)
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"mcp_servers": servers,
	})
}

// ShowMCPServer handles GET /mcpservers/{mcpserver_name}
func (h *MCPHandlers) ShowMCPServer(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcpserverName := vars["mcpserver_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ShowMCPServerRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		McpServerName: mcpserverName,
	}

	resp, err := h.engine.mcpClient.ShowMCPServer(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to get MCP server")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"mcp_server": h.mcpServerToJSON(resp.McpServer),
	})
}

// AddMCPServer handles POST /mcpservers
func (h *MCPHandlers) AddMCPServer(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPServerName        string   `json:"mcp_server_name"`
		MCPServerDescription string   `json:"mcp_server_description"`
		MCPServerHostIDs     []string `json:"mcp_server_host_ids"`
		MCPServerPort        int32    `json:"mcp_server_port"`
		MCPServerEnabled     bool     `json:"mcp_server_enabled"`
		PolicyIDs            []string `json:"policy_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.AddMCPServerRequest{
		TenantId:             profile.TenantId,
		WorkspaceName:        workspaceName,
		McpServerName:        reqBody.MCPServerName,
		McpServerDescription: reqBody.MCPServerDescription,
		McpServerHostIds:     reqBody.MCPServerHostIDs,
		McpServerPort:        reqBody.MCPServerPort,
		McpServerEnabled:     reqBody.MCPServerEnabled,
		PolicyIds:            reqBody.PolicyIDs,
		OwnerId:              profile.UserId,
	}

	resp, err := h.engine.mcpClient.AddMCPServer(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to create MCP server")
		return
	}

	// Convert protobuf response to JSON-friendly format
	serverJSON := h.mcpServerToJSON(resp.McpServer)

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":    resp.Message,
		"success":    resp.Success,
		"mcp_server": serverJSON,
		"status":     resp.Status.String(),
	})
}

// ModifyMCPServer handles PUT /mcpservers/{mcpserver_name}
func (h *MCPHandlers) ModifyMCPServer(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcpserverName := vars["mcpserver_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPServerDescription *string  `json:"mcp_server_description,omitempty"`
		MCPServerHostIDs     []string `json:"mcp_server_host_ids,omitempty"`
		MCPServerPort        *int32   `json:"mcp_server_port,omitempty"`
		MCPServerEnabled     *bool    `json:"mcp_server_enabled,omitempty"`
		PolicyIDs            []string `json:"policy_ids,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ModifyMCPServerRequest{
		TenantId:             profile.TenantId,
		WorkspaceName:        workspaceName,
		McpServerName:        mcpserverName,
		McpServerDescription: reqBody.MCPServerDescription,
		McpServerHostIds:     reqBody.MCPServerHostIDs,
		McpServerPort:        reqBody.MCPServerPort,
		McpServerEnabled:     reqBody.MCPServerEnabled,
		PolicyIds:            reqBody.PolicyIDs,
	}

	resp, err := h.engine.mcpClient.ModifyMCPServer(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to modify MCP server")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":    resp.Message,
		"success":    resp.Success,
		"mcp_server": h.mcpServerToJSON(resp.McpServer),
		"status":     resp.Status.String(),
	})
}

// DeleteMCPServer handles DELETE /mcpservers/{mcpserver_name}
func (h *MCPHandlers) DeleteMCPServer(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcpserverName := vars["mcpserver_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.DeleteMCPServerRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		McpServerName: mcpserverName,
	}

	resp, err := h.engine.mcpClient.DeleteMCPServer(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to delete MCP server")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// ============================================================================
// MCP Resource Handlers
// ============================================================================

// ListMCPResources handles GET /mcpresources
func (h *MCPHandlers) ListMCPResources(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ListMCPResourcesRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	resp, err := h.engine.mcpClient.ListMCPResources(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to list MCP resources")
		return
	}

	// Convert protobuf responses to JSON-friendly format
	resources := make([]map[string]interface{}, len(resp.McpResources))
	for i, resource := range resp.McpResources {
		resources[i] = h.mcpResourceToJSON(resource)
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"mcp_resources": resources,
	})
}

// ShowMCPResource handles GET /mcpresources/{mcpresource_name}
func (h *MCPHandlers) ShowMCPResource(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcpresourceName := vars["mcpresource_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ShowMCPResourceRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		McpResourceName: mcpresourceName,
	}

	resp, err := h.engine.mcpClient.ShowMCPResource(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to get MCP resource")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"mcp_resource": h.mcpResourceToJSON(resp.McpResource),
	})
}

// AddMCPResource handles POST /mcpresources
func (h *MCPHandlers) AddMCPResource(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPResourceName        string                 `json:"mcp_resource_name"`
		MCPResourceDescription string                 `json:"mcp_resource_description"`
		MCPResourceConfig      map[string]interface{} `json:"mcp_resource_config"`
		MappingName            string                 `json:"mapping_name"`
		PolicyIDs              []string               `json:"policy_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	// Convert config to structpb.Struct
	var configStruct *structpb.Struct
	var err error
	if len(reqBody.MCPResourceConfig) > 0 {
		configStruct, err = structpb.NewStruct(reqBody.MCPResourceConfig)
		if err != nil {
			h.writeErrorResponse(w, http.StatusBadRequest, "invalid config", err.Error())
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.AddMCPResourceRequest{
		TenantId:               profile.TenantId,
		WorkspaceName:          workspaceName,
		McpResourceName:        reqBody.MCPResourceName,
		McpResourceDescription: reqBody.MCPResourceDescription,
		McpResourceConfig:      configStruct,
		MappingName:            reqBody.MappingName,
		PolicyIds:              reqBody.PolicyIDs,
		OwnerId:                profile.UserId,
	}

	resp, err := h.engine.mcpClient.AddMCPResource(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to create MCP resource")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":      resp.Message,
		"success":      resp.Success,
		"mcp_resource": h.mcpResourceToJSON(resp.McpResource),
		"status":       resp.Status.String(),
	})
}

// AttachMCPResource handles POST /mcpresources/attach
func (h *MCPHandlers) AttachMCPResource(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPResourceName string `json:"mcp_resource_name"`
		MCPServerName   string `json:"mcp_server_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.AttachMCPResourceRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		McpResourceName: reqBody.MCPResourceName,
		McpServerName:   reqBody.MCPServerName,
	}

	resp, err := h.engine.mcpClient.AttachMCPResource(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to attach MCP resource")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// DetachMCPResource handles POST /mcpresources/detach
func (h *MCPHandlers) DetachMCPResource(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPResourceName string `json:"mcp_resource_name"`
		MCPServerName   string `json:"mcp_server_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.DetachMCPResourceRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		McpResourceName: reqBody.MCPResourceName,
		McpServerName:   reqBody.MCPServerName,
	}

	resp, err := h.engine.mcpClient.DetachMCPResource(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to detach MCP resource")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// DeleteMCPResource handles DELETE /mcpresources/{mcpresource_name}
func (h *MCPHandlers) DeleteMCPResource(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcpresourceName := vars["mcpresource_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.DeleteMCPResourceRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		McpResourceName: mcpresourceName,
	}

	resp, err := h.engine.mcpClient.DeleteMCPResource(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to delete MCP resource")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// ============================================================================
// MCP Tool Handlers
// ============================================================================

// ListMCPTools handles GET /mcptools
func (h *MCPHandlers) ListMCPTools(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ListMCPToolsRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	resp, err := h.engine.mcpClient.ListMCPTools(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to list MCP tools")
		return
	}

	// Convert protobuf responses to JSON-friendly format
	tools := make([]map[string]interface{}, len(resp.McpTools))
	for i, tool := range resp.McpTools {
		tools[i] = h.mcpToolToJSON(tool)
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"mcp_tools": tools,
	})
}

// ShowMCPTool handles GET /mcptools/{mcptool_name}
func (h *MCPHandlers) ShowMCPTool(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcptoolName := vars["mcptool_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.ShowMCPToolRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		McpToolName:   mcptoolName,
	}

	resp, err := h.engine.mcpClient.ShowMCPTool(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to get MCP tool")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"mcp_tool": h.mcpToolToJSON(resp.McpTool),
	})
}

// AddMCPTool handles POST /mcptools
func (h *MCPHandlers) AddMCPTool(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPToolName        string                 `json:"mcp_tool_name"`
		MCPToolDescription string                 `json:"mcp_tool_description"`
		MCPToolConfig      map[string]interface{} `json:"mcp_tool_config"`
		MappingName        string                 `json:"mapping_name"`
		PolicyIDs          []string               `json:"policy_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	// Convert config to structpb.Struct
	var configStruct *structpb.Struct
	var err error
	if len(reqBody.MCPToolConfig) > 0 {
		configStruct, err = structpb.NewStruct(reqBody.MCPToolConfig)
		if err != nil {
			h.writeErrorResponse(w, http.StatusBadRequest, "invalid config", err.Error())
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.AddMCPToolRequest{
		TenantId:           profile.TenantId,
		WorkspaceName:      workspaceName,
		McpToolName:        reqBody.MCPToolName,
		McpToolDescription: reqBody.MCPToolDescription,
		McpToolConfig:      configStruct,
		MappingName:        reqBody.MappingName,
		PolicyIds:          reqBody.PolicyIDs,
		OwnerId:            profile.UserId,
	}

	resp, err := h.engine.mcpClient.AddMCPTool(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to create MCP tool")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":  resp.Message,
		"success":  resp.Success,
		"mcp_tool": h.mcpToolToJSON(resp.McpTool),
		"status":   resp.Status.String(),
	})
}

// AttachMCPTool handles POST /mcptools/attach
func (h *MCPHandlers) AttachMCPTool(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPToolName   string `json:"mcp_tool_name"`
		MCPServerName string `json:"mcp_server_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.AttachMCPToolRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		McpToolName:   reqBody.MCPToolName,
		McpServerName: reqBody.MCPServerName,
	}

	resp, err := h.engine.mcpClient.AttachMCPTool(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to attach MCP tool")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// DetachMCPTool handles POST /mcptools/detach
func (h *MCPHandlers) DetachMCPTool(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	var reqBody struct {
		MCPToolName   string `json:"mcp_tool_name"`
		MCPServerName string `json:"mcp_server_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.writeErrorResponse(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.DetachMCPToolRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		McpToolName:   reqBody.MCPToolName,
		McpServerName: reqBody.MCPServerName,
	}

	resp, err := h.engine.mcpClient.DetachMCPTool(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to detach MCP tool")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// DeleteMCPTool handles DELETE /mcptools/{mcptool_name}
func (h *MCPHandlers) DeleteMCPTool(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	workspaceName := vars["workspace_name"]
	mcptoolName := vars["mcptool_name"]

	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req := &corev1.DeleteMCPToolRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		McpToolName:   mcptoolName,
	}

	resp, err := h.engine.mcpClient.DeleteMCPTool(ctx, req)
	if err != nil {
		h.handleGRPCError(w, err, "Failed to delete MCP tool")
		return
	}

	h.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message": resp.Message,
		"success": resp.Success,
		"status":  resp.Status.String(),
	})
}

// ============================================================================
// Helper methods
// ============================================================================

func (h *MCPHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (h *MCPHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":   message,
		"details": details,
	})
}

func (h *MCPHandlers) handleGRPCError(w http.ResponseWriter, err error, message string) {
	if h.engine.logger != nil {
		h.engine.logger.Errorf("%s: %v", message, err)
	}
	h.writeErrorResponse(w, http.StatusInternalServerError, message, fmt.Sprintf("%v", err))
}

// mcpServerToJSON converts a protobuf MCPServer to a JSON-friendly map
func (h *MCPHandlers) mcpServerToJSON(server *corev1.MCPServer) map[string]interface{} {
	if server == nil {
		return nil
	}

	return map[string]interface{}{
		"mcp_server_id":          server.McpServerId,
		"tenant_id":              server.TenantId,
		"workspace_id":           server.WorkspaceId,
		"mcp_server_name":        server.McpServerName,
		"mcp_server_description": server.McpServerDescription,
		"mcp_server_host_ids":    server.McpServerHostIds,
		"mcp_server_port":        server.McpServerPort,
		"mcp_server_enabled":     server.McpServerEnabled,
		"policy_ids":             server.PolicyIds,
		"owner_id":               server.OwnerId,
		"status_message":         server.StatusMessage,
		"status":                 server.Status.String(), // Convert enum to string
	}
}

// mcpResourceToJSON converts a protobuf MCPResource to a JSON-friendly map
func (h *MCPHandlers) mcpResourceToJSON(resource *corev1.MCPResource) map[string]interface{} {
	if resource == nil {
		return nil
	}

	return map[string]interface{}{
		"mcp_resource_id":          resource.McpResourceId,
		"tenant_id":                resource.TenantId,
		"workspace_id":             resource.WorkspaceId,
		"mcp_resource_name":        resource.McpResourceName,
		"mcp_resource_description": resource.McpResourceDescription,
		"mcp_resource_config":      resource.McpResourceConfig.AsMap(),
		"mapping_id":               resource.MappingId,
		"policy_ids":               resource.PolicyIds,
		"owner_id":                 resource.OwnerId,
	}
}

// mcpToolToJSON converts a protobuf MCPTool to a JSON-friendly map
func (h *MCPHandlers) mcpToolToJSON(tool *corev1.MCPTool) map[string]interface{} {
	if tool == nil {
		return nil
	}

	return map[string]interface{}{
		"mcp_tool_id":          tool.McpToolId,
		"tenant_id":            tool.TenantId,
		"workspace_id":         tool.WorkspaceId,
		"mcp_tool_name":        tool.McpToolName,
		"mcp_tool_description": tool.McpToolDescription,
		"mcp_tool_config":      tool.McpToolConfig.AsMap(),
		"mapping_id":           tool.MappingId,
		"policy_ids":           tool.PolicyIds,
		"owner_id":             tool.OwnerId,
	}
}
