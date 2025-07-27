package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WorkspaceHandlers contains the workspace endpoint handlers
type WorkspaceHandlers struct {
	engine *Engine
}

// NewWorkspaceHandlers creates a new instance of WorkspaceHandlers
func NewWorkspaceHandlers(engine *Engine) *WorkspaceHandlers {
	return &WorkspaceHandlers{
		engine: engine,
	}
}

// ListWorkspaces handles GET /{tenant_url}/api/v1/workspaces
func (wh *WorkspaceHandlers) ListWorkspaces(w http.ResponseWriter, r *http.Request) {
	wh.engine.TrackOperation()
	defer wh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		wh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if wh.engine.logger != nil {
		wh.engine.logger.Infof("List workspaces request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListWorkspacesRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := wh.engine.workspaceClient.ListWorkspaces(ctx, grpcReq)
	if err != nil {
		wh.handleGRPCError(w, err, "Failed to list workspaces")
		return
	}

	// Convert gRPC response to REST response
	workspaces := make([]Workspace, len(grpcResp.Workspaces))
	for i, ws := range grpcResp.Workspaces {
		workspaces[i] = Workspace{
			WorkspaceID:          ws.WorkspaceId,
			WorkspaceName:        ws.WorkspaceName,
			WorkspaceDescription: ws.WorkspaceDescription,
			InstanceCount:        ws.InstanceCount,
			DatabaseCount:        ws.DatabaseCount,
			RepoCount:            ws.RepoCount,
			MappingCount:         ws.MappingCount,
			RelationshipCount:    ws.RelationshipCount,
			OwnerID:              ws.OwnerId,
		}
	}

	response := ListWorkspacesResponse{
		Workspaces: workspaces,
	}

	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Successfully listed %d workspaces for tenant: %s", len(workspaces), profile.TenantId)
	}

	wh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowWorkspace handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}
func (wh *WorkspaceHandlers) ShowWorkspace(w http.ResponseWriter, r *http.Request) {
	wh.engine.TrackOperation()
	defer wh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		wh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Show workspace request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowWorkspaceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := wh.engine.workspaceClient.ShowWorkspace(ctx, grpcReq)
	if err != nil {
		wh.handleGRPCError(w, err, "Failed to show workspace")
		return
	}

	// Convert gRPC response to REST response
	workspace := Workspace{
		WorkspaceID:          grpcResp.Workspace.WorkspaceId,
		WorkspaceName:        grpcResp.Workspace.WorkspaceName,
		WorkspaceDescription: grpcResp.Workspace.WorkspaceDescription,
		InstanceCount:        grpcResp.Workspace.InstanceCount,
		DatabaseCount:        grpcResp.Workspace.DatabaseCount,
		RepoCount:            grpcResp.Workspace.RepoCount,
		MappingCount:         grpcResp.Workspace.MappingCount,
		RelationshipCount:    grpcResp.Workspace.RelationshipCount,
		OwnerID:              grpcResp.Workspace.OwnerId,
	}

	response := ShowWorkspaceResponse{
		Workspace: workspace,
	}

	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Successfully showed workspace: %s for tenant: %s", workspaceName, profile.TenantId)
	}

	wh.writeJSONResponse(w, http.StatusOK, response)
}

// AddWorkspace handles POST /{tenant_url}/api/v1/workspaces
func (wh *WorkspaceHandlers) AddWorkspace(w http.ResponseWriter, r *http.Request) {
	wh.engine.TrackOperation()
	defer wh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		wh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddWorkspaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if wh.engine.logger != nil {
			wh.engine.logger.Errorf("Failed to parse add workspace request body: %v", err)
		}
		wh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.WorkspaceName == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Log request
	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Add workspace request for workspace: %s, tenant: %s, user: %s", req.WorkspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddWorkspaceRequest{
		TenantId:             profile.TenantId,
		WorkspaceName:        req.WorkspaceName,
		WorkspaceDescription: &req.WorkspaceDescription,
		OwnerId:              profile.UserId,
	}

	grpcResp, err := wh.engine.workspaceClient.AddWorkspace(ctx, grpcReq)
	if err != nil {
		wh.handleGRPCError(w, err, "Failed to add workspace")
		return
	}

	// Convert gRPC response to REST response
	workspace := Workspace{
		WorkspaceID:          grpcResp.Workspace.WorkspaceId,
		WorkspaceName:        grpcResp.Workspace.WorkspaceName,
		WorkspaceDescription: grpcResp.Workspace.WorkspaceDescription,
		InstanceCount:        grpcResp.Workspace.InstanceCount,
		DatabaseCount:        grpcResp.Workspace.DatabaseCount,
		RepoCount:            grpcResp.Workspace.RepoCount,
		MappingCount:         grpcResp.Workspace.MappingCount,
		RelationshipCount:    grpcResp.Workspace.RelationshipCount,
		OwnerID:              grpcResp.Workspace.OwnerId,
	}

	response := AddWorkspaceResponse{
		Message:   grpcResp.Message,
		Success:   grpcResp.Success,
		Workspace: workspace,
		Status:    convertStatus(grpcResp.Status),
	}

	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Successfully added workspace: %s for tenant: %s", req.WorkspaceName, profile.TenantId)
	}

	wh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyWorkspace handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}
func (wh *WorkspaceHandlers) ModifyWorkspace(w http.ResponseWriter, r *http.Request) {
	wh.engine.TrackOperation()
	defer wh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		wh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyWorkspaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if wh.engine.logger != nil {
			wh.engine.logger.Errorf("Failed to parse modify workspace request body: %v", err)
		}
		wh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Modify workspace request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyWorkspaceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	// Set optional fields if provided
	if req.WorkspaceNameNew != "" {
		grpcReq.WorkspaceNameNew = &req.WorkspaceNameNew
	}
	if req.WorkspaceDescription != "" {
		grpcReq.WorkspaceDescription = &req.WorkspaceDescription
	}

	grpcResp, err := wh.engine.workspaceClient.ModifyWorkspace(ctx, grpcReq)
	if err != nil {
		wh.handleGRPCError(w, err, "Failed to modify workspace")
		return
	}

	// Convert gRPC response to REST response
	workspace := Workspace{
		WorkspaceID:          grpcResp.Workspace.WorkspaceId,
		WorkspaceName:        grpcResp.Workspace.WorkspaceName,
		WorkspaceDescription: grpcResp.Workspace.WorkspaceDescription,
		InstanceCount:        grpcResp.Workspace.InstanceCount,
		DatabaseCount:        grpcResp.Workspace.DatabaseCount,
		RepoCount:            grpcResp.Workspace.RepoCount,
		MappingCount:         grpcResp.Workspace.MappingCount,
		RelationshipCount:    grpcResp.Workspace.RelationshipCount,
		OwnerID:              grpcResp.Workspace.OwnerId,
	}

	response := ModifyWorkspaceResponse{
		Message:   grpcResp.Message,
		Success:   grpcResp.Success,
		Workspace: workspace,
		Status:    convertStatus(grpcResp.Status),
	}

	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Successfully modified workspace: %s for tenant: %s", workspaceName, profile.TenantId)
	}

	wh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteWorkspace handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}
func (wh *WorkspaceHandlers) DeleteWorkspace(w http.ResponseWriter, r *http.Request) {
	wh.engine.TrackOperation()
	defer wh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		wh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		wh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Delete workspace request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteWorkspaceRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := wh.engine.workspaceClient.DeleteWorkspace(ctx, grpcReq)
	if err != nil {
		wh.handleGRPCError(w, err, "Failed to delete workspace")
		return
	}

	// Convert gRPC response to REST response
	response := DeleteWorkspaceResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if wh.engine.logger != nil {
		wh.engine.logger.Infof("Successfully deleted workspace: %s for tenant: %s", workspaceName, profile.TenantId)
	}

	wh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

// handleGRPCError handles gRPC errors and converts them to HTTP responses
func (wh *WorkspaceHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if wh.engine.logger != nil {
		wh.engine.logger.Errorf("gRPC error: %v", err)
	}

	st, ok := status.FromError(err)
	if !ok {
		wh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		wh.writeErrorResponse(w, http.StatusNotFound, "Resource not found", st.Message())
	case codes.AlreadyExists:
		wh.writeErrorResponse(w, http.StatusConflict, "Resource already exists", st.Message())
	case codes.InvalidArgument:
		wh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request", st.Message())
	case codes.PermissionDenied:
		wh.writeErrorResponse(w, http.StatusForbidden, "Permission denied", st.Message())
	case codes.Unauthenticated:
		wh.writeErrorResponse(w, http.StatusUnauthorized, "Authentication required", st.Message())
	case codes.Unavailable:
		wh.writeErrorResponse(w, http.StatusServiceUnavailable, "Service unavailable", st.Message())
	case codes.DeadlineExceeded:
		wh.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", st.Message())
	default:
		wh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, st.Message())
	}
}

// writeJSONResponse writes a JSON response
func (wh *WorkspaceHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if wh.engine.logger != nil {
			wh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

// writeErrorResponse writes an error response
func (wh *WorkspaceHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusError,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if wh.engine.logger != nil {
			wh.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}
