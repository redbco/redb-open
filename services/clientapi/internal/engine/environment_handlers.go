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

// EnvironmentHandlers contains the environment endpoint handlers
type EnvironmentHandlers struct {
	engine *Engine
}

// NewEnvironmentHandlers creates a new instance of EnvironmentHandlers
func NewEnvironmentHandlers(engine *Engine) *EnvironmentHandlers {
	return &EnvironmentHandlers{
		engine: engine,
	}
}

// ListEnvironments handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/environments
func (eh *EnvironmentHandlers) ListEnvironments(w http.ResponseWriter, r *http.Request) {
	eh.engine.TrackOperation()
	defer eh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		eh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if eh.engine.logger != nil {
		eh.engine.logger.Infof("List environments request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListEnvironmentsRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := eh.engine.environmentClient.ListEnvironments(ctx, grpcReq)
	if err != nil {
		eh.handleGRPCError(w, err, "Failed to list environments")
		return
	}

	// Convert gRPC response to REST response
	environments := make([]Environment, len(grpcResp.Environments))
	for i, env := range grpcResp.Environments {
		environments[i] = Environment{
			EnvironmentID:           env.EnvironmentId,
			EnvironmentName:         env.EnvironmentName,
			EnvironmentDescription:  env.EnvironmentDescription,
			EnvironmentIsProduction: env.EnvironmentIsProduction,
			EnvironmentCriticality:  env.EnvironmentCriticality,
			EnvironmentPriority:     env.EnvironmentPriority,
			InstanceCount:           env.InstanceCount,
			DatabaseCount:           env.DatabaseCount,
			Status:                  convertStatus(env.Status),
			OwnerID:                 env.OwnerId,
		}
	}

	response := ListEnvironmentsResponse{
		Environments: environments,
	}

	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Successfully listed %d environments for workspace: %s", len(environments), workspaceName)
	}

	eh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowEnvironment handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/environments/{environment_name}
func (eh *EnvironmentHandlers) ShowEnvironment(w http.ResponseWriter, r *http.Request) {
	eh.engine.TrackOperation()
	defer eh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	environmentName := vars["environment_name"]

	if tenantURL == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	if environmentName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "environment_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		eh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Show environment request for environment: %s, workspace: %s, tenant: %s, user: %s", environmentName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowEnvironmentRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		EnvironmentName: environmentName,
	}

	grpcResp, err := eh.engine.environmentClient.ShowEnvironment(ctx, grpcReq)
	if err != nil {
		eh.handleGRPCError(w, err, "Failed to show environment")
		return
	}

	// Convert gRPC response to REST response
	environment := Environment{
		EnvironmentID:           grpcResp.Environment.EnvironmentId,
		EnvironmentName:         grpcResp.Environment.EnvironmentName,
		EnvironmentDescription:  grpcResp.Environment.EnvironmentDescription,
		EnvironmentIsProduction: grpcResp.Environment.EnvironmentIsProduction,
		EnvironmentCriticality:  grpcResp.Environment.EnvironmentCriticality,
		EnvironmentPriority:     grpcResp.Environment.EnvironmentPriority,
		InstanceCount:           grpcResp.Environment.InstanceCount,
		DatabaseCount:           grpcResp.Environment.DatabaseCount,
		Status:                  convertStatus(grpcResp.Environment.Status),
		OwnerID:                 grpcResp.Environment.OwnerId,
	}

	response := ShowEnvironmentResponse{
		Environment: environment,
	}

	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Successfully showed environment: %s for workspace: %s", environmentName, workspaceName)
	}

	eh.writeJSONResponse(w, http.StatusOK, response)
}

// AddEnvironment handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/environments
func (eh *EnvironmentHandlers) AddEnvironment(w http.ResponseWriter, r *http.Request) {
	eh.engine.TrackOperation()
	defer eh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		eh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddEnvironmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if eh.engine.logger != nil {
			eh.engine.logger.Errorf("Failed to parse add environment request body: %v", err)
		}
		eh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.EnvironmentName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "environment_name is required", "")
		return
	}

	// Note: owner_id is always set from authenticated user profile, not from request body
	// This prevents users from creating resources that belong to someone else

	// Log request
	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Add environment request: %s for workspace: %s, tenant: %s, user: %s", req.EnvironmentName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request - owner_id is always the authenticated user
	grpcReq := &corev1.AddEnvironmentRequest{
		TenantId:               profile.TenantId,
		WorkspaceName:          workspaceName,
		EnvironmentName:        req.EnvironmentName,
		EnvironmentDescription: &req.EnvironmentDescription,
		OwnerId:                profile.UserId, // Always use authenticated user's ID
	}

	// Set optional fields
	if req.EnvironmentIsProduction != nil {
		grpcReq.EnvironmentIsProduction = req.EnvironmentIsProduction
	}
	if req.EnvironmentCriticality != nil {
		grpcReq.EnvironmentCriticality = req.EnvironmentCriticality
	}
	if req.EnvironmentPriority != nil {
		grpcReq.EnvironmentPriority = req.EnvironmentPriority
	}

	grpcResp, err := eh.engine.environmentClient.AddEnvironment(ctx, grpcReq)
	if err != nil {
		eh.handleGRPCError(w, err, "Failed to add environment")
		return
	}

	// Convert gRPC response to REST response
	environment := Environment{
		EnvironmentID:           grpcResp.Environment.EnvironmentId,
		EnvironmentName:         grpcResp.Environment.EnvironmentName,
		EnvironmentDescription:  grpcResp.Environment.EnvironmentDescription,
		EnvironmentIsProduction: grpcResp.Environment.EnvironmentIsProduction,
		EnvironmentCriticality:  grpcResp.Environment.EnvironmentCriticality,
		EnvironmentPriority:     grpcResp.Environment.EnvironmentPriority,
		InstanceCount:           grpcResp.Environment.InstanceCount,
		DatabaseCount:           grpcResp.Environment.DatabaseCount,
		Status:                  convertStatus(grpcResp.Environment.Status),
		OwnerID:                 grpcResp.Environment.OwnerId,
	}

	response := AddEnvironmentResponse{
		Message:     grpcResp.Message,
		Success:     grpcResp.Success,
		Environment: environment,
		Status:      convertStatus(grpcResp.Status),
	}

	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Successfully added environment: %s for workspace: %s", req.EnvironmentName, workspaceName)
	}

	eh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyEnvironment handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/environments/{environment_name}
func (eh *EnvironmentHandlers) ModifyEnvironment(w http.ResponseWriter, r *http.Request) {
	eh.engine.TrackOperation()
	defer eh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	environmentName := vars["environment_name"]

	if tenantURL == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	if environmentName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "environment_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		eh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyEnvironmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if eh.engine.logger != nil {
			eh.engine.logger.Errorf("Failed to parse modify environment request body: %v", err)
		}
		eh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Modify environment request for environment: %s, workspace: %s, tenant: %s, user: %s", environmentName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifyEnvironmentRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		EnvironmentName: environmentName,
	}

	// Set optional fields
	if req.EnvironmentNameNew != "" {
		grpcReq.EnvironmentNameNew = &req.EnvironmentNameNew
	}
	if req.EnvironmentDescription != "" {
		grpcReq.EnvironmentDescription = &req.EnvironmentDescription
	}
	if req.EnvironmentIsProduction != nil {
		grpcReq.EnvironmentIsProduction = req.EnvironmentIsProduction
	}
	if req.EnvironmentCriticality != nil {
		grpcReq.EnvironmentCriticality = req.EnvironmentCriticality
	}
	if req.EnvironmentPriority != nil {
		grpcReq.EnvironmentPriority = req.EnvironmentPriority
	}

	grpcResp, err := eh.engine.environmentClient.ModifyEnvironment(ctx, grpcReq)
	if err != nil {
		eh.handleGRPCError(w, err, "Failed to modify environment")
		return
	}

	// Convert gRPC response to REST response
	environment := Environment{
		EnvironmentID:           grpcResp.Environment.EnvironmentId,
		EnvironmentName:         grpcResp.Environment.EnvironmentName,
		EnvironmentDescription:  grpcResp.Environment.EnvironmentDescription,
		EnvironmentIsProduction: grpcResp.Environment.EnvironmentIsProduction,
		EnvironmentCriticality:  grpcResp.Environment.EnvironmentCriticality,
		EnvironmentPriority:     grpcResp.Environment.EnvironmentPriority,
		InstanceCount:           grpcResp.Environment.InstanceCount,
		DatabaseCount:           grpcResp.Environment.DatabaseCount,
		Status:                  convertStatus(grpcResp.Environment.Status),
		OwnerID:                 grpcResp.Environment.OwnerId,
	}

	response := ModifyEnvironmentResponse{
		Message:     grpcResp.Message,
		Success:     grpcResp.Success,
		Environment: environment,
		Status:      convertStatus(grpcResp.Status),
	}

	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Successfully modified environment: %s for workspace: %s", environmentName, workspaceName)
	}

	eh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteEnvironment handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/environments/{environment_name}
func (eh *EnvironmentHandlers) DeleteEnvironment(w http.ResponseWriter, r *http.Request) {
	eh.engine.TrackOperation()
	defer eh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	environmentName := vars["environment_name"]

	if tenantURL == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	if environmentName == "" {
		eh.writeErrorResponse(w, http.StatusBadRequest, "environment_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		eh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Delete environment request for environment: %s, workspace: %s, tenant: %s, user: %s", environmentName, workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteEnvironmentRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		EnvironmentName: environmentName,
	}

	grpcResp, err := eh.engine.environmentClient.DeleteEnvironment(ctx, grpcReq)
	if err != nil {
		eh.handleGRPCError(w, err, "Failed to delete environment")
		return
	}

	response := DeleteEnvironmentResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if eh.engine.logger != nil {
		eh.engine.logger.Infof("Successfully deleted environment: %s for workspace: %s", environmentName, workspaceName)
	}

	eh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (eh *EnvironmentHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		eh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	var httpStatus int
	switch grpcStatus.Code() {
	case codes.NotFound:
		httpStatus = http.StatusNotFound
	case codes.InvalidArgument:
		httpStatus = http.StatusBadRequest
	case codes.AlreadyExists:
		httpStatus = http.StatusConflict
	case codes.PermissionDenied:
		httpStatus = http.StatusForbidden
	case codes.Unauthenticated:
		httpStatus = http.StatusUnauthorized
	default:
		httpStatus = http.StatusInternalServerError
	}

	message := grpcStatus.Message()
	if message == "" {
		message = defaultMessage
	}

	eh.writeErrorResponse(w, httpStatus, message, "")
}

func (eh *EnvironmentHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if eh.engine.logger != nil {
			eh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (eh *EnvironmentHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusError,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if eh.engine.logger != nil {
			eh.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}
