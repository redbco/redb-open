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

// UserHandlers contains the user endpoint handlers
type UserHandlers struct {
	engine *Engine
}

// NewUserHandlers creates a new instance of UserHandlers
func NewUserHandlers(engine *Engine) *UserHandlers {
	return &UserHandlers{
		engine: engine,
	}
}

// ListUsers handles GET /{tenant_url}/api/v1/users
func (uh *UserHandlers) ListUsers(w http.ResponseWriter, r *http.Request) {
	uh.engine.TrackOperation()
	defer uh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		uh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if uh.engine.logger != nil {
		uh.engine.logger.Infof("List users request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListUsersRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := uh.engine.userClient.ListUsers(ctx, grpcReq)
	if err != nil {
		uh.handleGRPCError(w, err, "Failed to list users")
		return
	}

	// Convert gRPC response to REST response
	users := make([]User, len(grpcResp.Users))
	for i, u := range grpcResp.Users {
		users[i] = User{
			TenantID:    u.TenantId,
			UserID:      u.UserId,
			UserName:    u.UserName,
			UserEmail:   u.UserEmail,
			UserEnabled: u.UserEnabled,
		}
	}

	response := ListUsersResponse{
		Users: users,
	}

	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Successfully listed %d users for tenant: %s", len(users), profile.TenantId)
	}

	uh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowUser handles GET /{tenant_url}/api/v1/users/{user_id}
func (uh *UserHandlers) ShowUser(w http.ResponseWriter, r *http.Request) {
	uh.engine.TrackOperation()
	defer uh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	userID := vars["user_id"]

	if tenantURL == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if userID == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "user_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		uh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Show user request for user: %s, tenant: %s, user: %s", userID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowUserRequest{
		TenantId: profile.TenantId,
		UserId:   userID,
	}

	grpcResp, err := uh.engine.userClient.ShowUser(ctx, grpcReq)
	if err != nil {
		uh.handleGRPCError(w, err, "Failed to show user")
		return
	}

	// Convert gRPC response to REST response
	user := User{
		TenantID:    grpcResp.User.TenantId,
		UserID:      grpcResp.User.UserId,
		UserName:    grpcResp.User.UserName,
		UserEmail:   grpcResp.User.UserEmail,
		UserEnabled: grpcResp.User.UserEnabled,
	}

	response := ShowUserResponse{
		User: user,
	}

	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Successfully showed user: %s for tenant: %s", userID, profile.TenantId)
	}

	uh.writeJSONResponse(w, http.StatusOK, response)
}

// AddUser handles POST /{tenant_url}/api/v1/users
func (uh *UserHandlers) AddUser(w http.ResponseWriter, r *http.Request) {
	uh.engine.TrackOperation()
	defer uh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		uh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if uh.engine.logger != nil {
			uh.engine.logger.Errorf("Failed to parse add user request body: %v", err)
		}
		uh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.UserName == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "user_name is required", "")
		return
	}
	if req.UserEmail == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "user_email is required", "")
		return
	}
	if req.UserPassword == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "user_password is required", "")
		return
	}

	// Log request
	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Add user request for user: %s, tenant: %s, user: %s", req.UserEmail, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddUserRequest{
		TenantId:     profile.TenantId,
		UserName:     req.UserName,
		UserEmail:    req.UserEmail,
		UserPassword: req.UserPassword,
	}

	// Set optional fields if provided
	if req.UserEnabled != nil {
		grpcReq.UserEnabled = req.UserEnabled
	}

	grpcResp, err := uh.engine.userClient.AddUser(ctx, grpcReq)
	if err != nil {
		uh.handleGRPCError(w, err, "Failed to add user")
		return
	}

	// Convert gRPC response to REST response
	user := User{
		TenantID:    grpcResp.User.TenantId,
		UserID:      grpcResp.User.UserId,
		UserName:    grpcResp.User.UserName,
		UserEmail:   grpcResp.User.UserEmail,
		UserEnabled: grpcResp.User.UserEnabled,
	}

	response := AddUserResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		User:    user,
		Status:  convertStatus(grpcResp.Status),
	}

	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Successfully added user: %s for tenant: %s", req.UserEmail, profile.TenantId)
	}

	uh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyUser handles PUT /{tenant_url}/api/v1/users/{user_id}
func (uh *UserHandlers) ModifyUser(w http.ResponseWriter, r *http.Request) {
	uh.engine.TrackOperation()
	defer uh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	userID := vars["user_id"]

	if tenantURL == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if userID == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "user_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		uh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if uh.engine.logger != nil {
			uh.engine.logger.Errorf("Failed to parse modify user request body: %v", err)
		}
		uh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Modify user request for user: %s, tenant: %s, user: %s", userID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyUserRequest{
		TenantId: profile.TenantId,
		UserId:   userID,
	}

	// Set optional fields if provided
	if req.UserName != "" {
		grpcReq.UserName = &req.UserName
	}
	if req.UserEmail != "" {
		grpcReq.UserEmail = &req.UserEmail
	}
	if req.UserPassword != "" {
		grpcReq.UserPassword = &req.UserPassword
	}
	if req.UserEnabled != nil {
		grpcReq.UserEnabled = req.UserEnabled
	}

	grpcResp, err := uh.engine.userClient.ModifyUser(ctx, grpcReq)
	if err != nil {
		uh.handleGRPCError(w, err, "Failed to modify user")
		return
	}

	// Convert gRPC response to REST response
	user := User{
		TenantID:    grpcResp.User.TenantId,
		UserID:      grpcResp.User.UserId,
		UserName:    grpcResp.User.UserName,
		UserEmail:   grpcResp.User.UserEmail,
		UserEnabled: grpcResp.User.UserEnabled,
	}

	response := ModifyUserResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		User:    user,
		Status:  convertStatus(grpcResp.Status),
	}

	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Successfully modified user: %s for tenant: %s", userID, profile.TenantId)
	}

	uh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteUser handles DELETE /{tenant_url}/api/v1/users/{user_id}
func (uh *UserHandlers) DeleteUser(w http.ResponseWriter, r *http.Request) {
	uh.engine.TrackOperation()
	defer uh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	userID := vars["user_id"]

	if tenantURL == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if userID == "" {
		uh.writeErrorResponse(w, http.StatusBadRequest, "user_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		uh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Delete user request for user: %s, tenant: %s, user: %s", userID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteUserRequest{
		TenantId: profile.TenantId,
		UserId:   userID,
	}

	grpcResp, err := uh.engine.userClient.DeleteUser(ctx, grpcReq)
	if err != nil {
		uh.handleGRPCError(w, err, "Failed to delete user")
		return
	}

	// Convert gRPC response to REST response
	response := DeleteUserResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if uh.engine.logger != nil {
		uh.engine.logger.Infof("Successfully deleted user: %s for tenant: %s", userID, profile.TenantId)
	}

	uh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

// handleGRPCError handles gRPC errors and converts them to HTTP responses
func (uh *UserHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if uh.engine.logger != nil {
		uh.engine.logger.Errorf("gRPC error: %v", err)
	}

	st, ok := status.FromError(err)
	if !ok {
		uh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		uh.writeErrorResponse(w, http.StatusNotFound, "Resource not found", st.Message())
	case codes.AlreadyExists:
		uh.writeErrorResponse(w, http.StatusConflict, "Resource already exists", st.Message())
	case codes.InvalidArgument:
		uh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request", st.Message())
	case codes.PermissionDenied:
		uh.writeErrorResponse(w, http.StatusForbidden, "Permission denied", st.Message())
	case codes.Unauthenticated:
		uh.writeErrorResponse(w, http.StatusUnauthorized, "Authentication required", st.Message())
	case codes.Unavailable:
		uh.writeErrorResponse(w, http.StatusServiceUnavailable, "Service unavailable", st.Message())
	case codes.DeadlineExceeded:
		uh.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", st.Message())
	default:
		uh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, st.Message())
	}
}

// writeJSONResponse writes a JSON response
func (uh *UserHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if uh.engine.logger != nil {
			uh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

// writeErrorResponse writes an error response
func (uh *UserHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	// Log error responses for monitoring and debugging
	if uh.engine.logger != nil {
		if statusCode >= 500 {
			// Log 5xx errors as errors
			uh.engine.logger.Errorf("HTTP %d - %s: %s", statusCode, message, error)
		} else if statusCode >= 400 {
			// Log 4xx errors as warnings
			uh.engine.logger.Warnf("HTTP %d - %s: %s", statusCode, message, error)
		}
	}

	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusError,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if uh.engine.logger != nil {
			uh.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}
