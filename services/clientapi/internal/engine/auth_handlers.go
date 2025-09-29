package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AuthHandlers contains the authentication endpoint handlers
type AuthHandlers struct {
	engine *Engine
}

// NewAuthHandlers creates a new instance of AuthHandlers
func NewAuthHandlers(engine *Engine) *AuthHandlers {
	return &AuthHandlers{
		engine: engine,
	}
}

// Login handles POST /{tenant_url}/api/v1/auth/login
func (ah *AuthHandlers) Login(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Log incoming request details
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Login request received for tenant: %s", tenantURL)
		ah.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
		ah.engine.logger.Debugf("User-Agent: %s", r.Header.Get("User-Agent"))
	}

	// Parse request body
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Failed to parse login request body: %v", err)
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.Username == "" || req.Password == "" {
		if ah.engine.logger != nil {
			ah.engine.logger.Warnf("Login request missing required fields - username: %t, password: %t",
				req.Username != "", req.Password != "")
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Username and password are required", "")
		return
	}

	// Log authentication attempt (without password)
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Authentication attempt for user: %s, tenant: %s", req.Username, tenantURL)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if security client is available
	if ah.engine.GetSecurityClient() == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security client is nil - gRPC connection may have failed during startup")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Authentication service unavailable", "")
		return
	}

	// Extract session information from request headers if not provided in body
	if req.UserAgent == "" {
		req.UserAgent = r.Header.Get("User-Agent")
	}
	if req.IPAddress == "" {
		req.IPAddress = r.RemoteAddr
	}

	// Call security service gRPC
	grpcReq := &securityv1.LoginRequest{
		Username:        req.Username,
		Password:        req.Password,
		TenantUrl:       tenantURL,
		ExpiryTimeHours: &req.ExpiryTimeHours,
		SessionName:     &req.SessionName,
		UserAgent:       &req.UserAgent,
		IpAddress:       &req.IPAddress,
		Platform:        &req.Platform,
		Browser:         &req.Browser,
		OperatingSystem: &req.OperatingSystem,
		DeviceType:      &req.DeviceType,
		Location:        &req.Location,
	}

	// Log gRPC call attempt
	if ah.engine.logger != nil {
		ah.engine.logger.Debugf("Making gRPC Login call to security service")
		ah.engine.logger.Debugf("gRPC request: username=%s, tenant_url=%s", grpcReq.Username, grpcReq.TenantUrl)
	}

	grpcResp, err := ah.engine.GetSecurityClient().Login(ctx, grpcReq)
	if err != nil {
		// Don't log as error initially - let handleGRPCError determine appropriate level
		ah.handleGRPCError(w, err, "Login failed")
		return
	}

	// Log successful gRPC response
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("gRPC Login call successful for user: %s", req.Username)
		ah.engine.logger.Debugf("gRPC response status: %v", grpcResp.Status)
	}

	// Convert gRPC response to REST response
	response := LoginResponse{
		Profile: Profile{
			TenantID: grpcResp.Profile.TenantId,
			UserID:   grpcResp.Profile.UserId,
			Username: grpcResp.Profile.Username,
			Email:    grpcResp.Profile.Email,
			Name:     grpcResp.Profile.Name,
		},
		AccessToken:  grpcResp.AccessToken,
		RefreshToken: grpcResp.RefreshToken,
		SessionID:    grpcResp.SessionId,
		Status:       convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Login successful for user: %s@%s", req.Username, tenantURL)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// Logout handles POST /{tenant_url}/api/v1/auth/logout
func (ah *AuthHandlers) Logout(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Parse request body
	var req LogoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.RefreshToken == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "Refresh token is required", "")
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call security service gRPC
	grpcReq := &securityv1.LogoutRequest{
		RefreshToken: req.RefreshToken,
	}

	grpcResp, err := ah.engine.GetSecurityClient().Logout(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Logout failed")
		return
	}

	// Convert gRPC response to REST response
	response := LogoutResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Status == commonv1.Status_STATUS_SUCCESS,
		Status:  convertStatus(grpcResp.Status),
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// GetProfile handles GET /{tenant_url}/api/v1/auth/profile
func (ah *AuthHandlers) GetProfile(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Get profile from context (set by authentication middleware)
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Convert gRPC profile to REST response
	response := GetProfileResponse{
		Profile: Profile{
			TenantID: profile.TenantId,
			UserID:   profile.UserId,
			Username: profile.Username,
			Email:    profile.Email,
			Name:     profile.Name,
		},
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// ChangePassword handles POST /{tenant_url}/api/v1/auth/change-password
func (ah *AuthHandlers) ChangePassword(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Log incoming request details
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Change password request received")
		ah.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
		ah.engine.logger.Debugf("User-Agent: %s", r.Header.Get("User-Agent"))
	}

	// Parse request body
	var req ChangePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Failed to parse change password request body: %v", err)
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate required fields
	if req.OldPassword == "" || req.NewPassword == "" {
		if ah.engine.logger != nil {
			ah.engine.logger.Warnf("Change password request missing required fields - old_password: %t, new_password: %t",
				req.OldPassword != "", req.NewPassword != "")
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Old password and new password are required", "")
		return
	}

	// Get profile from context (set by authentication middleware)
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Profile not found in context for change password request")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log change password attempt (without passwords)
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Change password attempt for user: %s, tenant: %s", profile.Username, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if security client is available
	if ah.engine.GetSecurityClient() == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security client is nil - gRPC connection may have failed during startup")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Authentication service unavailable", "")
		return
	}

	// Call security service gRPC
	grpcReq := &securityv1.ChangePasswordRequest{
		TenantId:    profile.TenantId,
		UserId:      profile.UserId,
		OldPassword: req.OldPassword,
		NewPassword: req.NewPassword,
	}

	// Log gRPC call attempt
	if ah.engine.logger != nil {
		ah.engine.logger.Debugf("Making gRPC ChangePassword call to security service")
		ah.engine.logger.Debugf("gRPC request: tenant_id=%s, user_id=%s", grpcReq.TenantId, grpcReq.UserId)
	}

	grpcResp, err := ah.engine.GetSecurityClient().ChangePassword(ctx, grpcReq)
	if err != nil {
		// Don't log as error initially - let handleGRPCError determine appropriate level
		ah.handleGRPCError(w, err, "Change password failed")
		return
	}

	// Log successful gRPC response
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("gRPC ChangePassword call successful for user: %s", profile.Username)
		ah.engine.logger.Debugf("gRPC response status: %v", grpcResp.Status)
	}

	// Convert gRPC response to REST response
	response := ChangePasswordResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Status == commonv1.Status_STATUS_SUCCESS,
		Status:  convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Change password successful for user: %s@%s", profile.Username, profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// ListSessions handles GET /{tenant_url}/api/v1/auth/sessions
func (ah *AuthHandlers) ListSessions(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Get profile from context (set by authentication middleware)
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Profile not found in context for list sessions request")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log incoming request details
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("List sessions request received for user: %s", profile.Username)
		ah.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if security client is available
	if ah.engine.GetSecurityClient() == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security client is nil - gRPC connection may have failed during startup")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Authentication service unavailable", "")
		return
	}

	// Call security service gRPC
	grpcReq := &securityv1.ListSessionsRequest{
		TenantId: profile.TenantId,
		UserId:   profile.UserId,
	}

	// Log gRPC call attempt
	if ah.engine.logger != nil {
		ah.engine.logger.Debugf("Making gRPC ListSessions call to security service")
		ah.engine.logger.Debugf("gRPC request: tenant_id=%s, user_id=%s", grpcReq.TenantId, grpcReq.UserId)
	}

	grpcResp, err := ah.engine.GetSecurityClient().ListSessions(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "List sessions failed")
		return
	}

	// Log successful gRPC response
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("gRPC ListSessions call successful for user: %s", profile.Username)
		ah.engine.logger.Debugf("gRPC response status: %v", grpcResp.Status)
	}

	// Convert gRPC response to REST response
	sessions := make([]SessionInfo, len(grpcResp.Sessions))
	for i, session := range grpcResp.Sessions {
		sessions[i] = SessionInfo{
			SessionID:       session.SessionId,
			SessionName:     session.SessionName,
			UserAgent:       session.UserAgent,
			IPAddress:       session.IpAddress,
			Platform:        session.Platform,
			Browser:         session.Browser,
			OperatingSystem: session.OperatingSystem,
			DeviceType:      session.DeviceType,
			Location:        session.Location,
			LastActivity:    session.LastActivity,
			Created:         session.Created,
			Expires:         session.Expires,
			IsCurrent:       session.IsCurrent,
		}
	}

	response := ListSessionsResponse{
		Sessions: sessions,
		Status:   convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("List sessions successful for user: %s@%s", profile.Username, profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// LogoutSession handles POST /{tenant_url}/api/v1/auth/sessions/{session_id}/logout
func (ah *AuthHandlers) LogoutSession(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract session_id from path
	vars := mux.Vars(r)
	sessionID := vars["session_id"]
	if sessionID == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "session_id is required", "")
		return
	}

	// Get profile from context (set by authentication middleware)
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Profile not found in context for logout session request")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log incoming request details
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Logout session request received for user: %s, session: %s", profile.Username, sessionID)
		ah.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if security client is available
	if ah.engine.GetSecurityClient() == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security client is nil - gRPC connection may have failed during startup")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Authentication service unavailable", "")
		return
	}

	// Call security service gRPC
	grpcReq := &securityv1.LogoutSessionRequest{
		TenantId:  profile.TenantId,
		UserId:    profile.UserId,
		SessionId: sessionID,
	}

	// Log gRPC call attempt
	if ah.engine.logger != nil {
		ah.engine.logger.Debugf("Making gRPC LogoutSession call to security service")
		ah.engine.logger.Debugf("gRPC request: tenant_id=%s, user_id=%s, session_id=%s", grpcReq.TenantId, grpcReq.UserId, grpcReq.SessionId)
	}

	grpcResp, err := ah.engine.GetSecurityClient().LogoutSession(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Logout session failed")
		return
	}

	// Log successful gRPC response
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("gRPC LogoutSession call successful for user: %s, session: %s", profile.Username, sessionID)
		ah.engine.logger.Debugf("gRPC response status: %v", grpcResp.Status)
	}

	// Convert gRPC response to REST response
	response := LogoutSessionResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Status == commonv1.Status_STATUS_SUCCESS,
		Status:  convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Logout session successful for user: %s@%s, session: %s", profile.Username, profile.TenantId, sessionID)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// LogoutAllSessions handles POST /{tenant_url}/api/v1/auth/sessions/logout-all
func (ah *AuthHandlers) LogoutAllSessions(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Get profile from context (set by authentication middleware)
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Profile not found in context for logout all sessions request")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body (optional)
	var req LogoutAllSessionsRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if ah.engine.logger != nil {
				ah.engine.logger.Errorf("Failed to parse logout all sessions request body: %v", err)
			}
			ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
			return
		}
	}

	// Log incoming request details
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Logout all sessions request received for user: %s, exclude_current: %t", profile.Username, req.ExcludeCurrent)
		ah.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if security client is available
	if ah.engine.GetSecurityClient() == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security client is nil - gRPC connection may have failed during startup")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Authentication service unavailable", "")
		return
	}

	// Call security service gRPC
	grpcReq := &securityv1.LogoutAllSessionsRequest{
		TenantId:       profile.TenantId,
		UserId:         profile.UserId,
		ExcludeCurrent: &req.ExcludeCurrent,
	}

	// Log gRPC call attempt
	if ah.engine.logger != nil {
		ah.engine.logger.Debugf("Making gRPC LogoutAllSessions call to security service")
		ah.engine.logger.Debugf("gRPC request: tenant_id=%s, user_id=%s, exclude_current=%t", grpcReq.TenantId, grpcReq.UserId, *grpcReq.ExcludeCurrent)
	}

	grpcResp, err := ah.engine.GetSecurityClient().LogoutAllSessions(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Logout all sessions failed")
		return
	}

	// Log successful gRPC response
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("gRPC LogoutAllSessions call successful for user: %s", profile.Username)
		ah.engine.logger.Debugf("gRPC response status: %v", grpcResp.Status)
	}

	// Convert gRPC response to REST response
	response := LogoutAllSessionsResponse{
		SessionsLoggedOut: grpcResp.SessionsLoggedOut,
		Message:           grpcResp.Message,
		Success:           grpcResp.Status == commonv1.Status_STATUS_SUCCESS,
		Status:            convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Logout all sessions successful for user: %s@%s, sessions_logged_out: %d", profile.Username, profile.TenantId, grpcResp.SessionsLoggedOut)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// UpdateSessionName handles PUT /{tenant_url}/api/v1/auth/sessions/{session_id}/name
func (ah *AuthHandlers) UpdateSessionName(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract session_id from path
	vars := mux.Vars(r)
	sessionID := vars["session_id"]
	if sessionID == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "session_id is required", "")
		return
	}

	// Get profile from context (set by authentication middleware)
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Profile not found in context for update session name request")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req UpdateSessionNameRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Failed to parse update session name request body: %v", err)
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate required fields
	if req.SessionName == "" {
		if ah.engine.logger != nil {
			ah.engine.logger.Warnf("Update session name request missing session_name")
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Session name is required", "")
		return
	}

	// Log incoming request details
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Update session name request received for user: %s, session: %s", profile.Username, sessionID)
		ah.engine.logger.Debugf("Request from: %s", r.RemoteAddr)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check if security client is available
	if ah.engine.GetSecurityClient() == nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security client is nil - gRPC connection may have failed during startup")
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Authentication service unavailable", "")
		return
	}

	// Call security service gRPC
	grpcReq := &securityv1.UpdateSessionNameRequest{
		TenantId:    profile.TenantId,
		UserId:      profile.UserId,
		SessionId:   sessionID,
		SessionName: req.SessionName,
	}

	// Log gRPC call attempt
	if ah.engine.logger != nil {
		ah.engine.logger.Debugf("Making gRPC UpdateSessionName call to security service")
		ah.engine.logger.Debugf("gRPC request: tenant_id=%s, user_id=%s, session_id=%s", grpcReq.TenantId, grpcReq.UserId, grpcReq.SessionId)
	}

	grpcResp, err := ah.engine.GetSecurityClient().UpdateSessionName(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Update session name failed")
		return
	}

	// Log successful gRPC response
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("gRPC UpdateSessionName call successful for user: %s, session: %s", profile.Username, sessionID)
		ah.engine.logger.Debugf("gRPC response status: %v", grpcResp.Status)
	}

	// Convert gRPC response to REST response
	response := UpdateSessionNameResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Status == commonv1.Status_STATUS_SUCCESS,
		Status:  convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Update session name successful for user: %s@%s, session: %s", profile.Username, profile.TenantId, sessionID)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// handleGRPCError maps gRPC errors to appropriate HTTP responses without exposing internal details
func (ah *AuthHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	// Extract gRPC status from error
	grpcStatus, ok := status.FromError(err)
	if !ok {
		// Not a gRPC error, treat as internal error
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Error is not a gRPC status error - this indicates a connection or protocol issue")
			ah.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			ah.engine.logger.Errorf("Original error: %v", err)
			ah.engine.logger.Errorf("Error type: %T", err)
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, "")
		return
	}

	// Map gRPC codes to HTTP status codes and user-friendly messages
	switch grpcStatus.Code() {
	case codes.Unauthenticated:
		// Expected user behavior - log as info
		if ah.engine.logger != nil {
			ah.engine.logger.Infof("Authentication failed for user - invalid credentials provided")
			ah.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusUnauthorized, "Invalid credentials", "")
	case codes.PermissionDenied:
		// Expected user behavior - log as info
		if ah.engine.logger != nil {
			ah.engine.logger.Infof("Permission denied for user")
			ah.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusForbidden, "Access denied", "")
	case codes.FailedPrecondition:
		// Expected user behavior (account disabled) - log as info
		if ah.engine.logger != nil {
			ah.engine.logger.Infof("Failed precondition: %s", grpcStatus.Message())
			ah.engine.logger.Debugf("gRPC Status: %s", grpcStatus.Code().String())
		}
		ah.writeErrorResponse(w, http.StatusForbidden, grpcStatus.Message(), "")
	case codes.NotFound:
		// Expected user behavior - log as info, don't reveal if user exists
		if ah.engine.logger != nil {
			ah.engine.logger.Infof("User or tenant not found - returning generic invalid credentials message")
			ah.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusUnauthorized, "Invalid credentials", "")
	case codes.InvalidArgument:
		// User input issue - log as info
		if ah.engine.logger != nil {
			ah.engine.logger.Infof("Invalid argument provided: %s", grpcStatus.Message())
			ah.engine.logger.Debugf("gRPC Status: %s", grpcStatus.Code().String())
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, grpcStatus.Message(), "")
	case codes.Unavailable:
		// System error - log as error
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security service is unavailable - check if service is running on correct port")
			ah.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			ah.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusServiceUnavailable, "Service temporarily unavailable", "")
	case codes.Unimplemented:
		// System error - log as error
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Security service method not implemented - this may indicate wrong service on the target port")
			ah.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			ah.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusServiceUnavailable, "Service temporarily unavailable", "")
	case codes.DeadlineExceeded:
		// System error - log as error
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("gRPC request timeout - security service took too long to respond")
			ah.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			ah.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", "")
	case codes.AlreadyExists:
		// User behavior - log as info
		if ah.engine.logger != nil {
			ah.engine.logger.Infof("Resource already exists")
			ah.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusConflict, "Resource already exists", "")
	case codes.ResourceExhausted:
		// Rate limiting - log as warning
		if ah.engine.logger != nil {
			ah.engine.logger.Warnf("Rate limit exceeded")
			ah.engine.logger.Debugf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusTooManyRequests, "Too many requests", "")
	case codes.Internal:
		// System error - log as error
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Internal server error in security service")
			ah.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			ah.engine.logger.Errorf("gRPC Status: %s - %s", grpcStatus.Code().String(), grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Internal server error", "")
	default:
		// Unknown error - log as error
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Unmapped gRPC error code: %s", grpcStatus.Code().String())
			ah.engine.logger.Errorf("gRPC request failed: %s", defaultMessage)
			ah.engine.logger.Errorf("Original gRPC error: %v", err)
			ah.engine.logger.Errorf("gRPC Status Message: %s", grpcStatus.Message())
		}
		ah.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, "")
	}
}

func (ah *AuthHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (ah *AuthHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusFailure,
	}

	json.NewEncoder(w).Encode(response)
}

// RefreshToken handles POST /{tenant_url}/api/v1/auth/refresh
func (ah *AuthHandlers) RefreshToken(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Parse request body
	var req RefreshTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.RefreshToken == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "Refresh token is required", "")
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call security service gRPC with refresh token
	grpcReq := &securityv1.AuthenticationRequest{
		TenantUrl: tenantURL,
		TokenType: "refresh",
		Token:     req.RefreshToken,
	}

	grpcResp, err := ah.engine.GetSecurityClient().Authenticate(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Token refresh failed")
		return
	}

	if grpcResp.Status != commonv1.Status_STATUS_SUCCESS {
		ah.writeErrorResponse(w, http.StatusUnauthorized, "Token refresh failed", "Invalid or expired refresh token")
		return
	}

	// Convert gRPC response to REST response
	response := RefreshTokenResponse{
		AccessToken:  grpcResp.AccessToken,
		RefreshToken: grpcResp.RefreshToken,
		Status:       convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Token refresh successful for tenant: %s", tenantURL)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}
