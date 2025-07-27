package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

// Context keys
const (
	profileContextKey contextKey = "profile"
)

// Middleware contains authentication and authorization middleware
type Middleware struct {
	engine *Engine
}

// NewMiddleware creates a new middleware instance
func NewMiddleware(engine *Engine) *Middleware {
	return &Middleware{
		engine: engine,
	}
}

// AuthenticationMiddleware authenticates requests using the security service
func (m *Middleware) AuthenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip authentication for certain routes
		if m.shouldSkipAuth(r) {
			next.ServeHTTP(w, r)
			return
		}

		// Extract tenant_url from path
		vars := mux.Vars(r)
		tenantURL := vars["tenant_url"]
		if tenantURL == "" {
			m.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
			return
		}

		// Extract token from Authorization header
		token := m.extractBearerToken(r)
		if token == "" {
			m.writeErrorResponse(w, http.StatusUnauthorized, "Authorization token is required", "")
			return
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		// Call security service to authenticate
		authReq := &securityv1.AuthenticationRequest{
			TenantUrl: tenantURL,
			TokenType: "Bearer",
			Token:     token,
		}

		authResp, err := m.engine.GetSecurityClient().Authenticate(ctx, authReq)
		if err != nil {
			m.writeErrorResponse(w, http.StatusInternalServerError, "Authentication failed", err.Error())
			return
		}

		if authResp.Status != commonv1.Status_STATUS_SUCCESS {
			m.writeErrorResponse(w, http.StatusUnauthorized, "Authentication failed", "Invalid or expired token")
			return
		}

		// Store profile in request context for use by handlers
		ctx = context.WithValue(r.Context(), profileContextKey, authResp.Profile)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}

// AuthorizationMiddleware authorizes requests using the security service
func (m *Middleware) AuthorizationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip authorization for certain routes
		if m.shouldSkipAuth(r) {
			next.ServeHTTP(w, r)
			return
		}

		// Get profile from context (set by authentication middleware)
		profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
		if !ok || profile == nil {
			m.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
			return
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		// Determine resource type and action based on the request
		resourceType, resourceID, action := m.determinePermissions(r)

		// Call security service to authorize
		authzReq := &securityv1.AuthorizationRequest{
			TenantId:     profile.TenantId,
			UserId:       profile.UserId,
			ResourceType: resourceType,
			ResourceId:   resourceID,
			Action:       action,
		}

		authzResp, err := m.engine.GetSecurityClient().Authorize(ctx, authzReq)
		if err != nil {
			m.writeErrorResponse(w, http.StatusInternalServerError, "Authorization failed", err.Error())
			return
		}

		if !authzResp.Authorized {
			m.writeErrorResponse(w, http.StatusForbidden, "Access denied", authzResp.Message)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// shouldSkipAuth determines if authentication should be skipped for a route
func (m *Middleware) shouldSkipAuth(r *http.Request) bool {
	path := r.URL.Path
	method := r.Method

	// Skip authentication for health checks
	if strings.HasSuffix(path, "/health") && method == http.MethodGet {
		return true
	}

	// Skip authentication for login endpoint
	if strings.HasSuffix(path, "/auth/login") && method == http.MethodPost {
		return true
	}

	// Skip authentication for OPTIONS requests (CORS preflight)
	if method == http.MethodOptions {
		return true
	}

	return false
}

// determinePermissions determines the resource type, resource ID, and action based on the request
func (m *Middleware) determinePermissions(r *http.Request) (resourceType, resourceID, action string) {
	path := r.URL.Path
	method := r.Method

	// Parse the path to determine resource type
	// Expected format: /{tenant_url}/api/v1/{resource}/{resource_id?}
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	if len(pathParts) >= 4 {
		resourceType = pathParts[3] // e.g., "auth", "database", "workspace"
	}

	if len(pathParts) >= 5 {
		resourceID = pathParts[4] // specific resource ID if present
	}

	// Determine action based on HTTP method
	switch method {
	case http.MethodGet:
		action = "read"
	case http.MethodPost:
		action = "create"
	case http.MethodPut, http.MethodPatch:
		action = "update"
	case http.MethodDelete:
		action = "delete"
	default:
		action = "unknown"
	}

	// Special cases for auth endpoints
	if resourceType == "auth" {
		switch {
		case strings.HasSuffix(path, "/profile"):
			action = "read_profile"
		case strings.HasSuffix(path, "/logout"):
			action = "logout"
		case strings.HasSuffix(path, "/toggle-root"):
			action = "toggle_root"
		case strings.HasSuffix(path, "/change-password"):
			action = "change_password"
		case strings.HasSuffix(path, "/sessions") && method == http.MethodGet:
			action = "list_sessions"
		case strings.Contains(path, "/sessions/") && strings.HasSuffix(path, "/logout"):
			action = "logout_session"
		case strings.HasSuffix(path, "/sessions/logout-all"):
			action = "logout_all_sessions"
		case strings.Contains(path, "/sessions/") && strings.HasSuffix(path, "/name"):
			action = "update_session_name"
		}
	}

	// Special cases for workspace endpoints
	if resourceType == "workspaces" {
		switch method {
		case http.MethodGet:
			if resourceID != "" {
				action = "read_workspace"
			} else {
				action = "list_workspaces"
			}
		case http.MethodPost:
			action = "create_workspace"
		case http.MethodPut, http.MethodPatch:
			action = "update_workspace"
		case http.MethodDelete:
			action = "delete_workspace"
		}
	}

	return resourceType, resourceID, action
}

// extractBearerToken extracts the bearer token from the Authorization header
func (m *Middleware) extractBearerToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}

	return parts[1]
}

// writeErrorResponse writes an error response in JSON format
func (m *Middleware) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusFailure,
	}

	json.NewEncoder(w).Encode(response)
}
