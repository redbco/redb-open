package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mcpserver/internal/protocol"
)

// Middleware handles authentication and authorization for MCP requests
type Middleware struct {
	logger         *logger.Logger
	securityClient securityv1.SecurityServiceClient
}

// NewMiddleware creates a new auth middleware
func NewMiddleware(logger *logger.Logger, securityClient securityv1.SecurityServiceClient) *Middleware {
	return &Middleware{
		logger:         logger,
		securityClient: securityClient,
	}
}

// Authenticate wraps an HTTP handler to add authentication
func (m *Middleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			m.writeError(w, nil, protocol.UnauthorizedError, "Missing authorization header")
			return
		}

		// Parse token type and token
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 {
			m.writeError(w, nil, protocol.UnauthorizedError, "Invalid authorization header format")
			return
		}

		tokenType := strings.ToLower(parts[0])
		token := parts[1]

		// Validate supported token types
		if tokenType != "bearer" && tokenType != "apitoken" {
			m.writeError(w, nil, protocol.UnauthorizedError, "Unsupported token type")
			return
		}

		// Map to security service token types
		securityTokenType := "jwt"
		if tokenType == "apitoken" {
			securityTokenType = "api_token"
		}

		// Validate token with security service
		ctx := r.Context()
		validateResp, err := m.securityClient.ValidateMCPSession(ctx, &securityv1.ValidateMCPSessionRequest{
			Token:     token,
			TokenType: securityTokenType,
		})

		if err != nil || !validateResp.Valid {
			message := "Invalid token"
			if validateResp != nil {
				message = validateResp.Message
			}
			m.logger.Warnf("MCP authentication failed: %s", message)
			m.writeError(w, nil, protocol.UnauthorizedError, message)
			return
		}

		// Create session context
		session := &SessionContext{
			TenantID:    validateResp.TenantId,
			WorkspaceID: validateResp.WorkspaceId,
			UserID:      validateResp.UserId,
			Token:       token,
			TokenType:   securityTokenType,
			Validated:   true,
		}

		// Add session to context
		ctx = SetSessionInContext(ctx, session)
		r = r.WithContext(ctx)

		// Call next handler
		next.ServeHTTP(w, r)
	})
}

// AuthorizeOperation checks if the session can perform an MCP operation
func (m *Middleware) AuthorizeOperation(ctx context.Context, mcpServerID, operationType, resourceID string, operationContext map[string]string) error {
	session, ok := GetSessionFromContext(ctx)
	if !ok {
		return fmt.Errorf("no session in context")
	}

	if !session.Validated {
		return fmt.Errorf("session not validated")
	}

	// Call security service to authorize
	authResp, err := m.securityClient.AuthorizeMCPOperation(ctx, &securityv1.AuthorizeMCPOperationRequest{
		TenantId:         session.TenantID,
		WorkspaceId:      session.WorkspaceID,
		UserId:           session.UserID,
		McpServerId:      mcpServerID,
		OperationType:    operationType,
		ResourceId:       resourceID,
		OperationContext: operationContext,
	})

	if err != nil {
		m.logger.Errorf("Authorization check failed: %v", err)
		return fmt.Errorf("authorization check failed: %w", err)
	}

	if !authResp.Authorized {
		m.logger.Warnf("Operation not authorized: %s for user %s", operationType, session.UserID)
		return fmt.Errorf("not authorized: %s", authResp.Message)
	}

	m.logger.Debugf("Operation authorized: %s for user %s (policies: %v)", operationType, session.UserID, authResp.AppliedPolicies)
	return nil
}

// writeError writes a JSON-RPC error response
func (m *Middleware) writeError(w http.ResponseWriter, id interface{}, code int, message string) {
	resp := protocol.JSONRPCResponse{
		JSONRPC: "2.0",
		Error: &protocol.RPCError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ExtractRequestID extracts the JSON-RPC request ID from the body without fully parsing
// This is used to provide the ID in authentication errors
func ExtractRequestID(r *http.Request) interface{} {
	// Try to read and restore body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	// Try to extract ID
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil
	}

	if id, ok := data["id"]; ok {
		return id
	}
	return nil
}
