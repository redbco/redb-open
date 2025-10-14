package auth

import (
	"context"
)

// ContextKey is the type for context keys
type ContextKey string

const (
	// SessionContextKey is the key for session information in context
	SessionContextKey ContextKey = "mcp_session"
)

// SessionContext holds authenticated session information
type SessionContext struct {
	TenantID    string
	WorkspaceID string
	UserID      string
	Token       string
	TokenType   string
	Validated   bool
}

// GetSessionFromContext retrieves the session from context
func GetSessionFromContext(ctx context.Context) (*SessionContext, bool) {
	session, ok := ctx.Value(SessionContextKey).(*SessionContext)
	return session, ok
}

// SetSessionInContext sets the session in context
func SetSessionInContext(ctx context.Context, session *SessionContext) context.Context {
	return context.WithValue(ctx, SessionContextKey, session)
}
