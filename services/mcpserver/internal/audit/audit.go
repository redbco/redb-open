package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/mcpserver/internal/auth"
)

// Logger handles audit logging for MCP operations
type Logger struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewLogger creates a new audit logger
func NewLogger(db *database.PostgreSQL, logger *logger.Logger) *Logger {
	return &Logger{
		db:     db,
		logger: logger,
	}
}

// LogOperation logs an MCP operation to the audit trail
func (a *Logger) LogOperation(ctx context.Context, action string, resourceType string, resourceID string, details map[string]interface{}, success bool) error {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		a.logger.Warn("Audit log attempted without session context")
		return nil // Don't fail the operation due to audit logging
	}

	// Prepare status
	status := "STATUS_SUCCESS"
	if !success {
		status = "STATUS_FAILURE"
	}

	// Marshal details to JSON
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		a.logger.Warnf("Failed to marshal audit details: %v", err)
		detailsJSON = []byte("{}")
	}

	// Insert audit log
	_, err = a.db.Pool().Exec(ctx, `
		INSERT INTO audit_log (
			tenant_id,
			user_id,
			action,
			resource_type,
			resource_id,
			change_details,
			status,
			created
		) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
	`, session.TenantID, session.UserID, action, resourceType, resourceID, detailsJSON, status)

	if err != nil {
		a.logger.Errorf("Failed to write audit log: %v", err)
		return err
	}

	a.logger.Debugf("Audit log: %s %s %s by user %s (%s)", action, resourceType, resourceID, session.UserID, status)
	return nil
}

// LogResourceRead logs a resource read operation
func (a *Logger) LogResourceRead(ctx context.Context, resourceID string, uri string, success bool) error {
	return a.LogOperation(ctx, "mcp_resource_read", "mcpresource", resourceID, map[string]interface{}{
		"uri": uri,
	}, success)
}

// LogToolCall logs a tool execution
func (a *Logger) LogToolCall(ctx context.Context, toolID string, toolName string, args map[string]interface{}, success bool) error {
	return a.LogOperation(ctx, "mcp_tool_call", "mcptool", toolID, map[string]interface{}{
		"tool_name": toolName,
		"arguments": args,
	}, success)
}

// LogPromptGet logs a prompt retrieval
func (a *Logger) LogPromptGet(ctx context.Context, promptID string, promptName string, args map[string]interface{}, success bool) error {
	return a.LogOperation(ctx, "mcp_prompt_get", "mcpprompt", promptID, map[string]interface{}{
		"prompt_name": promptName,
		"arguments":   args,
	}, success)
}

// LogInitialize logs a client initialization
func (a *Logger) LogInitialize(ctx context.Context, clientName string, clientVersion string, success bool) error {
	session, ok := auth.GetSessionFromContext(ctx)
	var userID string
	if ok {
		userID = session.UserID
	}

	return a.LogOperation(ctx, "mcp_initialize", "mcp_session", userID, map[string]interface{}{
		"client_name":    clientName,
		"client_version": clientVersion,
	}, success)
}

// LogAuthenticationFailure logs an authentication failure
func (a *Logger) LogAuthenticationFailure(ctx context.Context, reason string) error {
	// For authentication failures, we don't have a session yet
	// Log with minimal information
	_, err := a.db.Pool().Exec(ctx, `
		INSERT INTO audit_log (
			tenant_id,
			user_id,
			action,
			resource_type,
			change_details,
			status,
			created
		) VALUES ('', '', 'mcp_auth_failure', 'authentication', $1, $2, CURRENT_TIMESTAMP)
	`, fmt.Sprintf(`{"reason": "%s"}`, reason), "STATUS_FAILURE")

	if err != nil {
		a.logger.Errorf("Failed to write audit log for auth failure: %v", err)
		return err
	}

	return nil
}
