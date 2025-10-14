package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/services/mcpserver/internal/auth"
	"github.com/redbco/redb-open/services/mcpserver/internal/protocol"
)

// Handler handles MCP tool operations
type Handler struct {
	logger         *logger.Logger
	db             *database.PostgreSQL
	anchorClient   anchorv1.AnchorServiceClient
	authMiddleware *auth.Middleware
	mcpServerID    string
}

// NewHandler creates a new tool handler
func NewHandler(
	logger *logger.Logger,
	db *database.PostgreSQL,
	anchorClient anchorv1.AnchorServiceClient,
	authMiddleware *auth.Middleware,
	mcpServerID string,
) *Handler {
	return &Handler{
		logger:         logger,
		db:             db,
		anchorClient:   anchorClient,
		authMiddleware: authMiddleware,
		mcpServerID:    mcpServerID,
	}
}

// List returns the list of available tools
func (h *Handler) List(ctx context.Context, req *protocol.ListToolsRequest) (*protocol.ListToolsResult, error) {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		return nil, &protocol.RPCError{
			Code:    protocol.UnauthorizedError,
			Message: "No session in context",
		}
	}

	// Load tools from database for this MCP server
	rows, err := h.db.Pool().Query(ctx, `
		SELECT t.mcptool_id, t.mcptool_name, t.mcptool_description, t.mcptool_config
		FROM mcptools t
		JOIN mcp_server_tools st ON st.mcptool_id = t.mcptool_id
		WHERE st.mcpserver_id = $1 
		  AND t.tenant_id = $2 
		  AND t.workspace_id = $3
		ORDER BY t.mcptool_name
	`, h.mcpServerID, session.TenantID, session.WorkspaceID)
	if err != nil {
		h.logger.Errorf("Failed to query tools: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.InternalError,
			Message: "Failed to load tools",
		}
	}
	defer rows.Close()

	var tools []protocol.Tool
	for rows.Next() {
		var t models.MCPTool
		if err := rows.Scan(
			&t.MCPToolID,
			&t.MCPToolName,
			&t.MCPToolDescription,
			&t.MCPToolConfig,
		); err != nil {
			h.logger.Warnf("Failed to scan tool: %v", err)
			continue
		}

		// Check authorization for this tool
		if err := h.authMiddleware.AuthorizeOperation(ctx, h.mcpServerID, "tool_list", t.MCPToolID, nil); err != nil {
			h.logger.Debugf("Skipping unauthorized tool: %s", t.MCPToolName)
			continue
		}

		// Parse tool config to get input schema
		var config ToolConfig
		configBytes, _ := json.Marshal(t.MCPToolConfig)
		json.Unmarshal(configBytes, &config)

		inputSchema := config.InputSchema
		if inputSchema == nil {
			inputSchema = map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			}
		}

		tools = append(tools, protocol.Tool{
			Name:        t.MCPToolName,
			Description: t.MCPToolDescription,
			InputSchema: inputSchema,
		})
	}

	return &protocol.ListToolsResult{
		Tools: tools,
	}, nil
}

// Call executes a tool
func (h *Handler) Call(ctx context.Context, req *protocol.CallToolRequest) (*protocol.CallToolResult, error) {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		return nil, &protocol.RPCError{
			Code:    protocol.UnauthorizedError,
			Message: "No session in context",
		}
	}

	// Load tool from database
	var tool models.MCPTool
	err := h.db.Pool().QueryRow(ctx, `
		SELECT t.mcptool_id, t.mcptool_config, t.mapping_id
		FROM mcptools t
		JOIN mcp_server_tools st ON st.mcptool_id = t.mcptool_id
		WHERE st.mcpserver_id = $1 
		  AND t.mcptool_name = $2
		  AND t.tenant_id = $3 
		  AND t.workspace_id = $4
	`, h.mcpServerID, req.Name, session.TenantID, session.WorkspaceID).Scan(
		&tool.MCPToolID,
		&tool.MCPToolConfig,
		&tool.MappingID,
	)
	if err != nil {
		h.logger.Errorf("Failed to load tool: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.ToolExecutionError,
			Message: "Tool not found",
		}
	}

	// Parse tool config
	var config ToolConfig
	configBytes, _ := json.Marshal(tool.MCPToolConfig)
	json.Unmarshal(configBytes, &config)

	// Check authorization
	opContext := map[string]string{
		"operation": config.Operation,
	}
	if databaseID, ok := req.Arguments["database_id"].(string); ok {
		opContext["database_id"] = databaseID
	}
	if err := h.authMiddleware.AuthorizeOperation(ctx, h.mcpServerID, "tool_call", tool.MCPToolID, opContext); err != nil {
		return nil, &protocol.RPCError{
			Code:    protocol.ForbiddenError,
			Message: "Not authorized to execute this tool",
		}
	}

	// Execute tool based on operation type
	result, err := h.executeTool(ctx, session, config, tool.MCPToolConfig, req.Arguments)
	if err != nil {
		h.logger.Errorf("Tool execution failed: %v", err)
		return &protocol.CallToolResult{
			Content: []protocol.ToolContent{
				{
					Type: "text",
					Text: fmt.Sprintf("Error: %v", err),
				},
			},
			IsError: true,
		}, nil
	}

	return result, nil
}

// executeTool executes a tool based on its configuration
func (h *Handler) executeTool(ctx context.Context, session *auth.SessionContext, config ToolConfig, fullConfig map[string]interface{}, args map[string]interface{}) (*protocol.CallToolResult, error) {
	// Merge config defaults with arguments (arguments take precedence)
	mergedArgs := make(map[string]interface{})

	// First, copy values from tool config (database_id, table_name, etc.)
	if dbID, ok := fullConfig["database_id"].(string); ok && dbID != "" {
		mergedArgs["database_id"] = dbID
	}
	if tableName, ok := fullConfig["table_name"].(string); ok && tableName != "" {
		mergedArgs["table_name"] = tableName
	}

	// Then, overlay with provided arguments (allows overriding)
	for k, v := range args {
		mergedArgs[k] = v
	}

	switch config.Operation {
	case "query_database":
		return h.executeQuery(ctx, session, mergedArgs)
	case "insert_data":
		return h.executeInsert(ctx, session, mergedArgs)
	case "update_data":
		return h.executeUpdate(ctx, session, mergedArgs)
	case "delete_data":
		return h.executeDelete(ctx, session, mergedArgs)
	case "get_schema":
		return h.executeGetSchema(ctx, session, mergedArgs)
	case "deploy_schema":
		return h.executeDeploySchema(ctx, session, mergedArgs)
	case "execute_command":
		return h.executeCommand(ctx, session, mergedArgs)
	default:
		return nil, fmt.Errorf("unknown operation: %s", config.Operation)
	}
}

// executeQuery executes a database query
func (h *Handler) executeQuery(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	databaseIdentifier, _ := args["database_id"].(string)
	// Also check for "database_name" for backwards compatibility
	if databaseIdentifier == "" {
		databaseIdentifier, _ = args["database_name"].(string)
	}
	tableName, _ := args["table_name"].(string)
	options, _ := args["options"].(map[string]interface{})

	if databaseIdentifier == "" || tableName == "" {
		return nil, fmt.Errorf("database_id (or database_name) and table_name are required")
	}

	// Resolve database identifier to ID
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve database: %w", err)
	}

	optionsBytes, _ := json.Marshal(options)

	resp, err := h.anchorClient.FetchData(ctx, &anchorv1.FetchDataRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		TableName:   tableName,
		Options:     optionsBytes,
	})

	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("query failed: %s", resp.Message)
	}

	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type:     "text",
				Text:     string(resp.Data),
				MimeType: "application/json",
			},
		},
	}, nil
}

// executeInsert inserts data into a database
func (h *Handler) executeInsert(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	databaseIdentifier, _ := args["database_id"].(string)
	if databaseIdentifier == "" {
		databaseIdentifier, _ = args["database_name"].(string)
	}
	tableName, _ := args["table_name"].(string)
	data, _ := args["data"].([]interface{})

	if databaseIdentifier == "" || tableName == "" || len(data) == 0 {
		return nil, fmt.Errorf("database_id (or database_name), table_name, and data are required")
	}

	// Resolve database identifier to ID
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve database: %w", err)
	}

	dataBytes, _ := json.Marshal(data)

	resp, err := h.anchorClient.InsertData(ctx, &anchorv1.InsertDataRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		TableName:   tableName,
		Data:        dataBytes,
	})

	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("insert failed: %s", resp.Message)
	}

	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type: "text",
				Text: fmt.Sprintf("Inserted %d rows successfully", resp.RowsAffected),
			},
		},
	}, nil
}

// executeUpdate updates data in a database (placeholder)
func (h *Handler) executeUpdate(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	// TODO: Implement via Anchor when update endpoint is available
	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type: "text",
				Text: "Update operation not yet implemented",
			},
		},
	}, nil
}

// executeDelete deletes data from a database (placeholder)
func (h *Handler) executeDelete(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	// TODO: Implement via Anchor when delete endpoint is available
	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type: "text",
				Text: "Delete operation not yet implemented",
			},
		},
	}, nil
}

// executeGetSchema gets database schema
func (h *Handler) executeGetSchema(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	databaseIdentifier, _ := args["database_id"].(string)
	if databaseIdentifier == "" {
		databaseIdentifier, _ = args["database_name"].(string)
	}

	if databaseIdentifier == "" {
		return nil, fmt.Errorf("database_id (or database_name) is required")
	}

	// Resolve database identifier to ID
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve database: %w", err)
	}

	resp, err := h.anchorClient.GetDatabaseSchema(ctx, &anchorv1.GetDatabaseSchemaRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
	})

	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("get schema failed: %s", resp.Message)
	}

	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type:     "text",
				Text:     string(resp.Schema),
				MimeType: "application/json",
			},
		},
	}, nil
}

// executeDeploySchema deploys a schema to a database
func (h *Handler) executeDeploySchema(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	databaseIdentifier, _ := args["database_id"].(string)
	if databaseIdentifier == "" {
		databaseIdentifier, _ = args["database_name"].(string)
	}
	schema, _ := args["schema"].(map[string]interface{})

	if databaseIdentifier == "" || schema == nil {
		return nil, fmt.Errorf("database_id (or database_name) and schema are required")
	}

	// Resolve database identifier to ID
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve database: %w", err)
	}

	schemaBytes, _ := json.Marshal(schema)

	resp, err := h.anchorClient.DeployDatabaseSchema(ctx, &anchorv1.DeployDatabaseSchemaRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		Schema:      schemaBytes,
	})

	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("deploy schema failed: %s", resp.Message)
	}

	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type: "text",
				Text: "Schema deployed successfully",
			},
		},
	}, nil
}

// executeCommand executes a vendor-specific command
func (h *Handler) executeCommand(ctx context.Context, session *auth.SessionContext, args map[string]interface{}) (*protocol.CallToolResult, error) {
	databaseIdentifier, _ := args["database_id"].(string)
	if databaseIdentifier == "" {
		databaseIdentifier, _ = args["database_name"].(string)
	}
	command, _ := args["command"].(string)

	if databaseIdentifier == "" || command == "" {
		return nil, fmt.Errorf("database_id (or database_name) and command are required")
	}

	// Resolve database identifier to ID
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve database: %w", err)
	}

	resp, err := h.anchorClient.ExecuteCommand(ctx, &anchorv1.ExecuteCommandRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		Command:     command,
	})

	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("command execution failed: %s", resp.Message)
	}

	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type:     "text",
				Text:     string(resp.Data),
				MimeType: "application/json",
			},
		},
	}, nil
}

// ToolConfig represents tool configuration
type ToolConfig struct {
	Operation   string                 `json:"operation"`
	InputSchema map[string]interface{} `json:"input_schema"`
	Parameters  map[string]interface{} `json:"parameters"`
}

// resolveDatabaseIdentifier resolves a database identifier (ID or name) to a database ID
func (h *Handler) resolveDatabaseIdentifier(ctx context.Context, session *auth.SessionContext, identifier string) (string, error) {
	if identifier == "" {
		return "", fmt.Errorf("database identifier is empty")
	}

	// Check if it's already a ULID (database ID format: db_...)
	if strings.HasPrefix(identifier, "db_") {
		return identifier, nil
	}

	// Otherwise, treat it as a database name and look it up
	var databaseID string
	err := h.db.Pool().QueryRow(ctx, `
		SELECT database_id
		FROM databases
		WHERE database_name = $1
		  AND tenant_id = $2
		  AND workspace_id = $3
		  AND database_enabled = true
	`, identifier, session.TenantID, session.WorkspaceID).Scan(&databaseID)

	if err != nil {
		return "", fmt.Errorf("database not found: %s", identifier)
	}

	return databaseID, nil
}
