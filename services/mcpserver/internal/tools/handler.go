package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	transformationv1 "github.com/redbco/redb-open/api/proto/transformation/v1"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/grpcconfig"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/services/mcpserver/internal/auth"
	"github.com/redbco/redb-open/services/mcpserver/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Handler handles MCP tool operations
type Handler struct {
	logger         *logger.Logger
	db             *database.PostgreSQL
	anchorClient   anchorv1.AnchorServiceClient
	authMiddleware *auth.Middleware
	mcpServerID    string
	config         *config.Config // Global config for service discovery
}

// NewHandler creates a new tool handler
func NewHandler(
	logger *logger.Logger,
	db *database.PostgreSQL,
	anchorClient anchorv1.AnchorServiceClient,
	authMiddleware *auth.Middleware,
	mcpServerID string,
	config *config.Config,
) *Handler {
	return &Handler{
		logger:         logger,
		db:             db,
		anchorClient:   anchorClient,
		authMiddleware: authMiddleware,
		mcpServerID:    mcpServerID,
		config:         config,
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
	result, err := h.executeTool(ctx, session, config, tool.MCPToolConfig, tool.MappingID, req.Arguments)
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
func (h *Handler) executeTool(ctx context.Context, session *auth.SessionContext, config ToolConfig, fullConfig map[string]interface{}, mappingID string, args map[string]interface{}) (*protocol.CallToolResult, error) {
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
		return h.executeQuery(ctx, session, mappingID, mergedArgs)
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
func (h *Handler) executeQuery(ctx context.Context, session *auth.SessionContext, mappingID string, args map[string]interface{}) (*protocol.CallToolResult, error) {
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

	// Apply transformations if mapping exists
	resultData := resp.Data
	if mappingID != "" {
		// Load mapping rules for transformation
		mappingRules, err := h.loadMappingRules(ctx, session, mappingID)
		if err != nil {
			h.logger.Warnf("Failed to load mapping rules: %v", err)
			// Continue with untransformed data
		} else if len(mappingRules) > 0 {
			// Apply transformations
			transformedData, err := h.applyMappingTransformations(ctx, session, resp.Data, mappingRules)
			if err != nil {
				h.logger.Warnf("Failed to apply transformations: %v", err)
				// Continue with untransformed data
			} else {
				resultData = transformedData
			}
		}
	}

	return &protocol.CallToolResult{
		Content: []protocol.ToolContent{
			{
				Type:     "text",
				Text:     string(resultData),
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

// MappingRule represents a simplified mapping rule for transformations
type MappingRule struct {
	ID       string
	Name     string
	Metadata map[string]interface{}
}

// loadMappingRules loads all mapping rules for a given mapping
func (h *Handler) loadMappingRules(ctx context.Context, session *auth.SessionContext, mappingID string) ([]*MappingRule, error) {
	query := `
		SELECT mr.mapping_rule_id, mr.mapping_rule_name, mr.mapping_rule_metadata
		FROM mapping_rules mr
		JOIN mapping_rule_mappings mrm ON mr.mapping_rule_id = mrm.mapping_rule_id
		WHERE mrm.mapping_id = $1
		  AND mr.tenant_id = $2
		  AND mr.workspace_id = $3
		ORDER BY mrm.mapping_rule_order ASC
	`

	rows, err := h.db.Pool().Query(ctx, query, mappingID, session.TenantID, session.WorkspaceID)
	if err != nil {
		return nil, fmt.Errorf("failed to query mapping rules: %w", err)
	}
	defer rows.Close()

	var rules []*MappingRule
	for rows.Next() {
		var rule MappingRule
		var metadataBytes []byte

		err := rows.Scan(&rule.ID, &rule.Name, &metadataBytes)
		if err != nil {
			h.logger.Warnf("Failed to scan mapping rule: %v", err)
			continue
		}

		// Parse metadata
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &rule.Metadata); err != nil {
				h.logger.Warnf("Failed to parse metadata for rule %s: %v", rule.Name, err)
				continue
			}
		}

		rules = append(rules, &rule)
	}

	return rules, nil
}

// applyMappingTransformations applies transformation rules to data
func (h *Handler) applyMappingTransformations(ctx context.Context, session *auth.SessionContext, data []byte, rules []*MappingRule) ([]byte, error) {
	// Parse the JSON data (array of rows)
	var sourceRows []map[string]interface{}
	if err := json.Unmarshal(data, &sourceRows); err != nil {
		return nil, fmt.Errorf("failed to parse source data: %w", err)
	}

	// Try to get transformation client (optional - may not be available)
	var transformationClient transformationv1.TransformationServiceClient
	var transformationClientErr error
	transformationClient, transformationClientErr = h.getTransformationClient(ctx)
	if transformationClientErr != nil {
		h.logger.Warnf("Transformation service unavailable, will use direct mapping for all transformations: %v", transformationClientErr)
	}

	// Transform each row
	targetRows := make([]map[string]interface{}, 0, len(sourceRows))
	for _, sourceRow := range sourceRows {
		targetRow := make(map[string]interface{})

		// Apply each mapping rule
		for _, rule := range rules {
			// Extract source and target column names from metadata
			sourceColumn, _ := rule.Metadata["source_column"].(string)
			targetColumn, _ := rule.Metadata["target_column"].(string)
			transformationName, _ := rule.Metadata["transformation_name"].(string)

			if sourceColumn == "" || targetColumn == "" {
				h.logger.Warnf("Rule %s missing source or target column in metadata", rule.Name)
				continue
			}

			// Get the source value
			sourceValue, exists := sourceRow[sourceColumn]
			if !exists {
				h.logger.Debugf("Source column '%s' not found in row data", sourceColumn)
				// Set null for missing columns
				targetRow[targetColumn] = nil
				continue
			}

			// Apply transformation if needed
			var targetValue interface{}
			if transformationName != "" && transformationName != "direct_mapping" {
				// Call transformation service for non-direct transformations
				if transformationClient != nil {
					transformedValue, err := h.applyTransformation(ctx, transformationClient, transformationName, sourceValue)
					if err != nil {
						h.logger.Warnf("Failed to apply transformation '%s' to column '%s': %v, using original value",
							transformationName, sourceColumn, err)
						targetValue = sourceValue
					} else {
						targetValue = transformedValue
					}
				} else {
					// Transformation service not available, fall back to direct mapping
					h.logger.Debugf("Transformation service unavailable, using direct mapping for '%s'", transformationName)
					targetValue = sourceValue
				}
			} else {
				// Direct mapping - no transformation needed
				targetValue = sourceValue
			}

			// Set the target column with the (possibly transformed) value
			targetRow[targetColumn] = targetValue
		}

		targetRows = append(targetRows, targetRow)
	}

	// Convert back to JSON
	transformedData, err := json.Marshal(targetRows)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transformed data: %w", err)
	}

	return transformedData, nil
}

// applyTransformation applies a single transformation to a value
func (h *Handler) applyTransformation(ctx context.Context, client transformationv1.TransformationServiceClient, transformationName string, value interface{}) (interface{}, error) {
	// Convert value to string for transformation
	var inputStr string
	switch v := value.(type) {
	case string:
		inputStr = v
	case nil:
		return nil, nil
	default:
		// Convert other types to string
		inputStr = fmt.Sprintf("%v", v)
	}

	// Call transformation service
	transformReq := &transformationv1.TransformRequest{
		FunctionName: transformationName,
		Input:        inputStr,
	}

	transformResp, err := client.Transform(ctx, transformReq)
	if err != nil {
		return nil, fmt.Errorf("transformation service error: %w", err)
	}

	if transformResp.Status != commonv1.Status_STATUS_SUCCESS {
		return nil, fmt.Errorf("transformation failed: %s", transformResp.StatusMessage)
	}

	return transformResp.Output, nil
}

// getTransformationClient returns a transformation service client
func (h *Handler) getTransformationClient(ctx context.Context) (transformationv1.TransformationServiceClient, error) {
	// Get transformation service address from global config
	transformationAddr := grpcconfig.GetServiceAddress(h.config, "transformation")

	// Connect to transformation service without blocking
	// The connection is established lazily when first used
	conn, err := grpc.Dial(transformationAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to transformation service at %s: %w", transformationAddr, err)
	}

	return transformationv1.NewTransformationServiceClient(conn), nil
}
