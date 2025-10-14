package prompts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/pkg/models"
	"github.com/redbco/redb-open/services/mcpserver/internal/auth"
	"github.com/redbco/redb-open/services/mcpserver/internal/protocol"
)

// Handler handles MCP prompt operations
type Handler struct {
	logger         *logger.Logger
	db             *database.PostgreSQL
	authMiddleware *auth.Middleware
	mcpServerID    string
}

// NewHandler creates a new prompt handler
func NewHandler(
	logger *logger.Logger,
	db *database.PostgreSQL,
	authMiddleware *auth.Middleware,
	mcpServerID string,
) *Handler {
	return &Handler{
		logger:         logger,
		db:             db,
		authMiddleware: authMiddleware,
		mcpServerID:    mcpServerID,
	}
}

// List returns the list of available prompts
func (h *Handler) List(ctx context.Context, req *protocol.ListPromptsRequest) (*protocol.ListPromptsResult, error) {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		return nil, &protocol.RPCError{
			Code:    protocol.UnauthorizedError,
			Message: "No session in context",
		}
	}

	// Load prompts from database for this MCP server
	rows, err := h.db.Pool().Query(ctx, `
		SELECT p.mcpprompt_id, p.mcpprompt_name, p.mcpprompt_description, p.mcpprompt_config
		FROM mcpprompts p
		JOIN mcp_server_prompts sp ON sp.mcpprompt_id = p.mcpprompt_id
		WHERE sp.mcpserver_id = $1 
		  AND p.tenant_id = $2 
		  AND p.workspace_id = $3
		ORDER BY p.mcpprompt_name
	`, h.mcpServerID, session.TenantID, session.WorkspaceID)
	if err != nil {
		h.logger.Errorf("Failed to query prompts: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.InternalError,
			Message: "Failed to load prompts",
		}
	}
	defer rows.Close()

	var prompts []protocol.Prompt
	for rows.Next() {
		var p models.MCPPrompt
		if err := rows.Scan(
			&p.MCPPromptID,
			&p.MCPPromptName,
			&p.MCPPromptDescription,
			&p.MCPPromptConfig,
		); err != nil {
			h.logger.Warnf("Failed to scan prompt: %v", err)
			continue
		}

		// Check authorization for this prompt
		if err := h.authMiddleware.AuthorizeOperation(ctx, h.mcpServerID, "prompt_list", p.MCPPromptID, nil); err != nil {
			h.logger.Debugf("Skipping unauthorized prompt: %s", p.MCPPromptName)
			continue
		}

		// Parse prompt config to get arguments
		var config PromptConfig
		configBytes, _ := json.Marshal(p.MCPPromptConfig)
		json.Unmarshal(configBytes, &config)

		var arguments []protocol.PromptArgument
		for _, arg := range config.Arguments {
			arguments = append(arguments, protocol.PromptArgument{
				Name:        arg.Name,
				Description: arg.Description,
				Required:    arg.Required,
			})
		}

		prompts = append(prompts, protocol.Prompt{
			Name:        p.MCPPromptName,
			Description: p.MCPPromptDescription,
			Arguments:   arguments,
		})
	}

	return &protocol.ListPromptsResult{
		Prompts: prompts,
	}, nil
}

// Get returns a rendered prompt with arguments
func (h *Handler) Get(ctx context.Context, req *protocol.GetPromptRequest) (*protocol.GetPromptResult, error) {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		return nil, &protocol.RPCError{
			Code:    protocol.UnauthorizedError,
			Message: "No session in context",
		}
	}

	// Load prompt from database
	var prompt models.MCPPrompt
	err := h.db.Pool().QueryRow(ctx, `
		SELECT p.mcpprompt_id, p.mcpprompt_description, p.mcpprompt_config, p.mapping_id
		FROM mcpprompts p
		JOIN mcp_server_prompts sp ON sp.mcpprompt_id = p.mcpprompt_id
		WHERE sp.mcpserver_id = $1 
		  AND p.mcpprompt_name = $2
		  AND p.tenant_id = $3 
		  AND p.workspace_id = $4
	`, h.mcpServerID, req.Name, session.TenantID, session.WorkspaceID).Scan(
		&prompt.MCPPromptID,
		&prompt.MCPPromptDescription,
		&prompt.MCPPromptConfig,
		&prompt.MappingID,
	)
	if err != nil {
		h.logger.Errorf("Failed to load prompt: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.ResourceNotFoundError,
			Message: "Prompt not found",
		}
	}

	// Check authorization
	if err := h.authMiddleware.AuthorizeOperation(ctx, h.mcpServerID, "prompt_get", prompt.MCPPromptID, nil); err != nil {
		return nil, &protocol.RPCError{
			Code:    protocol.ForbiddenError,
			Message: "Not authorized to get this prompt",
		}
	}

	// Parse prompt config
	var config PromptConfig
	configBytes, _ := json.Marshal(prompt.MCPPromptConfig)
	json.Unmarshal(configBytes, &config)

	// Validate required arguments
	for _, arg := range config.Arguments {
		if arg.Required {
			if _, ok := req.Arguments[arg.Name]; !ok {
				return nil, &protocol.RPCError{
					Code:    protocol.InvalidParams,
					Message: fmt.Sprintf("Missing required argument: %s", arg.Name),
				}
			}
		}
	}

	// Render prompt with arguments
	messages, err := h.renderPrompt(ctx, config, req.Arguments)
	if err != nil {
		h.logger.Errorf("Failed to render prompt: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.InternalError,
			Message: fmt.Sprintf("Failed to render prompt: %v", err),
		}
	}

	return &protocol.GetPromptResult{
		Description: prompt.MCPPromptDescription,
		Messages:    messages,
	}, nil
}

// renderPrompt renders a prompt template with arguments
func (h *Handler) renderPrompt(ctx context.Context, config PromptConfig, args map[string]interface{}) ([]protocol.PromptMessage, error) {
	var messages []protocol.PromptMessage

	for _, msg := range config.Messages {
		// Substitute placeholders in message text
		text := msg.Text
		for key, value := range args {
			placeholder := fmt.Sprintf("{%s}", key)
			text = strings.ReplaceAll(text, placeholder, fmt.Sprintf("%v", value))
		}

		messages = append(messages, protocol.PromptMessage{
			Role: msg.Role,
			Content: protocol.PromptContent{
				Type: "text",
				Text: text,
			},
		})
	}

	return messages, nil
}

// PromptConfig represents prompt configuration
type PromptConfig struct {
	Arguments []PromptArgumentConfig `json:"arguments"`
	Messages  []PromptMessageConfig  `json:"messages"`
}

// PromptArgumentConfig represents a prompt argument configuration
type PromptArgumentConfig struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
	Type        string `json:"type"`
}

// PromptMessageConfig represents a prompt message configuration
type PromptMessageConfig struct {
	Role string `json:"role"`
	Text string `json:"text"`
}
