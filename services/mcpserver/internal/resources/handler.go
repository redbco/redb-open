package resources

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

// Handler handles MCP resource operations
type Handler struct {
	logger         *logger.Logger
	db             *database.PostgreSQL
	anchorClient   anchorv1.AnchorServiceClient
	authMiddleware *auth.Middleware
	mcpServerID    string
}

// NewHandler creates a new resource handler
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

// List returns the list of available resources
func (h *Handler) List(ctx context.Context, req *protocol.ListResourcesRequest) (*protocol.ListResourcesResult, error) {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		return nil, &protocol.RPCError{
			Code:    protocol.UnauthorizedError,
			Message: "No session in context",
		}
	}

	// Load resources from database for this MCP server
	rows, err := h.db.Pool().Query(ctx, `
		SELECT r.mcpresource_id, r.mcpresource_name, r.mcpresource_description, 
		       r.mcpresource_config, r.mapping_id
		FROM mcpresources r
		JOIN mcp_server_resources sr ON sr.mcpresource_id = r.mcpresource_id
		WHERE sr.mcpserver_id = $1 
		  AND r.tenant_id = $2 
		  AND r.workspace_id = $3
		ORDER BY r.mcpresource_name
	`, h.mcpServerID, session.TenantID, session.WorkspaceID)
	if err != nil {
		h.logger.Errorf("Failed to query resources: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.InternalError,
			Message: "Failed to load resources",
		}
	}
	defer rows.Close()

	var resources []protocol.Resource
	for rows.Next() {
		var r models.MCPResource
		if err := rows.Scan(
			&r.MCPResourceID,
			&r.MCPResourceName,
			&r.MCPResourceDescription,
			&r.MCPResourceConfig,
			&r.MappingID,
		); err != nil {
			h.logger.Warnf("Failed to scan resource: %v", err)
			continue
		}

		// Check authorization for this resource
		if err := h.authMiddleware.AuthorizeOperation(ctx, h.mcpServerID, "resource_list", r.MCPResourceID, nil); err != nil {
			h.logger.Debugf("Skipping unauthorized resource: %s", r.MCPResourceName)
			continue
		}

		// Parse config to determine resource type
		// Marshal the map to JSON bytes first, then unmarshal to ResourceConfig
		configJSON, err := json.Marshal(r.MCPResourceConfig)
		if err != nil {
			h.logger.Warnf("Failed to marshal resource config for %s: %v", r.MCPResourceName, err)
			continue
		}

		var config ResourceConfig
		if err := json.Unmarshal(configJSON, &config); err != nil {
			h.logger.Warnf("Failed to parse resource config for %s: %v", r.MCPResourceName, err)
			continue
		}

		// Build resource URI
		uri := h.buildResourceURI(r.MCPResourceName, config)

		resources = append(resources, protocol.Resource{
			URI:         uri,
			Name:        r.MCPResourceName,
			Description: r.MCPResourceDescription,
			MimeType:    "application/json",
			Metadata: map[string]interface{}{
				"resource_id": r.MCPResourceID,
				"mapping_id":  r.MappingID,
				"type":        config.Type,
			},
		})
	}

	return &protocol.ListResourcesResult{
		Resources: resources,
	}, nil
}

// Read reads the contents of a resource
func (h *Handler) Read(ctx context.Context, req *protocol.ReadResourceRequest) (*protocol.ReadResourceResult, error) {
	// Get session from context
	session, ok := auth.GetSessionFromContext(ctx)
	if !ok {
		return nil, &protocol.RPCError{
			Code:    protocol.UnauthorizedError,
			Message: "No session in context",
		}
	}

	// Parse URI
	_, config, err := h.parseResourceURI(req.URI)
	if err != nil {
		return nil, &protocol.RPCError{
			Code:    protocol.InvalidParams,
			Message: fmt.Sprintf("Invalid resource URI: %v", err),
		}
	}

	// Load resource from database by matching config
	// We need to find the resource that matches the database_id and table_name in the URI
	rows, err := h.db.Pool().Query(ctx, `
		SELECT r.mcpresource_id, r.mcpresource_name, r.mcpresource_config, r.mapping_id
		FROM mcpresources r
		JOIN mcp_server_resources sr ON sr.mcpresource_id = r.mcpresource_id
		WHERE sr.mcpserver_id = $1 
		  AND r.tenant_id = $2 
		  AND r.workspace_id = $3
	`, h.mcpServerID, session.TenantID, session.WorkspaceID)
	if err != nil {
		h.logger.Errorf("Failed to query resources: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.InternalError,
			Message: "Failed to query resources",
		}
	}
	defer rows.Close()

	// Find the resource that matches the URI config
	var r models.MCPResource
	found := false
	for rows.Next() {
		var tempResource models.MCPResource
		if err := rows.Scan(
			&tempResource.MCPResourceID,
			&tempResource.MCPResourceName,
			&tempResource.MCPResourceConfig,
			&tempResource.MappingID,
		); err != nil {
			continue
		}

		// Parse the stored config
		configJSON, err := json.Marshal(tempResource.MCPResourceConfig)
		if err != nil {
			continue
		}

		var storedConfig ResourceConfig
		if err := json.Unmarshal(configJSON, &storedConfig); err != nil {
			continue
		}

		// Check if this resource matches the requested URI
		if config.Type == "direct_table" {
			if storedConfig.Type == "direct_table" &&
				storedConfig.DatabaseID == config.DatabaseID &&
				storedConfig.TableName == config.TableName {
				r = tempResource
				found = true
				break
			}
		} else if config.Type == "mapped_table" {
			// For mapped tables, the URI contains the mapping name
			// We'd need to match by mapping_id, but for now skip
			continue
		}
	}

	if !found {
		h.logger.Errorf("Failed to load resource: no resource matches URI %s", req.URI)
		return nil, &protocol.RPCError{
			Code:    protocol.ResourceNotFoundError,
			Message: "Resource not found",
		}
	}

	// Check authorization
	opContext := map[string]string{
		"database_id": config.DatabaseID,
		"table_name":  config.TableName,
	}
	if err := h.authMiddleware.AuthorizeOperation(ctx, h.mcpServerID, "resource_read", r.MCPResourceID, opContext); err != nil {
		return nil, &protocol.RPCError{
			Code:    protocol.ForbiddenError,
			Message: "Not authorized to read this resource",
		}
	}

	// Read resource contents based on type
	var contents string
	if config.Type == "direct_table" {
		contents, err = h.readDirectTable(ctx, session, config)
	} else if config.Type == "mapped_table" {
		contents, err = h.readMappedTable(ctx, session, r.MappingID, config)
	} else {
		return nil, &protocol.RPCError{
			Code:    protocol.InvalidParams,
			Message: "Unknown resource type",
		}
	}

	if err != nil {
		h.logger.Errorf("Failed to read resource contents: %v", err)
		return nil, &protocol.RPCError{
			Code:    protocol.InternalError,
			Message: fmt.Sprintf("Failed to read resource: %v", err),
		}
	}

	return &protocol.ReadResourceResult{
		Contents: []protocol.ResourceContents{
			{
				URI:      req.URI,
				MimeType: "application/json",
				Text:     contents,
			},
		},
	}, nil
}

// Subscribe subscribes to resource updates (placeholder)
func (h *Handler) Subscribe(ctx context.Context, req *protocol.SubscribeRequest) error {
	// TODO: Implement resource subscription via CDC/polling
	return nil
}

// Unsubscribe unsubscribes from resource updates (placeholder)
func (h *Handler) Unsubscribe(ctx context.Context, req *protocol.UnsubscribeRequest) error {
	// TODO: Implement unsubscription
	return nil
}

// readDirectTable reads data directly from a database table
func (h *Handler) readDirectTable(ctx context.Context, session *auth.SessionContext, config ResourceConfig) (string, error) {
	// Resolve database identifier (could be ID or name)
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, config.DatabaseID)
	if err != nil {
		return "", fmt.Errorf("failed to resolve database: %w", err)
	}

	// Fetch data via Anchor service
	fetchResp, err := h.anchorClient.FetchData(ctx, &anchorv1.FetchDataRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		TableName:   config.TableName,
		Options:     []byte(`{"limit": 100}`), // Default limit
	})

	if err != nil {
		return "", fmt.Errorf("anchor fetch failed: %w", err)
	}

	if !fetchResp.Success {
		return "", fmt.Errorf("anchor fetch unsuccessful: %s", fetchResp.Message)
	}

	return string(fetchResp.Data), nil
}

// readMappedTable reads data from a mapped/virtual table
func (h *Handler) readMappedTable(ctx context.Context, session *auth.SessionContext, mappingID string, config ResourceConfig) (string, error) {
	// Load mapping definition
	var mapping models.Mapping
	err := h.db.Pool().QueryRow(ctx, `
		SELECT mapping_source_identifier, mapping_target_identifier, mapping_object
		FROM mappings
		WHERE mapping_id = $1 AND tenant_id = $2 AND workspace_id = $3
	`, mappingID, session.TenantID, session.WorkspaceID).Scan(
		&mapping.MappingSourceIdentifier,
		&mapping.MappingTargetIdentifier,
		&mapping.MappingObject,
	)
	if err != nil {
		return "", fmt.Errorf("failed to load mapping: %w", err)
	}

	// For now, treat mapped table as direct fetch with transformations applied
	// In a full implementation, this would:
	// 1. Fetch source data
	// 2. Apply mapping rules/transformations
	// 3. Return transformed data

	// Extract database identifier from mapping source identifier or config
	databaseIdentifier := config.DatabaseID
	if databaseIdentifier == "" {
		databaseIdentifier = mapping.MappingSourceIdentifier
	}

	// Resolve database identifier (could be ID or name)
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseIdentifier)
	if err != nil {
		return "", fmt.Errorf("failed to resolve database: %w", err)
	}

	fetchResp, err := h.anchorClient.FetchData(ctx, &anchorv1.FetchDataRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		TableName:   config.TableName,
		Options:     []byte(`{"limit": 100}`),
	})

	if err != nil {
		return "", fmt.Errorf("anchor fetch failed: %w", err)
	}

	if !fetchResp.Success {
		return "", fmt.Errorf("anchor fetch unsuccessful: %s", fetchResp.Message)
	}

	// TODO: Apply mapping transformations here
	return string(fetchResp.Data), nil
}

// buildResourceURI builds a resource URI from name and config
// Uses database name instead of ID for more user-friendly URIs
func (h *Handler) buildResourceURI(name string, config ResourceConfig) string {
	if config.Type == "direct_table" {
		// Use the database identifier as-is (could be name or ID)
		// The identifier in config.DatabaseID is what was stored
		return fmt.Sprintf("redb://database/%s/table/%s", config.DatabaseID, config.TableName)
	} else if config.Type == "mapped_table" {
		return fmt.Sprintf("redb://mapping/%s", name)
	}
	return fmt.Sprintf("redb://resource/%s", name)
}

// parseResourceURI parses a resource URI into components
// Supports both database names and IDs in the URI
func (h *Handler) parseResourceURI(uri string) (string, ResourceConfig, error) {
	if !strings.HasPrefix(uri, "redb://") {
		return "", ResourceConfig{}, fmt.Errorf("invalid URI scheme")
	}

	uri = strings.TrimPrefix(uri, "redb://")
	parts := strings.Split(uri, "/")

	if len(parts) < 2 {
		return "", ResourceConfig{}, fmt.Errorf("invalid URI format")
	}

	config := ResourceConfig{}

	switch parts[0] {
	case "database":
		if len(parts) != 4 || parts[2] != "table" {
			return "", ResourceConfig{}, fmt.Errorf("invalid database URI format, expected: redb://database/{name_or_id}/table/{table_name}")
		}
		config.Type = "direct_table"
		config.DatabaseID = parts[1] // Can be either database name or ID
		config.TableName = parts[3]
		return parts[3], config, nil

	case "mapping":
		if len(parts) != 2 {
			return "", ResourceConfig{}, fmt.Errorf("invalid mapping URI format, expected: redb://mapping/{mapping_name}")
		}
		config.Type = "mapped_table"
		return parts[1], config, nil

	default:
		return "", ResourceConfig{}, fmt.Errorf("unknown URI type: %s", parts[0])
	}
}

// ResourceConfig represents resource configuration
type ResourceConfig struct {
	Type       string `json:"type"`        // "direct_table" or "mapped_table"
	DatabaseID string `json:"database_id"` // For direct tables (can be ID or name)
	TableName  string `json:"table_name"`  // For direct tables
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

// getDatabaseName retrieves the database name for a given database ID
func (h *Handler) getDatabaseName(ctx context.Context, session *auth.SessionContext, databaseID string) (string, error) {
	var databaseName string
	err := h.db.Pool().QueryRow(ctx, `
		SELECT database_name
		FROM databases
		WHERE database_id = $1
		  AND tenant_id = $2
		  AND workspace_id = $3
	`, databaseID, session.TenantID, session.WorkspaceID).Scan(&databaseName)

	if err != nil {
		// If we can't get the name, return the ID
		return databaseID, nil
	}

	return databaseName, nil
}
