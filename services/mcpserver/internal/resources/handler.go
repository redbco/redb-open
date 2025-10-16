package resources

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

// Handler handles MCP resource operations
type Handler struct {
	logger         *logger.Logger
	db             *database.PostgreSQL
	anchorClient   anchorv1.AnchorServiceClient
	authMiddleware *auth.Middleware
	mcpServerID    string
	config         *config.Config // Global config for service discovery
}

// NewHandler creates a new resource handler
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
			// For mapped tables, match by resource name
			if storedConfig.Type == "mapped_table" && tempResource.MCPResourceName == config.ResourceName {
				r = tempResource
				found = true
				break
			}
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
	var mappingType string
	err := h.db.Pool().QueryRow(ctx, `
		SELECT mapping_source_identifier, mapping_target_identifier, mapping_type
		FROM mappings
		WHERE mapping_id = $1 AND tenant_id = $2 AND workspace_id = $3
	`, mappingID, session.TenantID, session.WorkspaceID).Scan(
		&mapping.MappingSourceIdentifier,
		&mapping.MappingTargetIdentifier,
		&mappingType,
	)
	if err != nil {
		return "", fmt.Errorf("failed to load mapping: %w", err)
	}

	// Parse source identifier to extract database name and table name
	// For table-scope mappings, format is "database_name.table_name"
	var databaseName, tableName string
	if mappingType == "table" {
		parts := strings.Split(mapping.MappingSourceIdentifier, ".")
		if len(parts) == 2 {
			databaseName = parts[0]
			tableName = parts[1]
		} else {
			return "", fmt.Errorf("invalid source identifier format for table mapping: %s", mapping.MappingSourceIdentifier)
		}
	} else {
		return "", fmt.Errorf("only table-scope mappings are supported for MCP resources")
	}

	// Resolve database identifier (could be ID or name)
	databaseID, err := h.resolveDatabaseIdentifier(ctx, session, databaseName)
	if err != nil {
		return "", fmt.Errorf("failed to resolve database: %w", err)
	}

	// Fetch data from source table
	fetchResp, err := h.anchorClient.FetchData(ctx, &anchorv1.FetchDataRequest{
		TenantId:    session.TenantID,
		WorkspaceId: session.WorkspaceID,
		DatabaseId:  databaseID,
		TableName:   tableName,
		Options:     []byte(`{"limit": 100}`),
	})

	if err != nil {
		return "", fmt.Errorf("anchor fetch failed: %w", err)
	}

	if !fetchResp.Success {
		return "", fmt.Errorf("anchor fetch unsuccessful: %s", fetchResp.Message)
	}

	// Load mapping rules for transformation
	mappingRules, err := h.loadMappingRules(ctx, session, mappingID)
	if err != nil {
		h.logger.Warnf("Failed to load mapping rules: %v", err)
		// Return untransformed data if we can't load rules
		return string(fetchResp.Data), nil
	}

	// Apply transformations if rules exist
	if len(mappingRules) > 0 {
		transformedData, err := h.applyMappingTransformations(ctx, session, fetchResp.Data, mappingRules)
		if err != nil {
			h.logger.Warnf("Failed to apply transformations: %v", err)
			// Fall back to untransformed data if transformation fails
			return string(fetchResp.Data), nil
		}
		return string(transformedData), nil
	}

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
			return "", ResourceConfig{}, fmt.Errorf("invalid mapping URI format, expected: redb://mapping/{resource_name}")
		}
		config.Type = "mapped_table"
		config.ResourceName = parts[1] // This is the resource name, not mapping name
		return parts[1], config, nil

	default:
		return "", ResourceConfig{}, fmt.Errorf("unknown URI type: %s", parts[0])
	}
}

// ResourceConfig represents resource configuration
type ResourceConfig struct {
	Type         string `json:"type"`          // "direct_table" or "mapped_table"
	DatabaseID   string `json:"database_id"`   // For direct tables (can be ID or name)
	TableName    string `json:"table_name"`    // For direct tables
	ResourceName string `json:"resource_name"` // For mapped tables (the resource name from URI)
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
