package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// ============================================================================
// MCP Server Handlers
// ============================================================================

func (s *Server) ListMCPServers(ctx context.Context, req *corev1.ListMCPServersRequest) (*corev1.ListMCPServersResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Query MCP servers
	query := `
		SELECT mcpserver_id, tenant_id, workspace_id, mcpserver_name, mcpserver_description,
		       COALESCE(mcpserver_host_ids, '{}') AS mcpserver_host_ids,
		       mcpserver_port, mcpserver_enabled,
		       COALESCE(policy_ids, '{}') AS policy_ids,
		       owner_id, status_message, status, created, updated
		FROM mcpservers
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY mcpserver_name
	`

	rows, err := s.engine.db.Pool().Query(ctx, query, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to list MCP servers: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to list MCP servers: %v", err)
	}
	defer rows.Close()

	var servers []*corev1.MCPServer
	for rows.Next() {
		var server corev1.MCPServer
		var statusStr string
		var created, updated time.Time

		err := rows.Scan(
			&server.McpServerId,
			&server.TenantId,
			&server.WorkspaceId,
			&server.McpServerName,
			&server.McpServerDescription,
			&server.McpServerHostIds,
			&server.McpServerPort,
			&server.McpServerEnabled,
			&server.PolicyIds,
			&server.OwnerId,
			&server.StatusMessage,
			&statusStr,
			&created,
			&updated,
		)
		if err != nil {
			s.engine.logger.Warnf("Failed to scan MCP server: %v", err)
			continue
		}

		// Convert status string to enum
		server.Status = s.convertStatusStringToEnum(statusStr)

		servers = append(servers, &server)
	}

	return &corev1.ListMCPServersResponse{
		McpServers: servers,
	}, nil
}

func (s *Server) ShowMCPServer(ctx context.Context, req *corev1.ShowMCPServerRequest) (*corev1.ShowMCPServerResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Query MCP server
	query := `
		SELECT mcpserver_id, tenant_id, workspace_id, mcpserver_name, mcpserver_description,
		       COALESCE(mcpserver_host_ids, '{}') AS mcpserver_host_ids,
		       mcpserver_port, mcpserver_enabled,
		       COALESCE(policy_ids, '{}') AS policy_ids,
		       owner_id, status_message, status, created, updated
		FROM mcpservers
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3
	`

	var server corev1.MCPServer
	var statusStr string
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, query, req.TenantId, workspaceID, req.McpServerName).Scan(
		&server.McpServerId,
		&server.TenantId,
		&server.WorkspaceId,
		&server.McpServerName,
		&server.McpServerDescription,
		&server.McpServerHostIds,
		&server.McpServerPort,
		&server.McpServerEnabled,
		&server.PolicyIds,
		&server.OwnerId,
		&server.StatusMessage,
		&statusStr,
		&created,
		&updated,
	)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to get MCP server: %v", err)
		return nil, status.Errorf(codes.NotFound, "MCP server not found: %v", err)
	}

	// Convert status string to enum
	server.Status = s.convertStatusStringToEnum(statusStr)

	return &corev1.ShowMCPServerResponse{
		McpServer: &server,
	}, nil
}

func (s *Server) AddMCPServer(ctx context.Context, req *corev1.AddMCPServerRequest) (*corev1.AddMCPServerResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// If no nodes specified, use local node
	hostIDs := req.McpServerHostIds
	if len(hostIDs) == 0 {
		localNodeID, err := s.getLocalNodeID(ctx)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to get local node ID: %v", err)
		}
		hostIDs = []string{localNodeID}
		s.engine.logger.Infof("No nodes specified, using local node: %s", localNodeID)
	}

	// Insert MCP server
	query := `
		INSERT INTO mcpservers (
			tenant_id, workspace_id, mcpserver_name, mcpserver_description,
			mcpserver_host_ids, mcpserver_port, mcpserver_enabled,
			policy_ids, owner_id, status_message, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING mcpserver_id, created, updated
	`

	var mcpserverID string
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, query,
		req.TenantId,
		workspaceID,
		req.McpServerName,
		req.McpServerDescription,
		hostIDs,
		req.McpServerPort,
		req.McpServerEnabled,
		req.PolicyIds,
		req.OwnerId,
		"MCP server created",
		"STATUS_CREATED",
	).Scan(&mcpserverID, &created, &updated)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to create MCP server: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create MCP server: %v", err)
	}

	s.engine.logger.Infof("Created MCP server %s (ID: %s) on port %d", req.McpServerName, mcpserverID, req.McpServerPort)

	// Return the created server
	server := &corev1.MCPServer{
		McpServerId:          mcpserverID,
		TenantId:             req.TenantId,
		WorkspaceId:          workspaceID,
		McpServerName:        req.McpServerName,
		McpServerDescription: req.McpServerDescription,
		McpServerHostIds:     req.McpServerHostIds,
		McpServerPort:        req.McpServerPort,
		McpServerEnabled:     req.McpServerEnabled,
		PolicyIds:            req.PolicyIds,
		OwnerId:              req.OwnerId,
		StatusMessage:        "MCP server created",
		Status:               commonv1.Status_STATUS_CREATED,
	}

	return &corev1.AddMCPServerResponse{
		Message:   fmt.Sprintf("MCP server '%s' created successfully", req.McpServerName),
		Success:   true,
		McpServer: server,
		Status:    commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyMCPServer(ctx context.Context, req *corev1.ModifyMCPServerRequest) (*corev1.ModifyMCPServerResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Build update query dynamically based on provided fields
	updates := []string{"updated = CURRENT_TIMESTAMP"}
	args := []interface{}{req.TenantId, workspaceID, req.McpServerName}
	paramCount := 4

	if req.McpServerDescription != nil {
		updates = append(updates, fmt.Sprintf("mcpserver_description = $%d", paramCount))
		args = append(args, *req.McpServerDescription)
		paramCount++
	}

	if len(req.McpServerHostIds) > 0 {
		updates = append(updates, fmt.Sprintf("mcpserver_host_ids = $%d", paramCount))
		args = append(args, req.McpServerHostIds)
		paramCount++
	}

	if req.McpServerPort != nil {
		updates = append(updates, fmt.Sprintf("mcpserver_port = $%d", paramCount))
		args = append(args, *req.McpServerPort)
		paramCount++
	}

	if req.McpServerEnabled != nil {
		updates = append(updates, fmt.Sprintf("mcpserver_enabled = $%d", paramCount))
		args = append(args, *req.McpServerEnabled)
		paramCount++
	}

	if len(req.PolicyIds) > 0 {
		updates = append(updates, fmt.Sprintf("policy_ids = $%d", paramCount))
		args = append(args, req.PolicyIds)
		paramCount++
	}

	if len(updates) == 1 {
		return nil, status.Errorf(codes.InvalidArgument, "no fields to update")
	}

	query := fmt.Sprintf(`
		UPDATE mcpservers
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3
	`, updates[0])
	for i := 1; i < len(updates); i++ {
		query = fmt.Sprintf("%s, %s", query, updates[i])
	}

	result, err := s.engine.db.Pool().Exec(ctx, query, args...)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to modify MCP server: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to modify MCP server: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP server not found")
	}

	s.engine.logger.Infof("Modified MCP server %s", req.McpServerName)

	// Get the updated server
	showResp, err := s.ShowMCPServer(ctx, &corev1.ShowMCPServerRequest{
		TenantId:      req.TenantId,
		WorkspaceName: req.WorkspaceName,
		McpServerName: req.McpServerName,
	})
	if err != nil {
		s.engine.IncrementErrors()
		return nil, err
	}

	return &corev1.ModifyMCPServerResponse{
		Message:   fmt.Sprintf("MCP server '%s' modified successfully", req.McpServerName),
		Success:   true,
		McpServer: showResp.McpServer,
		Status:    commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteMCPServer(ctx context.Context, req *corev1.DeleteMCPServerRequest) (*corev1.DeleteMCPServerResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Delete MCP server (cascade deletes will handle junction tables)
	query := `
		DELETE FROM mcpservers
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3
	`

	result, err := s.engine.db.Pool().Exec(ctx, query, req.TenantId, workspaceID, req.McpServerName)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to delete MCP server: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to delete MCP server: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP server not found")
	}

	s.engine.logger.Infof("Deleted MCP server %s", req.McpServerName)

	return &corev1.DeleteMCPServerResponse{
		Message: fmt.Sprintf("MCP server '%s' deleted successfully", req.McpServerName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ============================================================================
// MCP Resource Handlers
// ============================================================================

func (s *Server) ListMCPResources(ctx context.Context, req *corev1.ListMCPResourcesRequest) (*corev1.ListMCPResourcesResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Query MCP resources
	query := `
		SELECT mcpresource_id, tenant_id, workspace_id, mcpresource_name,
		       mcpresource_description, mcpresource_config, mapping_id,
		       COALESCE(policy_ids, '{}') AS policy_ids, owner_id, created, updated
		FROM mcpresources
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY mcpresource_name
	`

	rows, err := s.engine.db.Pool().Query(ctx, query, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to list MCP resources: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to list MCP resources: %v", err)
	}
	defer rows.Close()

	var resources []*corev1.MCPResource
	for rows.Next() {
		var resource corev1.MCPResource
		var configJSON []byte
		var created, updated time.Time

		err := rows.Scan(
			&resource.McpResourceId,
			&resource.TenantId,
			&resource.WorkspaceId,
			&resource.McpResourceName,
			&resource.McpResourceDescription,
			&configJSON,
			&resource.MappingId,
			&resource.PolicyIds,
			&resource.OwnerId,
			&created,
			&updated,
		)
		if err != nil {
			s.engine.logger.Warnf("Failed to scan MCP resource: %v", err)
			continue
		}

		// Convert config JSON to structpb.Struct
		if len(configJSON) > 0 {
			var configMap map[string]interface{}
			if err := json.Unmarshal(configJSON, &configMap); err == nil {
				resource.McpResourceConfig, _ = structpb.NewStruct(configMap)
			}
		}

		resources = append(resources, &resource)
	}

	return &corev1.ListMCPResourcesResponse{
		McpResources: resources,
	}, nil
}

func (s *Server) ShowMCPResource(ctx context.Context, req *corev1.ShowMCPResourceRequest) (*corev1.ShowMCPResourceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Query MCP resource
	query := `
		SELECT mcpresource_id, tenant_id, workspace_id, mcpresource_name,
		       mcpresource_description, mcpresource_config, mapping_id,
		       COALESCE(policy_ids, '{}') AS policy_ids, owner_id, created, updated
		FROM mcpresources
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcpresource_name = $3
	`

	var resource corev1.MCPResource
	var configJSON []byte
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, query, req.TenantId, workspaceID, req.McpResourceName).Scan(
		&resource.McpResourceId,
		&resource.TenantId,
		&resource.WorkspaceId,
		&resource.McpResourceName,
		&resource.McpResourceDescription,
		&configJSON,
		&resource.MappingId,
		&resource.PolicyIds,
		&resource.OwnerId,
		&created,
		&updated,
	)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to get MCP resource: %v", err)
		return nil, status.Errorf(codes.NotFound, "MCP resource not found: %v", err)
	}

	// Convert config JSON to structpb.Struct
	if len(configJSON) > 0 {
		var configMap map[string]interface{}
		if err := json.Unmarshal(configJSON, &configMap); err == nil {
			resource.McpResourceConfig, _ = structpb.NewStruct(configMap)
		}
	}

	return &corev1.ShowMCPResourceResponse{
		McpResource: &resource,
	}, nil
}

func (s *Server) AddMCPResource(ctx context.Context, req *corev1.AddMCPResourceRequest) (*corev1.AddMCPResourceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping details (including source identifier)
	var mappingID string
	var mappingSourceType string
	var mappingSourceIdentifier string
	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mapping_id, mapping_source_type, mapping_source_identifier FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3",
		req.TenantId, workspaceID, req.MappingName).Scan(&mappingID, &mappingSourceType, &mappingSourceIdentifier)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
	}

	// Start with the provided config or an empty map
	configMap := make(map[string]interface{})
	if req.McpResourceConfig != nil {
		configMap = req.McpResourceConfig.AsMap()
	}

	// Auto-populate config from mapping if not already specified
	if _, hasType := configMap["type"]; !hasType {
		configMap["type"] = "direct_table"
	}

	// Parse the source identifier to extract database and table names
	if mappingSourceType == "table" {
		// Format: "database_name.table_name"
		parts := strings.Split(mappingSourceIdentifier, ".")
		if len(parts) == 2 {
			if _, hasDB := configMap["database_id"]; !hasDB {
				configMap["database_id"] = parts[0]
			}
			if _, hasTable := configMap["table_name"]; !hasTable {
				configMap["table_name"] = parts[1]
			}
		}
	} else if mappingSourceType == "database" {
		// Format: "database_name"
		if _, hasDB := configMap["database_id"]; !hasDB {
			configMap["database_id"] = mappingSourceIdentifier
		}
	}

	// Convert to JSON
	configJSON, err := json.Marshal(configMap)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid config: %v", err)
	}

	// Insert MCP resource
	query := `
		INSERT INTO mcpresources (
			tenant_id, workspace_id, mcpresource_name, mcpresource_description,
			mcpresource_config, mapping_id, policy_ids, owner_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING mcpresource_id, created, updated
	`

	var mcpresourceID string
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, query,
		req.TenantId,
		workspaceID,
		req.McpResourceName,
		req.McpResourceDescription,
		configJSON,
		mappingID,
		req.PolicyIds,
		req.OwnerId,
	).Scan(&mcpresourceID, &created, &updated)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to create MCP resource: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create MCP resource: %v", err)
	}

	s.engine.logger.Infof("Created MCP resource %s (ID: %s) with mapping %s", req.McpResourceName, mcpresourceID, req.MappingName)

	// Return the created resource
	resource := &corev1.MCPResource{
		McpResourceId:          mcpresourceID,
		TenantId:               req.TenantId,
		WorkspaceId:            workspaceID,
		McpResourceName:        req.McpResourceName,
		McpResourceDescription: req.McpResourceDescription,
		McpResourceConfig:      req.McpResourceConfig,
		MappingId:              mappingID,
		PolicyIds:              req.PolicyIds,
		OwnerId:                req.OwnerId,
	}

	return &corev1.AddMCPResourceResponse{
		Message:     fmt.Sprintf("MCP resource '%s' created successfully", req.McpResourceName),
		Success:     true,
		McpResource: resource,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyMCPResource(ctx context.Context, req *corev1.ModifyMCPResourceRequest) (*corev1.ModifyMCPResourceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Build update query dynamically
	updates := []string{"updated = CURRENT_TIMESTAMP"}
	args := []interface{}{req.TenantId, workspaceID, req.McpResourceName}
	paramCount := 4

	if req.McpResourceDescription != nil {
		updates = append(updates, fmt.Sprintf("mcpresource_description = $%d", paramCount))
		args = append(args, *req.McpResourceDescription)
		paramCount++
	}

	if req.McpResourceConfig != nil {
		configJSON, err := json.Marshal(req.McpResourceConfig.AsMap())
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "invalid config: %v", err)
		}
		updates = append(updates, fmt.Sprintf("mcpresource_config = $%d", paramCount))
		args = append(args, configJSON)
		paramCount++
	}

	if req.MappingName != nil {
		// Get mapping ID
		var mappingID string
		err := s.engine.db.Pool().QueryRow(ctx,
			"SELECT mapping_id FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3",
			req.TenantId, workspaceID, *req.MappingName).Scan(&mappingID)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
		}
		updates = append(updates, fmt.Sprintf("mapping_id = $%d", paramCount))
		args = append(args, mappingID)
		paramCount++
	}

	if len(req.PolicyIds) > 0 {
		updates = append(updates, fmt.Sprintf("policy_ids = $%d", paramCount))
		args = append(args, req.PolicyIds)
		paramCount++
	}

	if len(updates) == 1 {
		return nil, status.Errorf(codes.InvalidArgument, "no fields to update")
	}

	query := fmt.Sprintf(`
		UPDATE mcpresources
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcpresource_name = $3
	`, updates[0])
	for i := 1; i < len(updates); i++ {
		query = fmt.Sprintf("%s, %s", query, updates[i])
	}

	result, err := s.engine.db.Pool().Exec(ctx, query, args...)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to modify MCP resource: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to modify MCP resource: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP resource not found")
	}

	s.engine.logger.Infof("Modified MCP resource %s", req.McpResourceName)

	// Get the updated resource
	showResp, err := s.ShowMCPResource(ctx, &corev1.ShowMCPResourceRequest{
		TenantId:        req.TenantId,
		WorkspaceName:   req.WorkspaceName,
		McpResourceName: req.McpResourceName,
	})
	if err != nil {
		s.engine.IncrementErrors()
		return nil, err
	}

	return &corev1.ModifyMCPResourceResponse{
		Message:     fmt.Sprintf("MCP resource '%s' modified successfully", req.McpResourceName),
		Success:     true,
		McpResource: showResp.McpResource,
		Status:      commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteMCPResource(ctx context.Context, req *corev1.DeleteMCPResourceRequest) (*corev1.DeleteMCPResourceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Delete MCP resource
	query := `
		DELETE FROM mcpresources
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcpresource_name = $3
	`

	result, err := s.engine.db.Pool().Exec(ctx, query, req.TenantId, workspaceID, req.McpResourceName)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to delete MCP resource: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to delete MCP resource: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP resource not found")
	}

	s.engine.logger.Infof("Deleted MCP resource %s", req.McpResourceName)

	return &corev1.DeleteMCPResourceResponse{
		Message: fmt.Sprintf("MCP resource '%s' deleted successfully", req.McpResourceName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) AttachMCPResource(ctx context.Context, req *corev1.AttachMCPResourceRequest) (*corev1.AttachMCPResourceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get server and resource IDs
	var serverID, resourceID string
	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcpserver_id FROM mcpservers WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3",
		req.TenantId, workspaceID, req.McpServerName).Scan(&serverID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP server not found: %v", err)
	}

	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcpresource_id FROM mcpresources WHERE tenant_id = $1 AND workspace_id = $2 AND mcpresource_name = $3",
		req.TenantId, workspaceID, req.McpResourceName).Scan(&resourceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP resource not found: %v", err)
	}

	// Insert into junction table
	_, err = s.engine.db.Pool().Exec(ctx,
		"INSERT INTO mcp_server_resources (mcpserver_id, mcpresource_id, created) VALUES ($1, $2, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
		serverID, resourceID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to attach MCP resource: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to attach MCP resource: %v", err)
	}

	s.engine.logger.Infof("Attached MCP resource %s to server %s", req.McpResourceName, req.McpServerName)

	return &corev1.AttachMCPResourceResponse{
		Message: fmt.Sprintf("MCP resource '%s' attached to server '%s' successfully", req.McpResourceName, req.McpServerName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DetachMCPResource(ctx context.Context, req *corev1.DetachMCPResourceRequest) (*corev1.DetachMCPResourceResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get server and resource IDs
	var serverID, resourceID string
	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcpserver_id FROM mcpservers WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3",
		req.TenantId, workspaceID, req.McpServerName).Scan(&serverID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP server not found: %v", err)
	}

	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcpresource_id FROM mcpresources WHERE tenant_id = $1 AND workspace_id = $2 AND mcpresource_name = $3",
		req.TenantId, workspaceID, req.McpResourceName).Scan(&resourceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP resource not found: %v", err)
	}

	// Delete from junction table
	result, err := s.engine.db.Pool().Exec(ctx,
		"DELETE FROM mcp_server_resources WHERE mcpserver_id = $1 AND mcpresource_id = $2",
		serverID, resourceID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to detach MCP resource: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to detach MCP resource: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP resource not attached to server")
	}

	s.engine.logger.Infof("Detached MCP resource %s from server %s", req.McpResourceName, req.McpServerName)

	return &corev1.DetachMCPResourceResponse{
		Message: fmt.Sprintf("MCP resource '%s' detached from server '%s' successfully", req.McpResourceName, req.McpServerName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ============================================================================
// MCP Tool Handlers (similar to resources)
// ============================================================================

func (s *Server) ListMCPTools(ctx context.Context, req *corev1.ListMCPToolsRequest) (*corev1.ListMCPToolsResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Query MCP tools
	query := `
		SELECT mcptool_id, tenant_id, workspace_id, mcptool_name,
		       mcptool_description, mcptool_config, mapping_id,
		       COALESCE(policy_ids, '{}') AS policy_ids, owner_id, created, updated
		FROM mcptools
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY mcptool_name
	`

	rows, err := s.engine.db.Pool().Query(ctx, query, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to list MCP tools: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to list MCP tools: %v", err)
	}
	defer rows.Close()

	var tools []*corev1.MCPTool
	for rows.Next() {
		var tool corev1.MCPTool
		var configJSON []byte
		var created, updated time.Time

		err := rows.Scan(
			&tool.McpToolId,
			&tool.TenantId,
			&tool.WorkspaceId,
			&tool.McpToolName,
			&tool.McpToolDescription,
			&configJSON,
			&tool.MappingId,
			&tool.PolicyIds,
			&tool.OwnerId,
			&created,
			&updated,
		)
		if err != nil {
			s.engine.logger.Warnf("Failed to scan MCP tool: %v", err)
			continue
		}

		// Convert config JSON to structpb.Struct
		if len(configJSON) > 0 {
			var configMap map[string]interface{}
			if err := json.Unmarshal(configJSON, &configMap); err == nil {
				tool.McpToolConfig, _ = structpb.NewStruct(configMap)
			}
		}

		tools = append(tools, &tool)
	}

	return &corev1.ListMCPToolsResponse{
		McpTools: tools,
	}, nil
}

func (s *Server) ShowMCPTool(ctx context.Context, req *corev1.ShowMCPToolRequest) (*corev1.ShowMCPToolResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Query MCP tool
	query := `
		SELECT mcptool_id, tenant_id, workspace_id, mcptool_name,
		       mcptool_description, mcptool_config, mapping_id,
		       COALESCE(policy_ids, '{}') AS policy_ids, owner_id, created, updated
		FROM mcptools
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcptool_name = $3
	`

	var tool corev1.MCPTool
	var configJSON []byte
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, query, req.TenantId, workspaceID, req.McpToolName).Scan(
		&tool.McpToolId,
		&tool.TenantId,
		&tool.WorkspaceId,
		&tool.McpToolName,
		&tool.McpToolDescription,
		&configJSON,
		&tool.MappingId,
		&tool.PolicyIds,
		&tool.OwnerId,
		&created,
		&updated,
	)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to get MCP tool: %v", err)
		return nil, status.Errorf(codes.NotFound, "MCP tool not found: %v", err)
	}

	// Convert config JSON to structpb.Struct
	if len(configJSON) > 0 {
		var configMap map[string]interface{}
		if err := json.Unmarshal(configJSON, &configMap); err == nil {
			tool.McpToolConfig, _ = structpb.NewStruct(configMap)
		}
	}

	return &corev1.ShowMCPToolResponse{
		McpTool: &tool,
	}, nil
}

func (s *Server) AddMCPTool(ctx context.Context, req *corev1.AddMCPToolRequest) (*corev1.AddMCPToolResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get mapping details (including source identifier)
	var mappingID string
	var mappingSourceType string
	var mappingSourceIdentifier string
	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mapping_id, mapping_source_type, mapping_source_identifier FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3",
		req.TenantId, workspaceID, req.MappingName).Scan(&mappingID, &mappingSourceType, &mappingSourceIdentifier)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
	}

	// Start with the provided config or an empty map
	configMap := make(map[string]interface{})
	if req.McpToolConfig != nil {
		configMap = req.McpToolConfig.AsMap()
	}

	// Auto-populate config from mapping if not already specified
	if _, hasOp := configMap["operation"]; !hasOp {
		configMap["operation"] = "query_database"
	}

	// Parse the source identifier to extract database and table names
	if mappingSourceType == "table" {
		// Format: "database_name.table_name"
		parts := strings.Split(mappingSourceIdentifier, ".")
		if len(parts) == 2 {
			if _, hasDB := configMap["database_id"]; !hasDB {
				configMap["database_id"] = parts[0]
			}
			if _, hasTable := configMap["table_name"]; !hasTable {
				configMap["table_name"] = parts[1]
			}
		}
	} else if mappingSourceType == "database" {
		// Format: "database_name"
		if _, hasDB := configMap["database_id"]; !hasDB {
			configMap["database_id"] = mappingSourceIdentifier
		}
	}

	// Ensure input_schema exists
	if _, hasSchema := configMap["input_schema"]; !hasSchema {
		configMap["input_schema"] = map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"query": map[string]interface{}{
					"type": "string",
				},
			},
			"required": []string{"query"},
		}
	}

	// Convert to JSON
	configJSON, err := json.Marshal(configMap)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid config: %v", err)
	}

	// Insert MCP tool
	query := `
		INSERT INTO mcptools (
			tenant_id, workspace_id, mcptool_name, mcptool_description,
			mcptool_config, mapping_id, policy_ids, owner_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING mcptool_id, created, updated
	`

	var mcptoolID string
	var created, updated time.Time

	err = s.engine.db.Pool().QueryRow(ctx, query,
		req.TenantId,
		workspaceID,
		req.McpToolName,
		req.McpToolDescription,
		configJSON,
		mappingID,
		req.PolicyIds,
		req.OwnerId,
	).Scan(&mcptoolID, &created, &updated)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to create MCP tool: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to create MCP tool: %v", err)
	}

	s.engine.logger.Infof("Created MCP tool %s (ID: %s) with mapping %s", req.McpToolName, mcptoolID, req.MappingName)

	// Return the created tool
	tool := &corev1.MCPTool{
		McpToolId:          mcptoolID,
		TenantId:           req.TenantId,
		WorkspaceId:        workspaceID,
		McpToolName:        req.McpToolName,
		McpToolDescription: req.McpToolDescription,
		McpToolConfig:      req.McpToolConfig,
		MappingId:          mappingID,
		PolicyIds:          req.PolicyIds,
		OwnerId:            req.OwnerId,
	}

	return &corev1.AddMCPToolResponse{
		Message: fmt.Sprintf("MCP tool '%s' created successfully", req.McpToolName),
		Success: true,
		McpTool: tool,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyMCPTool(ctx context.Context, req *corev1.ModifyMCPToolRequest) (*corev1.ModifyMCPToolResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Build update query dynamically
	updates := []string{"updated = CURRENT_TIMESTAMP"}
	args := []interface{}{req.TenantId, workspaceID, req.McpToolName}
	paramCount := 4

	if req.McpToolDescription != nil {
		updates = append(updates, fmt.Sprintf("mcptool_description = $%d", paramCount))
		args = append(args, *req.McpToolDescription)
		paramCount++
	}

	if req.McpToolConfig != nil {
		configJSON, err := json.Marshal(req.McpToolConfig.AsMap())
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "invalid config: %v", err)
		}
		updates = append(updates, fmt.Sprintf("mcptool_config = $%d", paramCount))
		args = append(args, configJSON)
		paramCount++
	}

	if req.MappingName != nil {
		// Get mapping ID
		var mappingID string
		err := s.engine.db.Pool().QueryRow(ctx,
			"SELECT mapping_id FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_name = $3",
			req.TenantId, workspaceID, *req.MappingName).Scan(&mappingID)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.NotFound, "mapping not found: %v", err)
		}
		updates = append(updates, fmt.Sprintf("mapping_id = $%d", paramCount))
		args = append(args, mappingID)
		paramCount++
	}

	if len(req.PolicyIds) > 0 {
		updates = append(updates, fmt.Sprintf("policy_ids = $%d", paramCount))
		args = append(args, req.PolicyIds)
		paramCount++
	}

	if len(updates) == 1 {
		return nil, status.Errorf(codes.InvalidArgument, "no fields to update")
	}

	query := fmt.Sprintf(`
		UPDATE mcptools
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcptool_name = $3
	`, updates[0])
	for i := 1; i < len(updates); i++ {
		query = fmt.Sprintf("%s, %s", query, updates[i])
	}

	result, err := s.engine.db.Pool().Exec(ctx, query, args...)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to modify MCP tool: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to modify MCP tool: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP tool not found")
	}

	s.engine.logger.Infof("Modified MCP tool %s", req.McpToolName)

	// Get the updated tool
	showResp, err := s.ShowMCPTool(ctx, &corev1.ShowMCPToolRequest{
		TenantId:      req.TenantId,
		WorkspaceName: req.WorkspaceName,
		McpToolName:   req.McpToolName,
	})
	if err != nil {
		s.engine.IncrementErrors()
		return nil, err
	}

	return &corev1.ModifyMCPToolResponse{
		Message: fmt.Sprintf("MCP tool '%s' modified successfully", req.McpToolName),
		Success: true,
		McpTool: showResp.McpTool,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteMCPTool(ctx context.Context, req *corev1.DeleteMCPToolRequest) (*corev1.DeleteMCPToolResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Delete MCP tool
	query := `
		DELETE FROM mcptools
		WHERE tenant_id = $1 AND workspace_id = $2 AND mcptool_name = $3
	`

	result, err := s.engine.db.Pool().Exec(ctx, query, req.TenantId, workspaceID, req.McpToolName)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to delete MCP tool: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to delete MCP tool: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP tool not found")
	}

	s.engine.logger.Infof("Deleted MCP tool %s", req.McpToolName)

	return &corev1.DeleteMCPToolResponse{
		Message: fmt.Sprintf("MCP tool '%s' deleted successfully", req.McpToolName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) AttachMCPTool(ctx context.Context, req *corev1.AttachMCPToolRequest) (*corev1.AttachMCPToolResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get server and tool IDs
	var serverID, toolID string
	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcpserver_id FROM mcpservers WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3",
		req.TenantId, workspaceID, req.McpServerName).Scan(&serverID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP server not found: %v", err)
	}

	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcptool_id FROM mcptools WHERE tenant_id = $1 AND workspace_id = $2 AND mcptool_name = $3",
		req.TenantId, workspaceID, req.McpToolName).Scan(&toolID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP tool not found: %v", err)
	}

	// Insert into junction table
	_, err = s.engine.db.Pool().Exec(ctx,
		"INSERT INTO mcp_server_tools (mcpserver_id, mcptool_id, created) VALUES ($1, $2, CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING",
		serverID, toolID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to attach MCP tool: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to attach MCP tool: %v", err)
	}

	s.engine.logger.Infof("Attached MCP tool %s to server %s", req.McpToolName, req.McpServerName)

	return &corev1.AttachMCPToolResponse{
		Message: fmt.Sprintf("MCP tool '%s' attached to server '%s' successfully", req.McpToolName, req.McpServerName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DetachMCPTool(ctx context.Context, req *corev1.DetachMCPToolRequest) (*corev1.DetachMCPToolResponse, error) {
	defer s.trackOperation()()

	// Get workspace service
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID from workspace name
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get server and tool IDs
	var serverID, toolID string
	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcpserver_id FROM mcpservers WHERE tenant_id = $1 AND workspace_id = $2 AND mcpserver_name = $3",
		req.TenantId, workspaceID, req.McpServerName).Scan(&serverID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP server not found: %v", err)
	}

	err = s.engine.db.Pool().QueryRow(ctx,
		"SELECT mcptool_id FROM mcptools WHERE tenant_id = $1 AND workspace_id = $2 AND mcptool_name = $3",
		req.TenantId, workspaceID, req.McpToolName).Scan(&toolID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "MCP tool not found: %v", err)
	}

	// Delete from junction table
	result, err := s.engine.db.Pool().Exec(ctx,
		"DELETE FROM mcp_server_tools WHERE mcpserver_id = $1 AND mcptool_id = $2",
		serverID, toolID)
	if err != nil {
		s.engine.IncrementErrors()
		s.engine.logger.Errorf("Failed to detach MCP tool: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to detach MCP tool: %v", err)
	}

	if result.RowsAffected() == 0 {
		return nil, status.Errorf(codes.NotFound, "MCP tool not attached to server")
	}

	s.engine.logger.Infof("Detached MCP tool %s from server %s", req.McpToolName, req.McpServerName)

	return &corev1.DetachMCPToolResponse{
		Message: fmt.Sprintf("MCP tool '%s' detached from server '%s' successfully", req.McpToolName, req.McpServerName),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ============================================================================
// MCP Prompt Handlers (stubs for future implementation)
// ============================================================================

func (s *Server) ListMCPPrompts(ctx context.Context, req *corev1.ListMCPPromptsRequest) (*corev1.ListMCPPromptsResponse, error) {
	defer s.trackOperation()()
	return &corev1.ListMCPPromptsResponse{McpPrompts: []*corev1.MCPPrompt{}}, nil
}

func (s *Server) ShowMCPPrompt(ctx context.Context, req *corev1.ShowMCPPromptRequest) (*corev1.ShowMCPPromptResponse, error) {
	defer s.trackOperation()()
	return nil, status.Errorf(codes.Unimplemented, "prompts not yet implemented")
}

func (s *Server) AddMCPPrompt(ctx context.Context, req *corev1.AddMCPPromptRequest) (*corev1.AddMCPPromptResponse, error) {
	defer s.trackOperation()()
	return nil, status.Errorf(codes.Unimplemented, "prompts not yet implemented")
}

func (s *Server) ModifyMCPPrompt(ctx context.Context, req *corev1.ModifyMCPPromptRequest) (*corev1.ModifyMCPPromptResponse, error) {
	defer s.trackOperation()()
	return nil, status.Errorf(codes.Unimplemented, "prompts not yet implemented")
}

func (s *Server) DeleteMCPPrompt(ctx context.Context, req *corev1.DeleteMCPPromptRequest) (*corev1.DeleteMCPPromptResponse, error) {
	defer s.trackOperation()()
	return nil, status.Errorf(codes.Unimplemented, "prompts not yet implemented")
}

func (s *Server) AttachMCPPrompt(ctx context.Context, req *corev1.AttachMCPPromptRequest) (*corev1.AttachMCPPromptResponse, error) {
	defer s.trackOperation()()
	return nil, status.Errorf(codes.Unimplemented, "prompts not yet implemented")
}

func (s *Server) DetachMCPPrompt(ctx context.Context, req *corev1.DetachMCPPromptRequest) (*corev1.DetachMCPPromptResponse, error) {
	defer s.trackOperation()()
	return nil, status.Errorf(codes.Unimplemented, "prompts not yet implemented")
}

// ============================================================================
// Helper Methods
// ============================================================================

func (s *Server) convertStatusStringToEnum(statusStr string) commonv1.Status {
	switch statusStr {
	case "STATUS_HEALTHY":
		return commonv1.Status_STATUS_HEALTHY
	case "STATUS_CREATED":
		return commonv1.Status_STATUS_CREATED
	case "STATUS_STARTED":
		return commonv1.Status_STATUS_STARTED
	case "STATUS_STOPPED":
		return commonv1.Status_STATUS_STOPPED
	case "STATUS_ERROR":
		return commonv1.Status_STATUS_ERROR
	default:
		return commonv1.Status_STATUS_UNKNOWN
	}
}

// getLocalNodeID retrieves the local node identity ID from the localidentity table
func (s *Server) getLocalNodeID(ctx context.Context) (string, error) {
	var identityID string
	query := `SELECT identity_id FROM localidentity LIMIT 1`

	err := s.engine.db.Pool().QueryRow(ctx, query).Scan(&identityID)
	if err != nil {
		return "", fmt.Errorf("failed to query local identity: %v", err)
	}

	if identityID == "" {
		return "", fmt.Errorf("local identity ID is empty")
	}

	return identityID, nil
}
