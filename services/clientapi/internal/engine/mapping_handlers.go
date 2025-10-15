package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MappingHandlers contains the mapping endpoint handlers
type MappingHandlers struct {
	engine *Engine
}

// NewMappingHandlers creates a new instance of MappingHandlers
func NewMappingHandlers(engine *Engine) *MappingHandlers {
	return &MappingHandlers{
		engine: engine,
	}
}

// ListMappings handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings
func (mh *MappingHandlers) ListMappings(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("List mappings request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListMappingsRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := mh.engine.mappingClient.ListMappings(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to list mappings")
		return
	}

	// Convert gRPC response to REST response
	mappings := make([]Mapping, len(grpcResp.Mappings))
	for i, mapping := range grpcResp.Mappings {
		mappings[i] = Mapping{
			TenantID:           mapping.TenantId,
			WorkspaceID:        mapping.WorkspaceId,
			MappingID:          mapping.MappingId,
			MappingName:        mapping.MappingName,
			MappingDescription: mapping.MappingDescription,
			MappingType:        mapping.MappingType,
			PolicyID:           mapping.PolicyId,
			OwnerID:            mapping.OwnerId,
			MappingRuleCount:   mapping.MappingRuleCount,
		}
	}

	response := ListMappingsResponse{
		Mappings: mappings,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully listed %d mappings for workspace: %s", len(mappings), workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowMapping handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}
func (mh *MappingHandlers) ShowMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Show mapping request for mapping: %s, workspace: %s, tenant: %s", mappingName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowMappingRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		MappingName:   mappingName,
	}

	grpcResp, err := mh.engine.mappingClient.ShowMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show mapping")
		return
	}

	// Convert gRPC response to REST response
	mapping := MappingWithRules{
		TenantID:           grpcResp.Mapping.TenantId,
		WorkspaceID:        grpcResp.Mapping.WorkspaceId,
		MappingID:          grpcResp.Mapping.MappingId,
		MappingName:        grpcResp.Mapping.MappingName,
		MappingDescription: grpcResp.Mapping.MappingDescription,
		MappingType:        grpcResp.Mapping.MappingType,
		PolicyID:           grpcResp.Mapping.PolicyId,
		OwnerID:            grpcResp.Mapping.OwnerId,
	}

	// Convert mapping rules (always include, even if empty)
	mappingRules := make([]MappingRuleInMapping, len(grpcResp.Mapping.MappingRules))
	for i, rule := range grpcResp.Mapping.MappingRules {
		mappingRules[i] = MappingRuleInMapping{
			MappingRuleID:                    rule.MappingRuleId,
			MappingRuleName:                  rule.MappingRuleName,
			MappingRuleDescription:           rule.MappingRuleDescription,
			MappingRuleMetadata:              mh.parseJSONString(rule.MappingRuleMetadata),
			MappingRuleSource:                rule.MappingRuleSource,
			MappingRuleTarget:                rule.MappingRuleTarget,
			MappingRuleTransformationID:      rule.MappingRuleTransformationId,
			MappingRuleTransformationName:    rule.MappingRuleTransformationName,
			MappingRuleTransformationOptions: rule.MappingRuleTransformationOptions,
		}
	}
	mapping.MappingRules = mappingRules

	response := ShowMappingResponse{
		Mapping: mapping,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed mapping: %s for workspace: %s", mappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// AddMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings
func (mh *MappingHandlers) AddMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse add mapping request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.MappingDescription == "" || req.Scope == "" || req.Source == "" || req.Target == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_name, mapping_description, scope, source, and target are required")
		return
	}

	// Validate scope
	if req.Scope != "database" && req.Scope != "table" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid scope", "scope must be 'database' or 'table'")
		return
	}

	// Check if target is MCP resource
	isMCPTarget := strings.HasPrefix(req.Target, "mcp://")

	// Parse source
	_, sourceTable, err := mh.parseSourceTarget(req.Source)
	if err != nil {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid source format", err.Error())
		return
	}

	// Parse target (skip if MCP resource)
	var targetTable string
	if !isMCPTarget {
		_, targetTable, err = mh.parseSourceTarget(req.Target)
		if err != nil {
			mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid target format", err.Error())
			return
		}

		// Validate scope-specific requirements for database-to-database mappings
		if req.Scope == "table" {
			if sourceTable == "" || targetTable == "" {
				mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid table scope", "table scope requires both source and target to include table names (format: database.table)")
				return
			}
		}
	} else {
		// Validate MCP target format
		mcpResourceName := strings.TrimPrefix(req.Target, "mcp://")
		if mcpResourceName == "" {
			mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid MCP target", "expected format: mcp://resource_name")
			return
		}
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add %s mapping request for mapping: %s, source: %s, target: %s, workspace: %s, tenant: %s",
			req.Scope, req.MappingName, req.Source, req.Target, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Ensure mapping name is unique by checking existing mappings
	uniqueName, err := mh.ensureUniqueMappingName(ctx, profile.TenantId, workspaceName, req.MappingName)
	if err != nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Failed to ensure unique mapping name", err.Error())
		return
	}

	// Call core service gRPC with unified request
	grpcReq := &corev1.AddMappingRequest{
		TenantId:           profile.TenantId,
		WorkspaceName:      workspaceName,
		OwnerId:            profile.UserId,
		MappingName:        uniqueName,
		MappingDescription: req.MappingDescription,
		Scope:              req.Scope,
		Source:             req.Source,
		Target:             req.Target,
	}

	if req.PolicyID != "" {
		grpcReq.PolicyId = &req.PolicyID
	}

	grpcResp, err := mh.engine.mappingClient.AddMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to add mapping")
		return
	}

	// Convert gRPC response to REST response
	mapping := Mapping{
		TenantID:           grpcResp.Mapping.TenantId,
		WorkspaceID:        grpcResp.Mapping.WorkspaceId,
		MappingID:          grpcResp.Mapping.MappingId,
		MappingName:        grpcResp.Mapping.MappingName,
		MappingDescription: grpcResp.Mapping.MappingDescription,
		MappingType:        grpcResp.Mapping.MappingType,
		PolicyID:           grpcResp.Mapping.PolicyId,
		OwnerID:            grpcResp.Mapping.OwnerId,
		MappingRuleCount:   grpcResp.Mapping.MappingRuleCount,
	}

	response := AddMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mapping: mapping,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully added mapping: %s for workspace: %s", req.MappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// AddDatabaseMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/database
func (mh *MappingHandlers) AddDatabaseMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddDatabaseMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse add database mapping request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.MappingDescription == "" || req.MappingSourceDatabaseName == "" || req.MappingTargetDatabaseName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_name, mapping_description, mapping_source_database_name, and mapping_target_database_name are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add database mapping request for mapping: %s, source: %s, target: %s, workspace: %s, tenant: %s",
			req.MappingName, req.MappingSourceDatabaseName, req.MappingTargetDatabaseName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddDatabaseMappingRequest{
		TenantId:                  profile.TenantId,
		WorkspaceName:             workspaceName,
		OwnerId:                   profile.UserId,
		MappingName:               req.MappingName,
		MappingDescription:        req.MappingDescription,
		MappingSourceDatabaseName: req.MappingSourceDatabaseName,
		MappingTargetDatabaseName: req.MappingTargetDatabaseName,
	}

	if req.PolicyID != "" {
		grpcReq.PolicyId = &req.PolicyID
	}

	grpcResp, err := mh.engine.mappingClient.AddDatabaseMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to add database mapping")
		return
	}

	// Convert gRPC response to REST response
	mapping := Mapping{
		TenantID:           grpcResp.Mapping.TenantId,
		WorkspaceID:        grpcResp.Mapping.WorkspaceId,
		MappingID:          grpcResp.Mapping.MappingId,
		MappingName:        grpcResp.Mapping.MappingName,
		MappingDescription: grpcResp.Mapping.MappingDescription,
		MappingType:        grpcResp.Mapping.MappingType,
		PolicyID:           grpcResp.Mapping.PolicyId,
		OwnerID:            grpcResp.Mapping.OwnerId,
		MappingRuleCount:   grpcResp.Mapping.MappingRuleCount,
	}

	response := AddDatabaseMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mapping: mapping,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully added database mapping: %s for workspace: %s", req.MappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// AddTableMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/table
func (mh *MappingHandlers) AddTableMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddTableMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse add table mapping request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.MappingDescription == "" || req.MappingSourceDatabaseName == "" || req.MappingSourceTableName == "" || req.MappingTargetDatabaseName == "" || req.MappingTargetTableName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_name, mapping_description, mapping_source_database_name, mapping_source_table_name, mapping_target_database_name, and mapping_target_table_name are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add table mapping request for mapping: %s, source: %s.%s, target: %s.%s, workspace: %s, tenant: %s",
			req.MappingName, req.MappingSourceDatabaseName, req.MappingSourceTableName, req.MappingTargetDatabaseName, req.MappingTargetTableName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Ensure mapping name is unique by checking existing mappings
	uniqueName, err := mh.ensureUniqueMappingName(ctx, profile.TenantId, workspaceName, req.MappingName)
	if err != nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Failed to ensure unique mapping name", err.Error())
		return
	}

	// Call core service gRPC
	grpcReq := &corev1.AddTableMappingRequest{
		TenantId:                  profile.TenantId,
		WorkspaceName:             workspaceName,
		OwnerId:                   profile.UserId,
		MappingName:               uniqueName,
		MappingDescription:        req.MappingDescription,
		MappingSourceDatabaseName: req.MappingSourceDatabaseName,
		MappingSourceTableName:    req.MappingSourceTableName,
		MappingTargetDatabaseName: req.MappingTargetDatabaseName,
		MappingTargetTableName:    req.MappingTargetTableName,
	}

	if req.PolicyID != "" {
		grpcReq.PolicyId = &req.PolicyID
	}

	grpcResp, err := mh.engine.mappingClient.AddTableMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to add table mapping")
		return
	}

	// Convert gRPC response to REST response
	mapping := Mapping{
		TenantID:           grpcResp.Mapping.TenantId,
		WorkspaceID:        grpcResp.Mapping.WorkspaceId,
		MappingID:          grpcResp.Mapping.MappingId,
		MappingName:        grpcResp.Mapping.MappingName,
		MappingDescription: grpcResp.Mapping.MappingDescription,
		MappingType:        grpcResp.Mapping.MappingType,
		PolicyID:           grpcResp.Mapping.PolicyId,
		OwnerID:            grpcResp.Mapping.OwnerId,
		MappingRuleCount:   grpcResp.Mapping.MappingRuleCount,
	}

	response := AddTableMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mapping: mapping,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully added table mapping: %s for workspace: %s", req.MappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyMapping handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}
func (mh *MappingHandlers) ModifyMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse modify mapping request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Modify mapping request for mapping: %s, workspace: %s, tenant: %s", mappingName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyMappingRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		MappingName:   mappingName,
	}

	if req.MappingNameNew != "" {
		grpcReq.MappingNameNew = &req.MappingNameNew
	}
	if req.MappingDescription != "" {
		grpcReq.MappingDescription = &req.MappingDescription
	}
	if req.PolicyID != "" {
		grpcReq.PolicyId = &req.PolicyID
	}

	grpcResp, err := mh.engine.mappingClient.ModifyMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to modify mapping")
		return
	}

	// Convert gRPC response to REST response
	mapping := Mapping{
		TenantID:           grpcResp.Mapping.TenantId,
		WorkspaceID:        grpcResp.Mapping.WorkspaceId,
		MappingID:          grpcResp.Mapping.MappingId,
		MappingName:        grpcResp.Mapping.MappingName,
		MappingDescription: grpcResp.Mapping.MappingDescription,
		MappingType:        grpcResp.Mapping.MappingType,
		PolicyID:           grpcResp.Mapping.PolicyId,
		OwnerID:            grpcResp.Mapping.OwnerId,
		MappingRuleCount:   grpcResp.Mapping.MappingRuleCount,
	}

	response := ModifyMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mapping: mapping,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully modified mapping: %s for workspace: %s", mappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteMapping handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}
func (mh *MappingHandlers) DeleteMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Delete mapping request for mapping: %s, workspace: %s, tenant: %s", mappingName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteMappingRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		MappingName:   mappingName,
	}

	grpcResp, err := mh.engine.mappingClient.DeleteMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to delete mapping")
		return
	}

	response := DeleteMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully deleted mapping: %s for workspace: %s", mappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ListMappingRules handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/mapping-rules
func (mh *MappingHandlers) ListMappingRules(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("List mapping rules request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListMappingRulesRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := mh.engine.mappingClient.ListMappingRules(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to list mapping rules")
		return
	}

	// Convert gRPC response to REST response
	mappingRules := make([]MappingRule, len(grpcResp.MappingRules))
	for i, rule := range grpcResp.MappingRules {
		mappingRules[i] = MappingRule{
			TenantID:                         rule.TenantId,
			WorkspaceID:                      rule.WorkspaceId,
			MappingRuleID:                    rule.MappingRuleId,
			MappingRuleName:                  rule.MappingRuleName,
			MappingRuleDescription:           rule.MappingRuleDescription,
			MappingRuleMetadata:              mh.parseJSONString(rule.MappingRuleMetadata),
			MappingRuleSource:                rule.MappingRuleSource,
			MappingRuleTarget:                rule.MappingRuleTarget,
			MappingRuleTransformationID:      rule.MappingRuleTransformationId,
			MappingRuleTransformationName:    rule.MappingRuleTransformationName,
			MappingRuleTransformationOptions: rule.MappingRuleTransformationOptions,
			OwnerID:                          rule.OwnerId,
			MappingCount:                     rule.MappingCount,
		}
	}

	response := ListMappingRulesResponse{
		MappingRules: mappingRules,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully listed %d mapping rules for workspace: %s", len(mappingRules), workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowMappingRule handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/mapping-rules/{mapping_rule_name}
func (mh *MappingHandlers) ShowMappingRule(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingRuleName := vars["mapping_rule_name"]

	if tenantURL == "" || workspaceName == "" || mappingRuleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_rule_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Show mapping rule request for rule: %s, workspace: %s, tenant: %s", mappingRuleName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowMappingRuleRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		MappingRuleName: mappingRuleName,
	}

	grpcResp, err := mh.engine.mappingClient.ShowMappingRule(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show mapping rule")
		return
	}

	// Convert gRPC response to REST response
	mappingRule := MappingRule{
		TenantID:                         grpcResp.MappingRule.TenantId,
		WorkspaceID:                      grpcResp.MappingRule.WorkspaceId,
		MappingRuleID:                    grpcResp.MappingRule.MappingRuleId,
		MappingRuleName:                  grpcResp.MappingRule.MappingRuleName,
		MappingRuleDescription:           grpcResp.MappingRule.MappingRuleDescription,
		MappingRuleMetadata:              mh.parseJSONString(grpcResp.MappingRule.MappingRuleMetadata),
		MappingRuleSource:                grpcResp.MappingRule.MappingRuleSource,
		MappingRuleTarget:                grpcResp.MappingRule.MappingRuleTarget,
		MappingRuleTransformationID:      grpcResp.MappingRule.MappingRuleTransformationId,
		MappingRuleTransformationName:    grpcResp.MappingRule.MappingRuleTransformationName,
		MappingRuleTransformationOptions: grpcResp.MappingRule.MappingRuleTransformationOptions,
		OwnerID:                          grpcResp.MappingRule.OwnerId,
		MappingCount:                     grpcResp.MappingRule.MappingCount,
	}

	// Convert mappings (always include, even if empty)
	mappings := make([]Mapping, len(grpcResp.MappingRule.Mappings))
	for i, m := range grpcResp.MappingRule.Mappings {
		mappings[i] = Mapping{
			TenantID:           m.TenantId,
			WorkspaceID:        m.WorkspaceId,
			MappingID:          m.MappingId,
			MappingName:        m.MappingName,
			MappingDescription: m.MappingDescription,
			PolicyID:           m.PolicyId,
			OwnerID:            m.OwnerId,
			MappingRuleCount:   m.MappingRuleCount,
		}
	}
	mappingRule.Mappings = mappings

	response := ShowMappingRuleResponse{
		MappingRule: mappingRule,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed mapping rule: %s for workspace: %s", mappingRuleName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// AddMappingRule handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mapping-rules
func (mh *MappingHandlers) AddMappingRule(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddMappingRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse add mapping rule request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingRuleName == "" || req.MappingRuleDescription == "" || req.MappingRuleSource == "" || req.MappingRuleTarget == "" || req.MappingRuleTransformationName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_rule_name, mapping_rule_description, mapping_rule_source, mapping_rule_target, and mapping_rule_transformation_name are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add mapping rule request for rule: %s, workspace: %s, tenant: %s", req.MappingRuleName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddMappingRuleRequest{
		TenantId:                         profile.TenantId,
		WorkspaceName:                    workspaceName,
		OwnerId:                          profile.UserId,
		MappingRuleName:                  req.MappingRuleName,
		MappingRuleDescription:           req.MappingRuleDescription,
		MappingRuleSource:                req.MappingRuleSource,
		MappingRuleTarget:                req.MappingRuleTarget,
		MappingRuleTransformationName:    req.MappingRuleTransformationName,
		MappingRuleTransformationOptions: req.MappingRuleTransformationOptions,
	}

	grpcResp, err := mh.engine.mappingClient.AddMappingRule(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to add mapping rule")
		return
	}

	// Convert gRPC response to REST response
	mappingRule := MappingRule{
		TenantID:                         grpcResp.MappingRule.TenantId,
		WorkspaceID:                      grpcResp.MappingRule.WorkspaceId,
		MappingRuleID:                    grpcResp.MappingRule.MappingRuleId,
		MappingRuleName:                  grpcResp.MappingRule.MappingRuleName,
		MappingRuleDescription:           grpcResp.MappingRule.MappingRuleDescription,
		MappingRuleMetadata:              mh.parseJSONString(grpcResp.MappingRule.MappingRuleMetadata),
		MappingRuleSource:                grpcResp.MappingRule.MappingRuleSource,
		MappingRuleTarget:                grpcResp.MappingRule.MappingRuleTarget,
		MappingRuleTransformationID:      grpcResp.MappingRule.MappingRuleTransformationId,
		MappingRuleTransformationName:    grpcResp.MappingRule.MappingRuleTransformationName,
		MappingRuleTransformationOptions: grpcResp.MappingRule.MappingRuleTransformationOptions,
		OwnerID:                          grpcResp.MappingRule.OwnerId,
		MappingCount:                     grpcResp.MappingRule.MappingCount,
	}

	response := AddMappingRuleResponse{
		Message:     grpcResp.Message,
		Success:     grpcResp.Success,
		MappingRule: mappingRule,
		Status:      convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully added mapping rule: %s for workspace: %s", req.MappingRuleName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyMappingRule handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/mapping-rules/{mapping_rule_name}
func (mh *MappingHandlers) ModifyMappingRule(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingRuleName := vars["mapping_rule_name"]

	if tenantURL == "" || workspaceName == "" || mappingRuleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_rule_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyMappingRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse modify mapping rule request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Modify mapping rule request for rule: %s, workspace: %s, tenant: %s", mappingRuleName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyMappingRuleRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		MappingRuleName: mappingRuleName,
	}

	if req.MappingRuleNameNew != "" {
		grpcReq.MappingRuleNameNew = &req.MappingRuleNameNew
	}
	if req.MappingRuleDescription != "" {
		grpcReq.MappingRuleDescription = &req.MappingRuleDescription
	}
	if req.MappingRuleSource != "" {
		grpcReq.MappingRuleSource = &req.MappingRuleSource
	}
	if req.MappingRuleTarget != "" {
		grpcReq.MappingRuleTarget = &req.MappingRuleTarget
	}
	if req.MappingRuleTransformationName != "" {
		grpcReq.MappingRuleTransformationName = &req.MappingRuleTransformationName
	}
	if req.MappingRuleTransformationOptions != "" {
		grpcReq.MappingRuleTransformationOptions = &req.MappingRuleTransformationOptions
	}

	grpcResp, err := mh.engine.mappingClient.ModifyMappingRule(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to modify mapping rule")
		return
	}

	// Convert gRPC response to REST response
	mappingRule := MappingRule{
		TenantID:                         grpcResp.MappingRule.TenantId,
		WorkspaceID:                      grpcResp.MappingRule.WorkspaceId,
		MappingRuleID:                    grpcResp.MappingRule.MappingRuleId,
		MappingRuleName:                  grpcResp.MappingRule.MappingRuleName,
		MappingRuleDescription:           grpcResp.MappingRule.MappingRuleDescription,
		MappingRuleMetadata:              mh.parseJSONString(grpcResp.MappingRule.MappingRuleMetadata),
		MappingRuleSource:                grpcResp.MappingRule.MappingRuleSource,
		MappingRuleTarget:                grpcResp.MappingRule.MappingRuleTarget,
		MappingRuleTransformationID:      grpcResp.MappingRule.MappingRuleTransformationId,
		MappingRuleTransformationName:    grpcResp.MappingRule.MappingRuleTransformationName,
		MappingRuleTransformationOptions: grpcResp.MappingRule.MappingRuleTransformationOptions,
		OwnerID:                          grpcResp.MappingRule.OwnerId,
		MappingCount:                     grpcResp.MappingRule.MappingCount,
	}

	response := ModifyMappingRuleResponse{
		Message:     grpcResp.Message,
		Success:     grpcResp.Success,
		MappingRule: mappingRule,
		Status:      convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully modified mapping rule: %s for workspace: %s", mappingRuleName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteMappingRule handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/mapping-rules/{mapping_rule_name}
func (mh *MappingHandlers) DeleteMappingRule(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingRuleName := vars["mapping_rule_name"]

	if tenantURL == "" || workspaceName == "" || mappingRuleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_rule_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Delete mapping rule request for rule: %s, workspace: %s, tenant: %s", mappingRuleName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteMappingRuleRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		MappingRuleName: mappingRuleName,
	}

	grpcResp, err := mh.engine.mappingClient.DeleteMappingRule(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to delete mapping rule")
		return
	}

	response := DeleteMappingRuleResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully deleted mapping rule: %s for workspace: %s", mappingRuleName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// AttachMappingRule handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/attach-rule
func (mh *MappingHandlers) AttachMappingRule(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AttachMappingRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse attach mapping rule request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingRuleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_rule_name is required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Attach mapping rule request for mapping: %s, rule: %s, workspace: %s, tenant: %s", mappingName, req.MappingRuleName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AttachMappingRuleRequest{
		TenantId:         profile.TenantId,
		WorkspaceName:    workspaceName,
		MappingName:      mappingName,
		MappingRuleName:  req.MappingRuleName,
		MappingRuleOrder: req.MappingRuleOrder,
	}

	grpcResp, err := mh.engine.mappingClient.AttachMappingRule(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to attach mapping rule")
		return
	}

	response := AttachMappingRuleResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully attached mapping rule: %s to mapping: %s for workspace: %s", req.MappingRuleName, mappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// DetachMappingRule handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/detach-rule
func (mh *MappingHandlers) DetachMappingRule(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req DetachMappingRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse detach mapping rule request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingRuleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_rule_name is required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Detach mapping rule request for mapping: %s, rule: %s, workspace: %s, tenant: %s", mappingName, req.MappingRuleName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DetachMappingRuleRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		MappingName:     mappingName,
		MappingRuleName: req.MappingRuleName,
	}

	grpcResp, err := mh.engine.mappingClient.DetachMappingRule(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to detach mapping rule")
		return
	}

	response := DetachMappingRuleResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully detached mapping rule: %s from mapping: %s for workspace: %s", req.MappingRuleName, mappingName, workspaceName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

// parseJSONString safely parses a JSON string into an interface{} object
// If the string is empty or invalid JSON, it returns nil
func (mh *MappingHandlers) parseJSONString(jsonStr string) interface{} {
	if jsonStr == "" {
		return nil
	}

	var result interface{}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Warnf("Failed to parse JSON string: %v", err)
		}
		return nil
	}
	return result
}

func (mh *MappingHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			mh.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			mh.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			mh.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			mh.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			mh.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			mh.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		mh.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Errorf("Mapping handler gRPC error: %v", err)
	}
}

func (mh *MappingHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (mh *MappingHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	mh.writeJSONResponse(w, statusCode, response)
}

// parseSourceTarget parses database[.table] format
func (mh *MappingHandlers) parseSourceTarget(input string) (database, table string, err error) {
	if input == "" {
		return "", "", fmt.Errorf("source/target cannot be empty")
	}

	parts := strings.Split(input, ".")
	if len(parts) == 1 {
		// Only database name
		return parts[0], "", nil
	} else if len(parts) == 2 {
		// Database and table name
		return parts[0], parts[1], nil
	} else {
		return "", "", fmt.Errorf("invalid format '%s': expected 'database' or 'database.table'", input)
	}
}

// ensureUniqueMappingName ensures the mapping name is unique by appending a number if needed
func (mh *MappingHandlers) ensureUniqueMappingName(ctx context.Context, tenantID, workspaceName, proposedName string) (string, error) {
	// First, try the proposed name as-is
	if !mh.mappingNameExists(ctx, tenantID, workspaceName, proposedName) {
		return proposedName, nil
	}

	// If it exists, try appending numbers until we find a unique name
	for i := 2; i <= 100; i++ { // Limit to 100 attempts to avoid infinite loops
		candidateName := fmt.Sprintf("%s_%d", proposedName, i)
		if !mh.mappingNameExists(ctx, tenantID, workspaceName, candidateName) {
			if mh.engine.logger != nil {
				mh.engine.logger.Infof("Mapping name '%s' already exists, using '%s' instead", proposedName, candidateName)
			}
			return candidateName, nil
		}
	}

	// If we couldn't find a unique name after 100 attempts, return an error
	return "", fmt.Errorf("could not generate unique mapping name after 100 attempts for base name '%s'", proposedName)
}

// mappingNameExists checks if a mapping with the given name already exists
func (mh *MappingHandlers) mappingNameExists(ctx context.Context, tenantID, workspaceName, mappingName string) bool {
	// Create a gRPC request to list mappings
	grpcReq := &corev1.ListMappingsRequest{
		TenantId:      tenantID,
		WorkspaceName: workspaceName,
	}

	// Call the core service to list mappings
	grpcResp, err := mh.engine.mappingClient.ListMappings(ctx, grpcReq)
	if err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Warnf("Failed to list mappings for uniqueness check: %v", err)
		}
		// If we can't check, assume it doesn't exist to avoid blocking the operation
		return false
	}

	// Check if any existing mapping has the same name
	for _, mapping := range grpcResp.Mappings {
		if mapping.MappingName == mappingName {
			return true
		}
	}

	return false
}

// CopyMappingData handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/copy-data
func (mh *MappingHandlers) CopyMappingData(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req struct {
		BatchSize       int32 `json:"batch_size"`
		ParallelWorkers int32 `json:"parallel_workers"`
		DryRun          bool  `json:"dry_run"`
		Progress        bool  `json:"progress"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse copy mapping data request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Set default values
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}
	if req.ParallelWorkers <= 0 {
		req.ParallelWorkers = 4
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Copy mapping data request for mapping: %s, workspace: %s, tenant: %s, batch_size: %d, parallel_workers: %d, dry_run: %t",
			mappingName, workspaceName, profile.TenantId, req.BatchSize, req.ParallelWorkers, req.DryRun)
	}

	// Create context with timeout (longer timeout for data copying operations)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.CopyMappingDataRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		MappingName:     mappingName,
		BatchSize:       &req.BatchSize,
		ParallelWorkers: &req.ParallelWorkers,
		DryRun:          &req.DryRun,
	}

	// For now, we'll handle this as a simple request-response
	// TODO: Implement streaming response for real-time progress updates
	stream, err := mh.engine.mappingClient.CopyMappingData(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to start data copy")
		return
	}

	// Collect all streaming responses
	var lastResponse *corev1.CopyMappingDataResponse
	var allErrors []string

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			mh.handleGRPCError(w, err, "Error during data copy")
			return
		}

		lastResponse = resp
		allErrors = append(allErrors, resp.Errors...)

		// Log progress if available
		if mh.engine.logger != nil && resp.Status == "progress" {
			mh.engine.logger.Infof("Data copy progress for mapping %s: %d/%d rows processed, current table: %s",
				mappingName, resp.RowsProcessed, resp.TotalRows, resp.CurrentTable)
		}
	}

	if lastResponse == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "No response received from data copy operation", "")
		return
	}

	// Create response
	response := struct {
		Message       string   `json:"message"`
		Success       bool     `json:"success"`
		Status        string   `json:"status"`
		RowsProcessed int64    `json:"rows_processed"`
		TotalRows     int64    `json:"total_rows"`
		CurrentTable  string   `json:"current_table"`
		Errors        []string `json:"errors"`
		OperationID   string   `json:"operation_id"`
	}{
		Message:       lastResponse.Message,
		Success:       lastResponse.Status == "completed",
		Status:        lastResponse.Status,
		RowsProcessed: lastResponse.RowsProcessed,
		TotalRows:     lastResponse.TotalRows,
		CurrentTable:  lastResponse.CurrentTable,
		Errors:        allErrors,
		OperationID:   lastResponse.OperationId,
	}

	statusCode := http.StatusOK
	if !response.Success {
		statusCode = http.StatusInternalServerError
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Data copy operation completed for mapping: %s, success: %t, rows_processed: %d",
			mappingName, response.Success, response.RowsProcessed)
	}

	mh.writeJSONResponse(w, statusCode, response)
}

// AddRuleToMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/rules
func (mh *MappingHandlers) AddRuleToMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddRuleToMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.RuleName == "" || req.Source == "" || req.Target == "" || req.Transformation == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "rule_name, source, target, and transformation are required", "")
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Step 1: Create the mapping rule
	addRuleReq := &corev1.AddMappingRuleRequest{
		TenantId:                         profile.TenantId,
		WorkspaceName:                    workspaceName,
		MappingRuleName:                  req.RuleName,
		MappingRuleDescription:           fmt.Sprintf("Rule for %s mapping", mappingName),
		MappingRuleSource:                req.Source,
		MappingRuleTarget:                req.Target,
		MappingRuleTransformationName:    req.Transformation,
		MappingRuleTransformationOptions: "",
		OwnerId:                          profile.UserId,
	}

	ruleResp, err := mh.engine.mappingClient.AddMappingRule(ctx, addRuleReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to create mapping rule")
		return
	}

	// Step 2: Attach the rule to the mapping
	var order *int64
	if req.Order != nil {
		orderVal := int64(*req.Order)
		order = &orderVal
	}

	attachReq := &corev1.AttachMappingRuleRequest{
		TenantId:         profile.TenantId,
		WorkspaceName:    workspaceName,
		MappingName:      mappingName,
		MappingRuleName:  req.RuleName,
		MappingRuleOrder: order,
	}

	_, err = mh.engine.mappingClient.AttachMappingRule(ctx, attachReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to attach mapping rule to mapping")
		return
	}

	// Convert to REST response
	rule := mh.protoToMappingRule(ruleResp.MappingRule)

	response := AddRuleToMappingResponse{
		Message: "Rule added to mapping successfully",
		Success: true,
		Rule:    rule,
		Status:  convertStatus(ruleResp.Status),
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyRuleInMapping handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/rules/{rule_name}
func (mh *MappingHandlers) ModifyRuleInMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]
	ruleName := vars["rule_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" || ruleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "All path parameters are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyRuleInMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// At least one field must be provided
	if req.Source == nil && req.Target == nil && req.Transformation == nil && req.Order == nil {
		mh.writeErrorResponse(w, http.StatusBadRequest, "At least one field must be provided for modification", "")
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Modify the mapping rule
	modifyReq := &corev1.ModifyMappingRuleRequest{
		TenantId:                      profile.TenantId,
		WorkspaceName:                 workspaceName,
		MappingRuleName:               ruleName,
		MappingRuleSource:             req.Source,
		MappingRuleTarget:             req.Target,
		MappingRuleTransformationName: req.Transformation,
	}

	ruleResp, err := mh.engine.mappingClient.ModifyMappingRule(ctx, modifyReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to modify mapping rule")
		return
	}

	// Convert to REST response
	rule := mh.protoToMappingRule(ruleResp.MappingRule)

	response := ModifyRuleInMappingResponse{
		Message: "Rule modified successfully",
		Success: true,
		Rule:    rule,
		Status:  convertStatus(ruleResp.Status),
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// RemoveRuleFromMapping handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/rules/{rule_name}
func (mh *MappingHandlers) RemoveRuleFromMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]
	ruleName := vars["rule_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" || ruleName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "All path parameters are required", "")
		return
	}

	// Check for delete query parameter
	deleteRule := r.URL.Query().Get("delete") == "true"

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Detach the mapping rule from the mapping
	detachReq := &corev1.DetachMappingRuleRequest{
		TenantId:        profile.TenantId,
		WorkspaceName:   workspaceName,
		MappingName:     mappingName,
		MappingRuleName: ruleName,
	}

	_, err := mh.engine.mappingClient.DetachMappingRule(ctx, detachReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to detach mapping rule from mapping")
		return
	}

	// If delete flag is set, also delete the rule
	if deleteRule {
		deleteReq := &corev1.DeleteMappingRuleRequest{
			TenantId:        profile.TenantId,
			WorkspaceName:   workspaceName,
			MappingRuleName: ruleName,
		}

		_, err := mh.engine.mappingClient.DeleteMappingRule(ctx, deleteReq)
		if err != nil {
			mh.handleGRPCError(w, err, "Failed to delete mapping rule")
			return
		}
	}

	response := RemoveRuleFromMappingResponse{
		Message: "Rule removed from mapping successfully",
		Success: true,
		Status:  Status("STATUS_SUCCESS"),
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ListRulesInMapping handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/{mapping_name}/rules
func (mh *MappingHandlers) ListRulesInMapping(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	mappingName := vars["mapping_name"]

	if tenantURL == "" || workspaceName == "" || mappingName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and mapping_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		mh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Get the mapping with its rules
	showReq := &corev1.ShowMappingRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
		MappingName:   mappingName,
	}

	mappingResp, err := mh.engine.mappingClient.ShowMapping(ctx, showReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to get mapping")
		return
	}

	// Convert rules to response format
	rules := make([]MappingRuleInMapping, 0, len(mappingResp.Mapping.MappingRules))
	for _, protoRule := range mappingResp.Mapping.MappingRules {
		rules = append(rules, mh.protoToMappingRuleInMapping(protoRule))
	}

	response := ListRulesInMappingResponse{
		Rules: rules,
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper function to convert proto MappingRule to REST MappingRule
func (mh *MappingHandlers) protoToMappingRule(proto *corev1.MappingRule) MappingRule {
	var metadata interface{}
	if proto.MappingRuleMetadata != "" {
		// Try to parse as JSON
		var metadataObj map[string]interface{}
		if err := json.Unmarshal([]byte(proto.MappingRuleMetadata), &metadataObj); err == nil {
			metadata = metadataObj
		} else {
			metadata = proto.MappingRuleMetadata
		}
	}

	return MappingRule{
		TenantID:                         proto.TenantId,
		WorkspaceID:                      proto.WorkspaceId,
		MappingRuleID:                    proto.MappingRuleId,
		MappingRuleName:                  proto.MappingRuleName,
		MappingRuleDescription:           proto.MappingRuleDescription,
		MappingRuleMetadata:              metadata,
		MappingRuleSource:                proto.MappingRuleSource,
		MappingRuleTarget:                proto.MappingRuleTarget,
		MappingRuleTransformationID:      proto.MappingRuleTransformationId,
		MappingRuleTransformationName:    proto.MappingRuleTransformationName,
		MappingRuleTransformationOptions: proto.MappingRuleTransformationOptions,
		OwnerID:                          proto.OwnerId,
		MappingCount:                     proto.MappingCount,
	}
}

// Helper function to convert proto MappingRule to REST MappingRuleInMapping
func (mh *MappingHandlers) protoToMappingRuleInMapping(proto *corev1.MappingRule) MappingRuleInMapping {
	var metadata interface{}
	if proto.MappingRuleMetadata != "" {
		// Try to parse as JSON
		var metadataObj map[string]interface{}
		if err := json.Unmarshal([]byte(proto.MappingRuleMetadata), &metadataObj); err == nil {
			metadata = metadataObj
		} else {
			metadata = proto.MappingRuleMetadata
		}
	}

	return MappingRuleInMapping{
		MappingRuleID:                    proto.MappingRuleId,
		MappingRuleName:                  proto.MappingRuleName,
		MappingRuleDescription:           proto.MappingRuleDescription,
		MappingRuleMetadata:              metadata,
		MappingRuleSource:                proto.MappingRuleSource,
		MappingRuleTarget:                proto.MappingRuleTarget,
		MappingRuleTransformationID:      proto.MappingRuleTransformationId,
		MappingRuleTransformationName:    proto.MappingRuleTransformationName,
		MappingRuleTransformationOptions: proto.MappingRuleTransformationOptions,
	}
}
