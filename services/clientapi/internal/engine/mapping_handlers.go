package engine

import (
	"context"
	"encoding/json"
	"net/http"
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
	if req.MappingName == "" || req.MappingDescription == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "mapping_name and mapping_description are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add mapping request for mapping: %s, workspace: %s, tenant: %s", req.MappingName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddEmptyMappingRequest{
		TenantId:           profile.TenantId,
		WorkspaceName:      workspaceName,
		OwnerId:            profile.UserId,
		MappingName:        req.MappingName,
		MappingDescription: req.MappingDescription,
	}

	if req.PolicyID != "" {
		grpcReq.PolicyId = &req.PolicyID
	}

	grpcResp, err := mh.engine.mappingClient.AddEmptyMapping(ctx, grpcReq)
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

	// Call core service gRPC
	grpcReq := &corev1.AddTableMappingRequest{
		TenantId:                  profile.TenantId,
		WorkspaceName:             workspaceName,
		OwnerId:                   profile.UserId,
		MappingName:               req.MappingName,
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
