package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	securityv1 "github.com/redbco/redb-open/api/proto/security/v1"
)

// AddStreamToTableMappingRequest represents the REST request for creating a stream-to-table mapping
type AddStreamToTableMappingRequest struct {
	MappingName           string                   `json:"mapping_name"`
	MappingDescription    string                   `json:"mapping_description"`
	SourceIntegrationName string                   `json:"source_integration_name"`
	SourceTopicName       string                   `json:"source_topic_name"`
	TargetDatabaseName    string                   `json:"target_database_name"`
	TargetTableName       string                   `json:"target_table_name"`
	PolicyID              string                   `json:"policy_id,omitempty"`
	Filters               []StreamMappingFilterReq `json:"filters,omitempty"`
}

// AddTableToStreamMappingRequest represents the REST request for creating a table-to-stream mapping
type AddTableToStreamMappingRequest struct {
	MappingName           string                   `json:"mapping_name"`
	MappingDescription    string                   `json:"mapping_description"`
	SourceDatabaseName    string                   `json:"source_database_name"`
	SourceTableName       string                   `json:"source_table_name"`
	TargetIntegrationName string                   `json:"target_integration_name"`
	TargetTopicName       string                   `json:"target_topic_name"`
	PolicyID              string                   `json:"policy_id,omitempty"`
	Filters               []StreamMappingFilterReq `json:"filters,omitempty"`
}

// AddStreamToStreamMappingRequest represents the REST request for creating a stream-to-stream mapping
type AddStreamToStreamMappingRequest struct {
	MappingName           string                   `json:"mapping_name"`
	MappingDescription    string                   `json:"mapping_description"`
	SourceIntegrationName string                   `json:"source_integration_name"`
	SourceTopicName       string                   `json:"source_topic_name"`
	TargetIntegrationName string                   `json:"target_integration_name"`
	TargetTopicName       string                   `json:"target_topic_name"`
	PolicyID              string                   `json:"policy_id,omitempty"`
	Filters               []StreamMappingFilterReq `json:"filters,omitempty"`
}

// StreamMappingFilterReq represents a stream mapping filter in REST requests
type StreamMappingFilterReq struct {
	FilterType       string                 `json:"filter_type"`
	FilterExpression map[string]interface{} `json:"filter_expression"`
	FilterOrder      int32                  `json:"filter_order"`
	FilterOperator   string                 `json:"filter_operator"`
}

// AddStreamToTableMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/stream-to-table
func (mh *MappingHandlers) AddStreamToTableMapping(w http.ResponseWriter, r *http.Request) {
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
	var req AddStreamToTableMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse stream-to-table mapping request: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.MappingDescription == "" || req.SourceIntegrationName == "" ||
		req.SourceTopicName == "" || req.TargetDatabaseName == "" || req.TargetTableName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing",
			"mapping_name, mapping_description, source_integration_name, source_topic_name, target_database_name, and target_table_name are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add stream-to-table mapping: %s, source: %s/%s, target: %s.%s, workspace: %s",
			req.MappingName, req.SourceIntegrationName, req.SourceTopicName,
			req.TargetDatabaseName, req.TargetTableName, workspaceName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Convert filters to protobuf format
	protoFilters := make([]*corev1.StreamMappingFilter, len(req.Filters))
	for i, filter := range req.Filters {
		protoFilters[i] = &corev1.StreamMappingFilter{
			FilterType:       filter.FilterType,
			FilterExpression: convertMapToStruct(filter.FilterExpression),
			FilterOrder:      filter.FilterOrder,
			FilterOperator:   filter.FilterOperator,
		}
	}

	// Convert PolicyID to pointer if not empty
	var policyIDPtr *string
	if req.PolicyID != "" {
		policyIDPtr = &req.PolicyID
	}

	// Call core service gRPC
	grpcReq := &corev1.AddStreamToTableMappingRequest{
		TenantId:              profile.TenantId,
		WorkspaceName:         workspaceName,
		MappingName:           req.MappingName,
		MappingDescription:    req.MappingDescription,
		SourceIntegrationName: req.SourceIntegrationName,
		SourceTopicName:       req.SourceTopicName,
		TargetDatabaseName:    req.TargetDatabaseName,
		TargetTableName:       req.TargetTableName,
		PolicyId:              policyIDPtr,
		OwnerId:               profile.UserId,
		Filters:               protoFilters,
	}

	grpcResp, err := mh.engine.mappingClient.AddStreamToTableMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to create stream-to-table mapping")
		return
	}

	// Convert gRPC response to REST response
	response := AddMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  "success",
	}

	if grpcResp.Mapping != nil {
		mappingPtr := convertMappingProtoToRest(grpcResp.Mapping)
		if mappingPtr != nil {
			response.Mapping = *mappingPtr
		}
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// AddTableToStreamMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/table-to-stream
func (mh *MappingHandlers) AddTableToStreamMapping(w http.ResponseWriter, r *http.Request) {
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
	var req AddTableToStreamMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse table-to-stream mapping request: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.MappingDescription == "" || req.SourceDatabaseName == "" ||
		req.SourceTableName == "" || req.TargetIntegrationName == "" || req.TargetTopicName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing",
			"mapping_name, mapping_description, source_database_name, source_table_name, target_integration_name, and target_topic_name are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add table-to-stream mapping: %s, source: %s.%s, target: %s/%s, workspace: %s",
			req.MappingName, req.SourceDatabaseName, req.SourceTableName,
			req.TargetIntegrationName, req.TargetTopicName, workspaceName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Convert filters to protobuf format
	protoFilters := make([]*corev1.StreamMappingFilter, len(req.Filters))
	for i, filter := range req.Filters {
		protoFilters[i] = &corev1.StreamMappingFilter{
			FilterType:       filter.FilterType,
			FilterExpression: convertMapToStruct(filter.FilterExpression),
			FilterOrder:      filter.FilterOrder,
			FilterOperator:   filter.FilterOperator,
		}
	}

	// Convert PolicyID to pointer if not empty
	var policyIDPtr *string
	if req.PolicyID != "" {
		policyIDPtr = &req.PolicyID
	}

	// Call core service gRPC
	grpcReq := &corev1.AddTableToStreamMappingRequest{
		TenantId:              profile.TenantId,
		WorkspaceName:         workspaceName,
		MappingName:           req.MappingName,
		MappingDescription:    req.MappingDescription,
		SourceDatabaseName:    req.SourceDatabaseName,
		SourceTableName:       req.SourceTableName,
		TargetIntegrationName: req.TargetIntegrationName,
		TargetTopicName:       req.TargetTopicName,
		PolicyId:              policyIDPtr,
		OwnerId:               profile.UserId,
		Filters:               protoFilters,
	}

	grpcResp, err := mh.engine.mappingClient.AddTableToStreamMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to create table-to-stream mapping")
		return
	}

	// Convert gRPC response to REST response
	response := AddMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  "success",
	}

	if grpcResp.Mapping != nil {
		mappingPtr := convertMappingProtoToRest(grpcResp.Mapping)
		if mappingPtr != nil {
			response.Mapping = *mappingPtr
		}
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// AddStreamToStreamMapping handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/mappings/stream-to-stream
func (mh *MappingHandlers) AddStreamToStreamMapping(w http.ResponseWriter, r *http.Request) {
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
	var req AddStreamToStreamMappingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse stream-to-stream mapping request: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MappingName == "" || req.MappingDescription == "" || req.SourceIntegrationName == "" ||
		req.SourceTopicName == "" || req.TargetIntegrationName == "" || req.TargetTopicName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing",
			"mapping_name, mapping_description, source_integration_name, source_topic_name, target_integration_name, and target_topic_name are required")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add stream-to-stream mapping: %s, source: %s/%s, target: %s/%s, workspace: %s",
			req.MappingName, req.SourceIntegrationName, req.SourceTopicName,
			req.TargetIntegrationName, req.TargetTopicName, workspaceName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Convert filters to protobuf format
	protoFilters := make([]*corev1.StreamMappingFilter, len(req.Filters))
	for i, filter := range req.Filters {
		protoFilters[i] = &corev1.StreamMappingFilter{
			FilterType:       filter.FilterType,
			FilterExpression: convertMapToStruct(filter.FilterExpression),
			FilterOrder:      filter.FilterOrder,
			FilterOperator:   filter.FilterOperator,
		}
	}

	// Convert PolicyID to pointer if not empty
	var policyIDPtr *string
	if req.PolicyID != "" {
		policyIDPtr = &req.PolicyID
	}

	// Call core service gRPC
	grpcReq := &corev1.AddStreamToStreamMappingRequest{
		TenantId:              profile.TenantId,
		WorkspaceName:         workspaceName,
		MappingName:           req.MappingName,
		MappingDescription:    req.MappingDescription,
		SourceIntegrationName: req.SourceIntegrationName,
		SourceTopicName:       req.SourceTopicName,
		TargetIntegrationName: req.TargetIntegrationName,
		TargetTopicName:       req.TargetTopicName,
		PolicyId:              policyIDPtr,
		OwnerId:               profile.UserId,
		Filters:               protoFilters,
	}

	grpcResp, err := mh.engine.mappingClient.AddStreamToStreamMapping(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to create stream-to-stream mapping")
		return
	}

	// Convert gRPC response to REST response
	response := AddMappingResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  "success",
	}

	if grpcResp.Mapping != nil {
		mappingPtr := convertMappingProtoToRest(grpcResp.Mapping)
		if mappingPtr != nil {
			response.Mapping = *mappingPtr
		}
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// convertMappingProtoToRest is a helper function to convert proto Mapping to REST Mapping
func convertMappingProtoToRest(protoMapping *corev1.Mapping) *Mapping {
	if protoMapping == nil {
		return nil
	}

	return &Mapping{
		TenantID:           protoMapping.TenantId,
		WorkspaceID:        protoMapping.WorkspaceId,
		MappingID:          protoMapping.MappingId,
		MappingName:        protoMapping.MappingName,
		MappingDescription: protoMapping.MappingDescription,
		MappingType:        protoMapping.MappingType,
		OwnerID:            protoMapping.OwnerId,
	}
}
