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

// RelationshipHandlers contains the relationship endpoint handlers
type RelationshipHandlers struct {
	engine *Engine
}

// NewRelationshipHandlers creates a new instance of RelationshipHandlers
func NewRelationshipHandlers(engine *Engine) *RelationshipHandlers {
	return &RelationshipHandlers{
		engine: engine,
	}
}

// ListRelationships handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/relationships
func (rh *RelationshipHandlers) ListRelationships(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if workspaceName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "workspace_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("List relationships request for workspace: %s, tenant: %s, user: %s", workspaceName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListRelationshipsRequest{
		TenantId:      profile.TenantId,
		WorkspaceName: workspaceName,
	}

	grpcResp, err := rh.engine.relationshipClient.ListRelationships(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to list relationships")
		return
	}

	// Convert gRPC response to REST response
	relationships := make([]Relationship, len(grpcResp.Relationships))
	for i, relationship := range grpcResp.Relationships {
		relationships[i] = Relationship{
			TenantID:                     relationship.TenantId,
			WorkspaceID:                  relationship.WorkspaceId,
			RelationshipID:               relationship.RelationshipId,
			RelationshipName:             relationship.RelationshipName,
			RelationshipDescription:      relationship.RelationshipDescription,
			RelationshipType:             relationship.RelationshipType,
			RelationshipSourceType:       "table", // Default value since not in proto
			RelationshipTargetType:       "table", // Default value since not in proto
			RelationshipSourceDatabaseID: relationship.RelationshipSourceDatabaseId,
			RelationshipSourceTableName:  relationship.RelationshipSourceTableName,
			RelationshipTargetDatabaseID: relationship.RelationshipTargetDatabaseId,
			RelationshipTargetTableName:  relationship.RelationshipTargetTableName,
			MappingID:                    relationship.MappingId,
			PolicyID:                     relationship.PolicyId,
			StatusMessage:                relationship.StatusMessage,
			Status:                       convertStatus(relationship.Status),
			OwnerID:                      relationship.OwnerId,
		}
	}

	response := ListRelationshipsResponse{
		Relationships: relationships,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully listed %d relationships for workspace: %s", len(relationships), workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowRelationship handles GET /{tenant_url}/api/v1/workspaces/{workspace_name}/relationships/{relationship_id}
func (rh *RelationshipHandlers) ShowRelationship(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	relationshipName := vars["relationship_name"]

	if tenantURL == "" || workspaceName == "" || relationshipName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and relationship_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Show relationship request for relationship: %s, workspace: %s, tenant: %s", relationshipName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowRelationshipRequest{
		TenantId:         profile.TenantId,
		WorkspaceName:    workspaceName,
		RelationshipName: relationshipName,
	}

	grpcResp, err := rh.engine.relationshipClient.ShowRelationship(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to show relationship")
		return
	}

	// Convert gRPC response to REST response
	relationship := Relationship{
		TenantID:                     grpcResp.Relationship.TenantId,
		WorkspaceID:                  grpcResp.Relationship.WorkspaceId,
		RelationshipID:               grpcResp.Relationship.RelationshipId,
		RelationshipName:             grpcResp.Relationship.RelationshipName,
		RelationshipDescription:      grpcResp.Relationship.RelationshipDescription,
		RelationshipType:             grpcResp.Relationship.RelationshipType,
		RelationshipSourceType:       "table", // Default value since not in proto
		RelationshipTargetType:       "table", // Default value since not in proto
		RelationshipSourceDatabaseID: grpcResp.Relationship.RelationshipSourceDatabaseId,
		RelationshipSourceTableName:  grpcResp.Relationship.RelationshipSourceTableName,
		RelationshipTargetDatabaseID: grpcResp.Relationship.RelationshipTargetDatabaseId,
		RelationshipTargetTableName:  grpcResp.Relationship.RelationshipTargetTableName,
		MappingID:                    grpcResp.Relationship.MappingId,
		PolicyID:                     grpcResp.Relationship.PolicyId,
		StatusMessage:                grpcResp.Relationship.StatusMessage,
		Status:                       convertStatus(grpcResp.Relationship.Status),
		OwnerID:                      grpcResp.Relationship.OwnerId,
	}

	response := ShowRelationshipResponse{
		Relationship: relationship,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully showed relationship: %s for workspace: %s", relationshipName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// AddRelationship handles POST /{tenant_url}/api/v1/workspaces/{workspace_name}/relationships
func (rh *RelationshipHandlers) AddRelationship(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]

	if tenantURL == "" || workspaceName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url and workspace_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddRelationshipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse add relationship request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.RelationshipName == "" || req.RelationshipDescription == "" || req.RelationshipType == "" || req.RelationshipSourceDatabaseID == "" || req.RelationshipSourceTableName == "" || req.RelationshipTargetDatabaseID == "" || req.RelationshipTargetTableName == "" || req.MappingID == "" || req.PolicyID == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "relationship_name, relationship_description, relationship_type, relationship_source_database_id, relationship_source_table_name, relationship_target_database_id, relationship_target_table_name, mapping_id, and policy_id are required")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Add relationship request for relationship: %s, workspace: %s, tenant: %s", req.RelationshipName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddRelationshipRequest{
		TenantId:                     profile.TenantId,
		WorkspaceName:                workspaceName,
		OwnerId:                      profile.UserId,
		RelationshipName:             req.RelationshipName,
		RelationshipDescription:      req.RelationshipDescription,
		RelationshipType:             req.RelationshipType,
		RelationshipSourceDatabaseId: req.RelationshipSourceDatabaseID,
		RelationshipSourceTableName:  req.RelationshipSourceTableName,
		RelationshipTargetDatabaseId: req.RelationshipTargetDatabaseID,
		RelationshipTargetTableName:  req.RelationshipTargetTableName,
		MappingId:                    req.MappingID,
		PolicyId:                     req.PolicyID,
	}

	grpcResp, err := rh.engine.relationshipClient.AddRelationship(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to add relationship")
		return
	}

	// Convert gRPC response to REST response
	relationship := Relationship{
		TenantID:                     grpcResp.Relationship.TenantId,
		WorkspaceID:                  grpcResp.Relationship.WorkspaceId,
		RelationshipID:               grpcResp.Relationship.RelationshipId,
		RelationshipName:             grpcResp.Relationship.RelationshipName,
		RelationshipDescription:      grpcResp.Relationship.RelationshipDescription,
		RelationshipType:             grpcResp.Relationship.RelationshipType,
		RelationshipSourceType:       "table", // Default value since not in proto
		RelationshipTargetType:       "table", // Default value since not in proto
		RelationshipSourceDatabaseID: grpcResp.Relationship.RelationshipSourceDatabaseId,
		RelationshipSourceTableName:  grpcResp.Relationship.RelationshipSourceTableName,
		RelationshipTargetDatabaseID: grpcResp.Relationship.RelationshipTargetDatabaseId,
		RelationshipTargetTableName:  grpcResp.Relationship.RelationshipTargetTableName,
		MappingID:                    grpcResp.Relationship.MappingId,
		PolicyID:                     grpcResp.Relationship.PolicyId,
		StatusMessage:                grpcResp.Relationship.StatusMessage,
		Status:                       convertStatus(grpcResp.Relationship.Status),
		OwnerID:                      grpcResp.Relationship.OwnerId,
	}

	response := AddRelationshipResponse{
		Message:      grpcResp.Message,
		Success:      grpcResp.Success,
		Relationship: relationship,
		Status:       convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully added relationship: %s for workspace: %s", req.RelationshipName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyRelationship handles PUT /{tenant_url}/api/v1/workspaces/{workspace_name}/relationships/{relationship_id}
func (rh *RelationshipHandlers) ModifyRelationship(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	relationshipName := vars["relationship_name"]

	if tenantURL == "" || workspaceName == "" || relationshipName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and relationship_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyRelationshipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse modify relationship request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Modify relationship request for relationship: %s, workspace: %s, tenant: %s", relationshipName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyRelationshipRequest{
		TenantId:                     profile.TenantId,
		WorkspaceName:                workspaceName,
		RelationshipName:             relationshipName,
		RelationshipNameNew:          &req.RelationshipNameNew,
		RelationshipDescription:      &req.RelationshipDescription,
		RelationshipType:             &req.RelationshipType,
		RelationshipSourceDatabaseId: &req.RelationshipSourceDatabaseID,
		RelationshipSourceTableName:  &req.RelationshipSourceTableName,
		RelationshipTargetDatabaseId: &req.RelationshipTargetDatabaseID,
		RelationshipTargetTableName:  &req.RelationshipTargetTableName,
		MappingId:                    &req.MappingID,
		PolicyId:                     &req.PolicyID,
	}

	grpcResp, err := rh.engine.relationshipClient.ModifyRelationship(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to modify relationship")
		return
	}

	// Convert gRPC response to REST response
	relationship := Relationship{
		TenantID:                     grpcResp.Relationship.TenantId,
		WorkspaceID:                  grpcResp.Relationship.WorkspaceId,
		RelationshipID:               grpcResp.Relationship.RelationshipId,
		RelationshipName:             grpcResp.Relationship.RelationshipName,
		RelationshipDescription:      grpcResp.Relationship.RelationshipDescription,
		RelationshipType:             grpcResp.Relationship.RelationshipType,
		RelationshipSourceType:       "table", // Default value since not in proto
		RelationshipTargetType:       "table", // Default value since not in proto
		RelationshipSourceDatabaseID: grpcResp.Relationship.RelationshipSourceDatabaseId,
		RelationshipSourceTableName:  grpcResp.Relationship.RelationshipSourceTableName,
		RelationshipTargetDatabaseID: grpcResp.Relationship.RelationshipTargetDatabaseId,
		RelationshipTargetTableName:  grpcResp.Relationship.RelationshipTargetTableName,
		MappingID:                    grpcResp.Relationship.MappingId,
		PolicyID:                     grpcResp.Relationship.PolicyId,
		StatusMessage:                grpcResp.Relationship.StatusMessage,
		Status:                       convertStatus(grpcResp.Relationship.Status),
		OwnerID:                      grpcResp.Relationship.OwnerId,
	}

	response := ModifyRelationshipResponse{
		Message:      grpcResp.Message,
		Success:      grpcResp.Success,
		Relationship: relationship,
		Status:       convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully modified relationship: %s for workspace: %s", relationshipName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteRelationship handles DELETE /{tenant_url}/api/v1/workspaces/{workspace_name}/relationships/{relationship_id}
func (rh *RelationshipHandlers) DeleteRelationship(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	workspaceName := vars["workspace_name"]
	relationshipName := vars["relationship_name"]

	if tenantURL == "" || workspaceName == "" || relationshipName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url, workspace_name, and relationship_name are required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Delete relationship request for relationship: %s, workspace: %s, tenant: %s", relationshipName, workspaceName, profile.TenantId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteRelationshipRequest{
		TenantId:         profile.TenantId,
		WorkspaceName:    workspaceName,
		RelationshipName: relationshipName,
	}

	grpcResp, err := rh.engine.relationshipClient.DeleteRelationship(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to delete relationship")
		return
	}

	response := DeleteRelationshipResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully deleted relationship: %s for workspace: %s", relationshipName, workspaceName)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (rh *RelationshipHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			rh.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			rh.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			rh.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			rh.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			rh.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			rh.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		rh.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Errorf("Relationship handler gRPC error: %v", err)
	}
}

func (rh *RelationshipHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (rh *RelationshipHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	rh.writeJSONResponse(w, statusCode, response)
}
