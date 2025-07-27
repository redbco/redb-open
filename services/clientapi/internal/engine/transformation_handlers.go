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

// TransformationHandlers contains the transformation endpoint handlers
type TransformationHandlers struct {
	engine *Engine
}

// NewTransformationHandlers creates a new instance of TransformationHandlers
func NewTransformationHandlers(engine *Engine) *TransformationHandlers {
	return &TransformationHandlers{
		engine: engine,
	}
}

// ListTransformations handles GET /{tenant_url}/api/v1/transformations
func (th *TransformationHandlers) ListTransformations(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		th.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if th.engine.logger != nil {
		th.engine.logger.Infof("List transformations request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListTransformationsRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := th.engine.transformationClient.ListTransformations(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Failed to list transformations")
		return
	}

	// Convert gRPC response to REST response
	transformations := make([]Transformation, len(grpcResp.Transformations))
	for i, transformation := range grpcResp.Transformations {
		transformations[i] = Transformation{
			TenantID:                  transformation.TenantId,
			TransformationID:          transformation.TransformationId,
			TransformationName:        transformation.TransformationName,
			TransformationDescription: transformation.TransformationDescription,
			TransformationType:        transformation.TransformationType,
			TransformationVersion:     transformation.TransformationVersion,
			TransformationFunction:    transformation.TransformationFunction,
			OwnerID:                   transformation.OwnerId,
		}
	}

	response := ListTransformationsResponse{
		Transformations: transformations,
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Successfully listed %d transformations for tenant: %s", len(transformations), profile.TenantId)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// ShowTransformation handles GET /{tenant_url}/api/v1/transformations/{transformation_id}
func (th *TransformationHandlers) ShowTransformation(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	transformationID := vars["transformation_id"]

	if tenantURL == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if transformationID == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "transformation_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		th.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if th.engine.logger != nil {
		th.engine.logger.Infof("Show transformation request for transformation: %s, tenant: %s, user: %s", transformationID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowTransformationRequest{
		TenantId:         profile.TenantId,
		TransformationId: transformationID,
	}

	grpcResp, err := th.engine.transformationClient.ShowTransformation(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Failed to show transformation")
		return
	}

	// Convert gRPC response to REST response
	transformation := Transformation{
		TenantID:                  grpcResp.Transformation.TenantId,
		TransformationID:          grpcResp.Transformation.TransformationId,
		TransformationName:        grpcResp.Transformation.TransformationName,
		TransformationDescription: grpcResp.Transformation.TransformationDescription,
		TransformationType:        grpcResp.Transformation.TransformationType,
		TransformationVersion:     grpcResp.Transformation.TransformationVersion,
		TransformationFunction:    grpcResp.Transformation.TransformationFunction,
		OwnerID:                   grpcResp.Transformation.OwnerId,
	}

	response := ShowTransformationResponse{
		Transformation: transformation,
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Successfully showed transformation: %s for tenant: %s", transformationID, profile.TenantId)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// AddTransformation handles POST /{tenant_url}/api/v1/transformations
func (th *TransformationHandlers) AddTransformation(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		th.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddTransformationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Failed to parse add transformation request body: %v", err)
		}
		th.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.TransformationName == "" || req.TransformationDescription == "" || req.TransformationType == "" || req.TransformationVersion == "" || req.TransformationFunction == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "transformation_name, transformation_description, transformation_type, transformation_version, and transformation_function are required")
		return
	}

	// Log request
	if th.engine.logger != nil {
		th.engine.logger.Infof("Add transformation request for transformation: %s, tenant: %s, user: %s", req.TransformationName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddTransformationRequest{
		TenantId:                  profile.TenantId,
		OwnerId:                   profile.UserId,
		TransformationName:        req.TransformationName,
		TransformationDescription: req.TransformationDescription,
		TransformationType:        req.TransformationType,
		TransformationVersion:     req.TransformationVersion,
		TransformationFunction:    req.TransformationFunction,
	}

	grpcResp, err := th.engine.transformationClient.AddTransformation(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Failed to add transformation")
		return
	}

	// Convert gRPC response to REST response
	transformation := Transformation{
		TenantID:                  grpcResp.Transformation.TenantId,
		TransformationID:          grpcResp.Transformation.TransformationId,
		TransformationName:        grpcResp.Transformation.TransformationName,
		TransformationDescription: grpcResp.Transformation.TransformationDescription,
		TransformationType:        grpcResp.Transformation.TransformationType,
		TransformationVersion:     grpcResp.Transformation.TransformationVersion,
		TransformationFunction:    grpcResp.Transformation.TransformationFunction,
		OwnerID:                   grpcResp.Transformation.OwnerId,
	}

	response := AddTransformationResponse{
		Message:        grpcResp.Message,
		Success:        grpcResp.Success,
		Transformation: transformation,
		Status:         convertStatus(grpcResp.Status),
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Successfully added transformation: %s for tenant: %s", req.TransformationName, profile.TenantId)
	}

	th.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyTransformation handles PUT /{tenant_url}/api/v1/transformations/{transformation_id}
func (th *TransformationHandlers) ModifyTransformation(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	transformationID := vars["transformation_id"]

	if tenantURL == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if transformationID == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "transformation_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		th.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyTransformationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Failed to parse modify transformation request body: %v", err)
		}
		th.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if th.engine.logger != nil {
		th.engine.logger.Infof("Modify transformation request for transformation: %s, tenant: %s, user: %s", transformationID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyTransformationRequest{
		TenantId:                  profile.TenantId,
		TransformationId:          transformationID,
		TransformationNameNew:     &req.TransformationNameNew,
		TransformationDescription: &req.TransformationDescription,
		TransformationType:        &req.TransformationType,
		TransformationVersion:     &req.TransformationVersion,
		TransformationFunction:    &req.TransformationFunction,
	}

	grpcResp, err := th.engine.transformationClient.ModifyTransformation(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Failed to modify transformation")
		return
	}

	// Convert gRPC response to REST response
	transformation := Transformation{
		TenantID:                  grpcResp.Transformation.TenantId,
		TransformationID:          grpcResp.Transformation.TransformationId,
		TransformationName:        grpcResp.Transformation.TransformationName,
		TransformationDescription: grpcResp.Transformation.TransformationDescription,
		TransformationType:        grpcResp.Transformation.TransformationType,
		TransformationVersion:     grpcResp.Transformation.TransformationVersion,
		TransformationFunction:    grpcResp.Transformation.TransformationFunction,
		OwnerID:                   grpcResp.Transformation.OwnerId,
	}

	response := ModifyTransformationResponse{
		Message:        grpcResp.Message,
		Success:        grpcResp.Success,
		Transformation: transformation,
		Status:         convertStatus(grpcResp.Status),
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Successfully modified transformation: %s for tenant: %s", transformationID, profile.TenantId)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteTransformation handles DELETE /{tenant_url}/api/v1/transformations/{transformation_id}
func (th *TransformationHandlers) DeleteTransformation(w http.ResponseWriter, r *http.Request) {
	th.engine.TrackOperation()
	defer th.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	transformationID := vars["transformation_id"]

	if tenantURL == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if transformationID == "" {
		th.writeErrorResponse(w, http.StatusBadRequest, "transformation_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		th.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if th.engine.logger != nil {
		th.engine.logger.Infof("Delete transformation request for transformation: %s, tenant: %s, user: %s", transformationID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteTransformationRequest{
		TenantId:         profile.TenantId,
		TransformationId: transformationID,
	}

	grpcResp, err := th.engine.transformationClient.DeleteTransformation(ctx, grpcReq)
	if err != nil {
		th.handleGRPCError(w, err, "Failed to delete transformation")
		return
	}

	response := DeleteTransformationResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if th.engine.logger != nil {
		th.engine.logger.Infof("Successfully deleted transformation: %s for tenant: %s", transformationID, profile.TenantId)
	}

	th.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (th *TransformationHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			th.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			th.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			th.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			th.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			th.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			th.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		th.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if th.engine.logger != nil {
		th.engine.logger.Errorf("Transformation handler gRPC error: %v", err)
	}
}

func (th *TransformationHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if th.engine.logger != nil {
			th.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (th *TransformationHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	th.writeJSONResponse(w, statusCode, response)
}
