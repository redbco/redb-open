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

// RegionHandlers contains the region endpoint handlers
type RegionHandlers struct {
	engine *Engine
}

// NewRegionHandlers creates a new instance of RegionHandlers
func NewRegionHandlers(engine *Engine) *RegionHandlers {
	return &RegionHandlers{
		engine: engine,
	}
}

// ListRegions handles GET /{tenant_url}/api/v1/regions
func (rh *RegionHandlers) ListRegions(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
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
		rh.engine.logger.Infof("List regions request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListRegionsRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := rh.engine.regionClient.ListRegions(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to list regions")
		return
	}

	// Convert gRPC response to REST response
	regions := make([]Region, len(grpcResp.Regions))
	for i, region := range grpcResp.Regions {
		regions[i] = Region{
			RegionID:          region.RegionId,
			RegionName:        region.RegionName,
			RegionDescription: region.RegionDescription,
			RegionLocation:    region.RegionLocation,
			RegionLatitude:    region.RegionLatitude,
			RegionLongitude:   region.RegionLongitude,
			RegionType:        region.RegionType,
			NodeCount:         region.NodeCount,
			InstanceCount:     region.InstanceCount,
			DatabaseCount:     region.DatabaseCount,
			Status:            convertStatus(region.Status),
			GlobalRegion:      region.GlobalRegion,
			Created:           region.Created,
			Updated:           region.Updated,
		}
	}

	response := ListRegionsResponse{
		Regions: regions,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully listed %d regions for tenant: %s", len(regions), profile.TenantId)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowRegion handles GET /{tenant_url}/api/v1/regions/{region_id}
func (rh *RegionHandlers) ShowRegion(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	regionName := vars["region_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if regionName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "region_name is required", "")
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
		rh.engine.logger.Infof("Show region request for region: %s, tenant: %s, user: %s", regionName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowRegionRequest{
		TenantId:   profile.TenantId,
		RegionName: regionName,
	}

	grpcResp, err := rh.engine.regionClient.ShowRegion(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to show region")
		return
	}

	// Convert gRPC response to REST response
	region := Region{
		RegionID:          grpcResp.Region.RegionId,
		RegionName:        grpcResp.Region.RegionName,
		RegionDescription: grpcResp.Region.RegionDescription,
		RegionLocation:    grpcResp.Region.RegionLocation,
		RegionLatitude:    grpcResp.Region.RegionLatitude,
		RegionLongitude:   grpcResp.Region.RegionLongitude,
		RegionType:        grpcResp.Region.RegionType,
		NodeCount:         grpcResp.Region.NodeCount,
		InstanceCount:     grpcResp.Region.InstanceCount,
		DatabaseCount:     grpcResp.Region.DatabaseCount,
		Status:            convertStatus(grpcResp.Region.Status),
		GlobalRegion:      grpcResp.Region.GlobalRegion,
		Created:           grpcResp.Region.Created,
		Updated:           grpcResp.Region.Updated,
	}

	response := ShowRegionResponse{
		Region: region,
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully showed region: %s for tenant: %s", regionName, profile.TenantId)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// AddRegion handles POST /{tenant_url}/api/v1/regions
func (rh *RegionHandlers) AddRegion(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddRegionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse add region request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.RegionName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "region_name is required", "")
		return
	}

	if req.RegionType == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "region_type is required", "")
		return
	}

	// Note: owner_id is always set from authenticated user profile, not from request body
	// This prevents users from creating resources that belong to someone else

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Add region request: %s for tenant: %s, user: %s", req.RegionName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request - owner_id is always the authenticated user
	grpcReq := &corev1.AddRegionRequest{
		TenantId:          profile.TenantId,
		RegionName:        req.RegionName,
		RegionType:        req.RegionType,
		RegionDescription: &req.RegionDescription,
		RegionLocation:    &req.RegionLocation,
	}

	// Set optional fields
	if req.RegionLatitude != nil {
		grpcReq.RegionLatitude = req.RegionLatitude
	}
	if req.RegionLongitude != nil {
		grpcReq.RegionLongitude = req.RegionLongitude
	}

	grpcResp, err := rh.engine.regionClient.AddRegion(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to add region")
		return
	}

	// Convert gRPC response to REST response
	region := Region{
		RegionID:          grpcResp.Region.RegionId,
		RegionName:        grpcResp.Region.RegionName,
		RegionDescription: grpcResp.Region.RegionDescription,
		RegionLocation:    grpcResp.Region.RegionLocation,
		RegionLatitude:    grpcResp.Region.RegionLatitude,
		RegionLongitude:   grpcResp.Region.RegionLongitude,
		RegionType:        grpcResp.Region.RegionType,
		NodeCount:         grpcResp.Region.NodeCount,
		InstanceCount:     grpcResp.Region.InstanceCount,
		DatabaseCount:     grpcResp.Region.DatabaseCount,
		Status:            convertStatus(grpcResp.Region.Status),
		GlobalRegion:      grpcResp.Region.GlobalRegion,
		Created:           grpcResp.Region.Created,
		Updated:           grpcResp.Region.Updated,
	}

	response := AddRegionResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Region:  region,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully added region: %s for tenant: %s", req.RegionName, profile.TenantId)
	}

	rh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyRegion handles PUT /{tenant_url}/api/v1/regions/{region_id}
func (rh *RegionHandlers) ModifyRegion(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	regionName := vars["region_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if regionName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "region_name is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		rh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyRegionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to parse modify region request body: %v", err)
		}
		rh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Modify region request for region: %s, tenant: %s, user: %s", regionName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifyRegionRequest{
		TenantId:   profile.TenantId,
		RegionName: regionName,
	}

	// Set optional fields
	if req.RegionNameNew != "" {
		grpcReq.RegionNameNew = &req.RegionNameNew
	}
	if req.RegionDescription != "" {
		grpcReq.RegionDescription = &req.RegionDescription
	}
	if req.RegionLocation != "" {
		grpcReq.RegionLocation = &req.RegionLocation
	}
	if req.RegionLatitude != nil {
		grpcReq.RegionLatitude = req.RegionLatitude
	}
	if req.RegionLongitude != nil {
		grpcReq.RegionLongitude = req.RegionLongitude
	}

	grpcResp, err := rh.engine.regionClient.ModifyRegion(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to modify region")
		return
	}

	// Convert gRPC response to REST response
	region := Region{
		RegionID:          grpcResp.Region.RegionId,
		RegionName:        grpcResp.Region.RegionName,
		RegionDescription: grpcResp.Region.RegionDescription,
		RegionLocation:    grpcResp.Region.RegionLocation,
		RegionLatitude:    grpcResp.Region.RegionLatitude,
		RegionLongitude:   grpcResp.Region.RegionLongitude,
		RegionType:        grpcResp.Region.RegionType,
		NodeCount:         grpcResp.Region.NodeCount,
		InstanceCount:     grpcResp.Region.InstanceCount,
		DatabaseCount:     grpcResp.Region.DatabaseCount,
		Status:            convertStatus(grpcResp.Region.Status),
		GlobalRegion:      grpcResp.Region.GlobalRegion,
		Created:           grpcResp.Region.Created,
		Updated:           grpcResp.Region.Updated,
	}

	response := ModifyRegionResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Region:  region,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully modified region: %s for tenant: %s", regionName, profile.TenantId)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteRegion handles DELETE /{tenant_url}/api/v1/regions/{region_id}
func (rh *RegionHandlers) DeleteRegion(w http.ResponseWriter, r *http.Request) {
	rh.engine.TrackOperation()
	defer rh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	regionName := vars["region_name"]

	if tenantURL == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if regionName == "" {
		rh.writeErrorResponse(w, http.StatusBadRequest, "region_name is required", "")
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
		rh.engine.logger.Infof("Delete region request for region: %s, tenant: %s, user: %s", regionName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteRegionRequest{
		TenantId:   profile.TenantId,
		RegionName: regionName,
	}

	grpcResp, err := rh.engine.regionClient.DeleteRegion(ctx, grpcReq)
	if err != nil {
		rh.handleGRPCError(w, err, "Failed to delete region")
		return
	}

	response := DeleteRegionResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if rh.engine.logger != nil {
		rh.engine.logger.Infof("Successfully deleted region: %s for tenant: %s", regionName, profile.TenantId)
	}

	rh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (rh *RegionHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		rh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	var httpStatus int
	switch grpcStatus.Code() {
	case codes.NotFound:
		httpStatus = http.StatusNotFound
	case codes.InvalidArgument:
		httpStatus = http.StatusBadRequest
	case codes.AlreadyExists:
		httpStatus = http.StatusConflict
	case codes.PermissionDenied:
		httpStatus = http.StatusForbidden
	case codes.Unauthenticated:
		httpStatus = http.StatusUnauthorized
	default:
		httpStatus = http.StatusInternalServerError
	}

	message := grpcStatus.Message()
	if message == "" {
		message = defaultMessage
	}

	rh.writeErrorResponse(w, httpStatus, message, "")
}

func (rh *RegionHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (rh *RegionHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, error string) {
	response := ErrorResponse{
		Error:   error,
		Message: message,
		Status:  StatusError,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if rh.engine.logger != nil {
			rh.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}
