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

// SatelliteHandlers contains the satellite endpoint handlers
type SatelliteHandlers struct {
	engine *Engine
}

// NewSatelliteHandlers creates a new instance of SatelliteHandlers
func NewSatelliteHandlers(engine *Engine) *SatelliteHandlers {
	return &SatelliteHandlers{
		engine: engine,
	}
}

// ListSatellites handles GET /{tenant_url}/api/v1/satellites
func (sh *SatelliteHandlers) ListSatellites(w http.ResponseWriter, r *http.Request) {
	sh.engine.TrackOperation()
	defer sh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		sh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if sh.engine.logger != nil {
		sh.engine.logger.Infof("List satellites request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListSatellitesRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := sh.engine.satelliteClient.ListSatellites(ctx, grpcReq)
	if err != nil {
		sh.handleGRPCError(w, err, "Failed to list satellites")
		return
	}

	// Convert gRPC response to REST response
	satellites := make([]Satellite, len(grpcResp.Satellites))
	for i, sat := range grpcResp.Satellites {
		satellites[i] = Satellite{
			TenantID:             sat.TenantId,
			SatelliteID:          sat.SatelliteId,
			SatelliteName:        sat.SatelliteName,
			SatelliteDescription: sat.SatelliteDescription,
			SatellitePlatform:    sat.SatellitePlatform,
			SatelliteVersion:     sat.SatelliteVersion,
			IPAddress:            sat.IpAddress,
			NodeID:               sat.NodeId,
			Status:               convertStatus(sat.Status),
			OwnerID:              sat.OwnerId,
		}
	}

	response := ListSatellitesResponse{
		Satellites: satellites,
	}

	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Successfully listed %d satellites for tenant: %s", len(satellites), profile.TenantId)
	}

	sh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowSatellite handles GET /{tenant_url}/api/v1/satellites/{satellite_id}
func (sh *SatelliteHandlers) ShowSatellite(w http.ResponseWriter, r *http.Request) {
	sh.engine.TrackOperation()
	defer sh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	satelliteID := vars["satellite_id"]

	if tenantURL == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if satelliteID == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "satellite_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		sh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Show satellite request for satellite: %s, tenant: %s, user: %s", satelliteID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowSatelliteRequest{
		TenantId:    profile.TenantId,
		SatelliteId: satelliteID,
	}

	grpcResp, err := sh.engine.satelliteClient.ShowSatellite(ctx, grpcReq)
	if err != nil {
		sh.handleGRPCError(w, err, "Failed to show satellite")
		return
	}

	// Convert gRPC response to REST response
	satellite := Satellite{
		TenantID:             grpcResp.Satellite.TenantId,
		SatelliteID:          grpcResp.Satellite.SatelliteId,
		SatelliteName:        grpcResp.Satellite.SatelliteName,
		SatelliteDescription: grpcResp.Satellite.SatelliteDescription,
		SatellitePlatform:    grpcResp.Satellite.SatellitePlatform,
		SatelliteVersion:     grpcResp.Satellite.SatelliteVersion,
		IPAddress:            grpcResp.Satellite.IpAddress,
		NodeID:               grpcResp.Satellite.NodeId,
		Status:               convertStatus(grpcResp.Satellite.Status),
		OwnerID:              grpcResp.Satellite.OwnerId,
	}

	response := ShowSatelliteResponse{
		Satellite: satellite,
	}

	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Successfully showed satellite: %s for tenant: %s", satelliteID, profile.TenantId)
	}

	sh.writeJSONResponse(w, http.StatusOK, response)
}

// AddSatellite handles POST /{tenant_url}/api/v1/satellites
func (sh *SatelliteHandlers) AddSatellite(w http.ResponseWriter, r *http.Request) {
	sh.engine.TrackOperation()
	defer sh.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		sh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddSatelliteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if sh.engine.logger != nil {
			sh.engine.logger.Errorf("Failed to parse add satellite request body: %v", err)
		}
		sh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.SatelliteName == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "satellite_name is required", "")
		return
	}
	if req.SatellitePlatform == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "satellite_platform is required", "")
		return
	}
	if req.SatelliteVersion == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "satellite_version is required", "")
		return
	}
	if req.IPAddress == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "ip_address is required", "")
		return
	}
	if req.NodeID == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "node_id is required", "")
		return
	}
	if req.PublicKey == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "public_key is required", "")
		return
	}
	if req.PrivateKey == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "private_key is required", "")
		return
	}

	// Log request
	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Add satellite request: %s for tenant: %s, user: %s", req.SatelliteName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request - owner_id is always the authenticated user
	grpcReq := &corev1.AddSatelliteRequest{
		TenantId:          profile.TenantId,
		SatelliteName:     req.SatelliteName,
		SatellitePlatform: req.SatellitePlatform,
		SatelliteVersion:  req.SatelliteVersion,
		IpAddress:         req.IPAddress,
		NodeId:            req.NodeID,
		PublicKey:         req.PublicKey,
		PrivateKey:        req.PrivateKey,
		OwnerId:           profile.UserId,
	}

	// Set optional fields
	if req.SatelliteDescription != "" {
		grpcReq.SatelliteDescription = &req.SatelliteDescription
	}

	grpcResp, err := sh.engine.satelliteClient.AddSatellite(ctx, grpcReq)
	if err != nil {
		sh.handleGRPCError(w, err, "Failed to add satellite")
		return
	}

	// Convert gRPC response to REST response
	satellite := Satellite{
		TenantID:             grpcResp.Satellite.TenantId,
		SatelliteID:          grpcResp.Satellite.SatelliteId,
		SatelliteName:        grpcResp.Satellite.SatelliteName,
		SatelliteDescription: grpcResp.Satellite.SatelliteDescription,
		SatellitePlatform:    grpcResp.Satellite.SatellitePlatform,
		SatelliteVersion:     grpcResp.Satellite.SatelliteVersion,
		IPAddress:            grpcResp.Satellite.IpAddress,
		NodeID:               grpcResp.Satellite.NodeId,
		Status:               convertStatus(grpcResp.Satellite.Status),
		OwnerID:              grpcResp.Satellite.OwnerId,
	}

	response := AddSatelliteResponse{
		Message:   grpcResp.Message,
		Success:   grpcResp.Success,
		Satellite: satellite,
		Status:    convertStatus(grpcResp.Status),
	}

	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Successfully added satellite: %s for tenant: %s", req.SatelliteName, profile.TenantId)
	}

	sh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifySatellite handles PUT /{tenant_url}/api/v1/satellites/{satellite_id}
func (sh *SatelliteHandlers) ModifySatellite(w http.ResponseWriter, r *http.Request) {
	sh.engine.TrackOperation()
	defer sh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	satelliteID := vars["satellite_id"]

	if tenantURL == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if satelliteID == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "satellite_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		sh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifySatelliteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if sh.engine.logger != nil {
			sh.engine.logger.Errorf("Failed to parse modify satellite request body: %v", err)
		}
		sh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Modify satellite request for satellite: %s, tenant: %s, user: %s", satelliteID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifySatelliteRequest{
		TenantId:    profile.TenantId,
		SatelliteId: satelliteID,
	}

	// Set optional fields
	if req.SatelliteName != "" {
		grpcReq.SatelliteName = &req.SatelliteName
	}
	if req.SatelliteDescription != "" {
		grpcReq.SatelliteDescription = &req.SatelliteDescription
	}
	if req.SatellitePlatform != "" {
		grpcReq.SatellitePlatform = &req.SatellitePlatform
	}
	if req.SatelliteVersion != "" {
		grpcReq.SatelliteVersion = &req.SatelliteVersion
	}
	if req.IPAddress != "" {
		grpcReq.IpAddress = &req.IPAddress
	}
	if req.NodeID != "" {
		grpcReq.NodeId = &req.NodeID
	}
	if req.PublicKey != "" {
		grpcReq.PublicKey = &req.PublicKey
	}
	if req.PrivateKey != "" {
		grpcReq.PrivateKey = &req.PrivateKey
	}

	grpcResp, err := sh.engine.satelliteClient.ModifySatellite(ctx, grpcReq)
	if err != nil {
		sh.handleGRPCError(w, err, "Failed to modify satellite")
		return
	}

	// Convert gRPC response to REST response
	satellite := Satellite{
		TenantID:             grpcResp.Satellite.TenantId,
		SatelliteID:          grpcResp.Satellite.SatelliteId,
		SatelliteName:        grpcResp.Satellite.SatelliteName,
		SatelliteDescription: grpcResp.Satellite.SatelliteDescription,
		SatellitePlatform:    grpcResp.Satellite.SatellitePlatform,
		SatelliteVersion:     grpcResp.Satellite.SatelliteVersion,
		IPAddress:            grpcResp.Satellite.IpAddress,
		NodeID:               grpcResp.Satellite.NodeId,
		Status:               convertStatus(grpcResp.Satellite.Status),
		OwnerID:              grpcResp.Satellite.OwnerId,
	}

	response := ModifySatelliteResponse{
		Message:   grpcResp.Message,
		Success:   grpcResp.Success,
		Satellite: satellite,
		Status:    convertStatus(grpcResp.Status),
	}

	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Successfully modified satellite: %s for tenant: %s", satelliteID, profile.TenantId)
	}

	sh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteSatellite handles DELETE /{tenant_url}/api/v1/satellites/{satellite_id}
func (sh *SatelliteHandlers) DeleteSatellite(w http.ResponseWriter, r *http.Request) {
	sh.engine.TrackOperation()
	defer sh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	satelliteID := vars["satellite_id"]

	if tenantURL == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if satelliteID == "" {
		sh.writeErrorResponse(w, http.StatusBadRequest, "satellite_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		sh.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Delete satellite request for satellite: %s, tenant: %s, user: %s", satelliteID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteSatelliteRequest{
		TenantId:    profile.TenantId,
		SatelliteId: satelliteID,
	}

	grpcResp, err := sh.engine.satelliteClient.DeleteSatellite(ctx, grpcReq)
	if err != nil {
		sh.handleGRPCError(w, err, "Failed to delete satellite")
		return
	}

	response := DeleteSatelliteResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if sh.engine.logger != nil {
		sh.engine.logger.Infof("Successfully deleted satellite: %s for tenant: %s", satelliteID, profile.TenantId)
	}

	sh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

// handleGRPCError handles gRPC errors and converts them to HTTP responses
func (sh *SatelliteHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if sh.engine.logger != nil {
		sh.engine.logger.Errorf("gRPC error: %v", err)
	}

	st, ok := status.FromError(err)
	if !ok {
		sh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		sh.writeErrorResponse(w, http.StatusNotFound, "Resource not found", st.Message())
	case codes.AlreadyExists:
		sh.writeErrorResponse(w, http.StatusConflict, "Resource already exists", st.Message())
	case codes.InvalidArgument:
		sh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request", st.Message())
	case codes.PermissionDenied:
		sh.writeErrorResponse(w, http.StatusForbidden, "Permission denied", st.Message())
	case codes.Unauthenticated:
		sh.writeErrorResponse(w, http.StatusUnauthorized, "Authentication required", st.Message())
	case codes.Unavailable:
		sh.writeErrorResponse(w, http.StatusServiceUnavailable, "Service unavailable", st.Message())
	case codes.DeadlineExceeded:
		sh.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", st.Message())
	default:
		sh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, st.Message())
	}
}

// writeJSONResponse writes a JSON response
func (sh *SatelliteHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if sh.engine.logger != nil {
			sh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

// writeErrorResponse writes an error response
func (sh *SatelliteHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if sh.engine.logger != nil {
			sh.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}
