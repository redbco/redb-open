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

// AnchorHandlers contains the anchor endpoint handlers
type AnchorHandlers struct {
	engine *Engine
}

// NewAnchorHandlers creates a new instance of AnchorHandlers
func NewAnchorHandlers(engine *Engine) *AnchorHandlers {
	return &AnchorHandlers{
		engine: engine,
	}
}

// ListAnchors handles GET /{tenant_url}/api/v1/anchors
func (ah *AnchorHandlers) ListAnchors(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("List anchors request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListAnchorsRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := ah.engine.anchorClient.ListAnchors(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Failed to list anchors")
		return
	}

	// Convert gRPC response to REST response
	anchors := make([]Anchor, len(grpcResp.Anchors))
	for i, anchor := range grpcResp.Anchors {
		anchors[i] = Anchor{
			TenantID:          anchor.TenantId,
			AnchorID:          anchor.AnchorId,
			AnchorName:        anchor.AnchorName,
			AnchorDescription: anchor.AnchorDescription,
			AnchorPlatform:    anchor.AnchorPlatform,
			AnchorVersion:     anchor.AnchorVersion,
			IPAddress:         anchor.IpAddress,
			NodeID:            anchor.NodeId,
			Status:            convertStatus(anchor.Status),
			OwnerID:           anchor.OwnerId,
		}
	}

	response := ListAnchorsResponse{
		Anchors: anchors,
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Successfully listed %d anchors for tenant: %s", len(anchors), profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// ShowAnchor handles GET /{tenant_url}/api/v1/anchors/{anchor_id}
func (ah *AnchorHandlers) ShowAnchor(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	anchorID := vars["anchor_id"]

	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if anchorID == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "anchor_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Show anchor request for anchor: %s, tenant: %s, user: %s", anchorID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowAnchorRequest{
		TenantId: profile.TenantId,
		AnchorId: anchorID,
	}

	grpcResp, err := ah.engine.anchorClient.ShowAnchor(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Failed to show anchor")
		return
	}

	// Convert gRPC response to REST response
	anchor := Anchor{
		TenantID:          grpcResp.Anchor.TenantId,
		AnchorID:          grpcResp.Anchor.AnchorId,
		AnchorName:        grpcResp.Anchor.AnchorName,
		AnchorDescription: grpcResp.Anchor.AnchorDescription,
		AnchorPlatform:    grpcResp.Anchor.AnchorPlatform,
		AnchorVersion:     grpcResp.Anchor.AnchorVersion,
		IPAddress:         grpcResp.Anchor.IpAddress,
		NodeID:            grpcResp.Anchor.NodeId,
		Status:            convertStatus(grpcResp.Anchor.Status),
		OwnerID:           grpcResp.Anchor.OwnerId,
	}

	response := ShowAnchorResponse{
		Anchor: anchor,
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Successfully showed anchor: %s for tenant: %s", anchorID, profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// AddAnchor handles POST /{tenant_url}/api/v1/anchors
func (ah *AnchorHandlers) AddAnchor(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddAnchorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Failed to parse add anchor request body: %v", err)
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.AnchorName == "" || req.AnchorPlatform == "" || req.AnchorVersion == "" || req.IPAddress == "" || req.NodeID == "" || req.PublicKey == "" || req.PrivateKey == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "anchor_name, anchor_platform, anchor_version, ip_address, node_id, public_key, and private_key are required")
		return
	}

	// Log request
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Add anchor request for anchor: %s, tenant: %s, user: %s", req.AnchorName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddAnchorRequest{
		TenantId:          profile.TenantId,
		OwnerId:           profile.UserId,
		AnchorName:        req.AnchorName,
		AnchorDescription: &req.AnchorDescription,
		AnchorPlatform:    req.AnchorPlatform,
		AnchorVersion:     req.AnchorVersion,
		IpAddress:         req.IPAddress,
		NodeId:            req.NodeID,
		PublicKey:         req.PublicKey,
		PrivateKey:        req.PrivateKey,
	}

	grpcResp, err := ah.engine.anchorClient.AddAnchor(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Failed to add anchor")
		return
	}

	// Convert gRPC response to REST response
	anchor := Anchor{
		TenantID:          grpcResp.Anchor.TenantId,
		AnchorID:          grpcResp.Anchor.AnchorId,
		AnchorName:        grpcResp.Anchor.AnchorName,
		AnchorDescription: grpcResp.Anchor.AnchorDescription,
		AnchorPlatform:    grpcResp.Anchor.AnchorPlatform,
		AnchorVersion:     grpcResp.Anchor.AnchorVersion,
		IPAddress:         grpcResp.Anchor.IpAddress,
		NodeID:            grpcResp.Anchor.NodeId,
		Status:            convertStatus(grpcResp.Anchor.Status),
		OwnerID:           grpcResp.Anchor.OwnerId,
	}

	response := AddAnchorResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Anchor:  anchor,
		Status:  convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Successfully added anchor: %s for tenant: %s", req.AnchorName, profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyAnchor handles PUT /{tenant_url}/api/v1/anchors/{anchor_id}
func (ah *AnchorHandlers) ModifyAnchor(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	anchorID := vars["anchor_id"]

	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if anchorID == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "anchor_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyAnchorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Failed to parse modify anchor request body: %v", err)
		}
		ah.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Modify anchor request for anchor: %s, tenant: %s, user: %s", anchorID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyAnchorRequest{
		TenantId:          profile.TenantId,
		AnchorId:          anchorID,
		AnchorName:        &req.AnchorName,
		AnchorDescription: &req.AnchorDescription,
		AnchorPlatform:    &req.AnchorPlatform,
		AnchorVersion:     &req.AnchorVersion,
		IpAddress:         &req.IPAddress,
		NodeId:            &req.NodeID,
	}

	grpcResp, err := ah.engine.anchorClient.ModifyAnchor(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Failed to modify anchor")
		return
	}

	// Convert gRPC response to REST response
	anchor := Anchor{
		TenantID:          grpcResp.Anchor.TenantId,
		AnchorID:          grpcResp.Anchor.AnchorId,
		AnchorName:        grpcResp.Anchor.AnchorName,
		AnchorDescription: grpcResp.Anchor.AnchorDescription,
		AnchorPlatform:    grpcResp.Anchor.AnchorPlatform,
		AnchorVersion:     grpcResp.Anchor.AnchorVersion,
		IPAddress:         grpcResp.Anchor.IpAddress,
		NodeID:            grpcResp.Anchor.NodeId,
		Status:            convertStatus(grpcResp.Anchor.Status),
		OwnerID:           grpcResp.Anchor.OwnerId,
	}

	response := ModifyAnchorResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Anchor:  anchor,
		Status:  convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Successfully modified anchor: %s for tenant: %s", anchorID, profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteAnchor handles DELETE /{tenant_url}/api/v1/anchors/{anchor_id}
func (ah *AnchorHandlers) DeleteAnchor(w http.ResponseWriter, r *http.Request) {
	ah.engine.TrackOperation()
	defer ah.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	anchorID := vars["anchor_id"]

	if tenantURL == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if anchorID == "" {
		ah.writeErrorResponse(w, http.StatusBadRequest, "anchor_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ah.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Delete anchor request for anchor: %s, tenant: %s, user: %s", anchorID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteAnchorRequest{
		TenantId: profile.TenantId,
		AnchorId: anchorID,
	}

	grpcResp, err := ah.engine.anchorClient.DeleteAnchor(ctx, grpcReq)
	if err != nil {
		ah.handleGRPCError(w, err, "Failed to delete anchor")
		return
	}

	response := DeleteAnchorResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Infof("Successfully deleted anchor: %s for tenant: %s", anchorID, profile.TenantId)
	}

	ah.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (ah *AnchorHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			ah.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			ah.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			ah.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			ah.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			ah.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			ah.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		ah.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if ah.engine.logger != nil {
		ah.engine.logger.Errorf("Anchor handler gRPC error: %v", err)
	}
}

func (ah *AnchorHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if ah.engine.logger != nil {
			ah.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (ah *AnchorHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	ah.writeJSONResponse(w, statusCode, response)
}
