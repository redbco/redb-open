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
	"google.golang.org/protobuf/types/known/structpb"
)

// PolicyHandlers contains the policy endpoint handlers
type PolicyHandlers struct {
	engine *Engine
}

// NewPolicyHandlers creates a new instance of PolicyHandlers
func NewPolicyHandlers(engine *Engine) *PolicyHandlers {
	return &PolicyHandlers{
		engine: engine,
	}
}

// ListPolicies handles GET /{tenant_url}/api/v1/policies
func (ph *PolicyHandlers) ListPolicies(w http.ResponseWriter, r *http.Request) {
	ph.engine.TrackOperation()
	defer ph.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ph.engine.logger != nil {
		ph.engine.logger.Infof("List policies request for tenant: %s, user: %s", profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListPoliciesRequest{
		TenantId: profile.TenantId,
	}

	grpcResp, err := ph.engine.policyClient.ListPolicies(ctx, grpcReq)
	if err != nil {
		ph.handleGRPCError(w, err, "Failed to list policies")
		return
	}

	// Convert gRPC response to REST response
	policies := make([]Policy, len(grpcResp.Policies))
	for i, policy := range grpcResp.Policies {
		policies[i] = Policy{
			TenantID:          policy.TenantId,
			PolicyID:          policy.PolicyId,
			PolicyName:        policy.PolicyName,
			PolicyDescription: policy.PolicyDescription,
			PolicyObject:      policy.PolicyObject,
			OwnerID:           policy.OwnerId,
		}
	}

	response := ListPoliciesResponse{
		Policies: policies,
	}

	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Successfully listed %d policies for tenant: %s", len(policies), profile.TenantId)
	}

	ph.writeJSONResponse(w, http.StatusOK, response)
}

// ShowPolicy handles GET /{tenant_url}/api/v1/policies/{policy_id}
func (ph *PolicyHandlers) ShowPolicy(w http.ResponseWriter, r *http.Request) {
	ph.engine.TrackOperation()
	defer ph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	policyID := vars["policy_id"]

	if tenantURL == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if policyID == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "policy_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Show policy request for policy: %s, tenant: %s, user: %s", policyID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowPolicyRequest{
		TenantId: profile.TenantId,
		PolicyId: policyID,
	}

	grpcResp, err := ph.engine.policyClient.ShowPolicy(ctx, grpcReq)
	if err != nil {
		ph.handleGRPCError(w, err, "Failed to show policy")
		return
	}

	// Convert gRPC response to REST response
	policy := Policy{
		TenantID:          grpcResp.Policy.TenantId,
		PolicyID:          grpcResp.Policy.PolicyId,
		PolicyName:        grpcResp.Policy.PolicyName,
		PolicyDescription: grpcResp.Policy.PolicyDescription,
		PolicyObject:      grpcResp.Policy.PolicyObject,
		OwnerID:           grpcResp.Policy.OwnerId,
	}

	response := ShowPolicyResponse{
		Policy: policy,
	}

	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Successfully showed policy: %s for tenant: %s", policyID, profile.TenantId)
	}

	ph.writeJSONResponse(w, http.StatusOK, response)
}

// AddPolicy handles POST /{tenant_url}/api/v1/policies
func (ph *PolicyHandlers) AddPolicy(w http.ResponseWriter, r *http.Request) {
	ph.engine.TrackOperation()
	defer ph.engine.UntrackOperation()

	// Extract tenant_url from path
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	if tenantURL == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req AddPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ph.engine.logger != nil {
			ph.engine.logger.Errorf("Failed to parse add policy request body: %v", err)
		}
		ph.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.PolicyName == "" || req.PolicyDescription == "" || req.PolicyObject == nil {
		ph.writeErrorResponse(w, http.StatusBadRequest, "Required fields missing", "policy_name, policy_description, and policy_object are required")
		return
	}

	// Log request
	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Add policy request for policy: %s, tenant: %s, user: %s", req.PolicyName, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.AddPolicyRequest{
		TenantId:          profile.TenantId,
		OwnerId:           profile.UserId,
		PolicyName:        req.PolicyName,
		PolicyDescription: req.PolicyDescription,
		PolicyObject:      req.PolicyObject.(*structpb.Struct),
	}

	grpcResp, err := ph.engine.policyClient.AddPolicy(ctx, grpcReq)
	if err != nil {
		ph.handleGRPCError(w, err, "Failed to add policy")
		return
	}

	// Convert gRPC response to REST response
	policy := Policy{
		TenantID:          grpcResp.Policy.TenantId,
		PolicyID:          grpcResp.Policy.PolicyId,
		PolicyName:        grpcResp.Policy.PolicyName,
		PolicyDescription: grpcResp.Policy.PolicyDescription,
		PolicyObject:      grpcResp.Policy.PolicyObject,
		OwnerID:           grpcResp.Policy.OwnerId,
	}

	response := AddPolicyResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Policy:  policy,
		Status:  convertStatus(grpcResp.Status),
	}

	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Successfully added policy: %s for tenant: %s", req.PolicyName, profile.TenantId)
	}

	ph.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyPolicy handles PUT /{tenant_url}/api/v1/policies/{policy_id}
func (ph *PolicyHandlers) ModifyPolicy(w http.ResponseWriter, r *http.Request) {
	ph.engine.TrackOperation()
	defer ph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	policyID := vars["policy_id"]

	if tenantURL == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if policyID == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "policy_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Parse request body
	var req ModifyPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if ph.engine.logger != nil {
			ph.engine.logger.Errorf("Failed to parse modify policy request body: %v", err)
		}
		ph.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Modify policy request for policy: %s, tenant: %s, user: %s", policyID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ModifyPolicyRequest{
		TenantId:          profile.TenantId,
		PolicyId:          policyID,
		PolicyNameNew:     &req.PolicyNameNew,
		PolicyDescription: &req.PolicyDescription,
		PolicyObject:      req.PolicyObject.(*structpb.Struct),
	}

	grpcResp, err := ph.engine.policyClient.ModifyPolicy(ctx, grpcReq)
	if err != nil {
		ph.handleGRPCError(w, err, "Failed to modify policy")
		return
	}

	// Convert gRPC response to REST response
	policy := Policy{
		TenantID:          grpcResp.Policy.TenantId,
		PolicyID:          grpcResp.Policy.PolicyId,
		PolicyName:        grpcResp.Policy.PolicyName,
		PolicyDescription: grpcResp.Policy.PolicyDescription,
		PolicyObject:      grpcResp.Policy.PolicyObject,
		OwnerID:           grpcResp.Policy.OwnerId,
	}

	response := ModifyPolicyResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Policy:  policy,
		Status:  convertStatus(grpcResp.Status),
	}

	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Successfully modified policy: %s for tenant: %s", policyID, profile.TenantId)
	}

	ph.writeJSONResponse(w, http.StatusOK, response)
}

// DeletePolicy handles DELETE /{tenant_url}/api/v1/policies/{policy_id}
func (ph *PolicyHandlers) DeletePolicy(w http.ResponseWriter, r *http.Request) {
	ph.engine.TrackOperation()
	defer ph.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	tenantURL := vars["tenant_url"]
	policyID := vars["policy_id"]

	if tenantURL == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "tenant_url is required", "")
		return
	}

	if policyID == "" {
		ph.writeErrorResponse(w, http.StatusBadRequest, "policy_id is required", "")
		return
	}

	// Get tenant_id from authenticated profile
	profile, ok := r.Context().Value(profileContextKey).(*securityv1.Profile)
	if !ok || profile == nil {
		ph.writeErrorResponse(w, http.StatusInternalServerError, "Profile not found in context", "")
		return
	}

	// Log request
	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Delete policy request for policy: %s, tenant: %s, user: %s", policyID, profile.TenantId, profile.UserId)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeletePolicyRequest{
		TenantId: profile.TenantId,
		PolicyId: policyID,
	}

	grpcResp, err := ph.engine.policyClient.DeletePolicy(ctx, grpcReq)
	if err != nil {
		ph.handleGRPCError(w, err, "Failed to delete policy")
		return
	}

	response := DeletePolicyResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if ph.engine.logger != nil {
		ph.engine.logger.Infof("Successfully deleted policy: %s for tenant: %s", policyID, profile.TenantId)
	}

	ph.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

func (ph *PolicyHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			ph.writeErrorResponse(w, http.StatusNotFound, st.Message(), defaultMessage)
		case codes.AlreadyExists:
			ph.writeErrorResponse(w, http.StatusConflict, st.Message(), defaultMessage)
		case codes.InvalidArgument:
			ph.writeErrorResponse(w, http.StatusBadRequest, st.Message(), defaultMessage)
		case codes.PermissionDenied:
			ph.writeErrorResponse(w, http.StatusForbidden, st.Message(), defaultMessage)
		case codes.Unauthenticated:
			ph.writeErrorResponse(w, http.StatusUnauthorized, st.Message(), defaultMessage)
		default:
			ph.writeErrorResponse(w, http.StatusInternalServerError, st.Message(), defaultMessage)
		}
	} else {
		ph.writeErrorResponse(w, http.StatusInternalServerError, err.Error(), defaultMessage)
	}

	if ph.engine.logger != nil {
		ph.engine.logger.Errorf("Policy handler gRPC error: %v", err)
	}
}

func (ph *PolicyHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if ph.engine.logger != nil {
			ph.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

func (ph *PolicyHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}
	ph.writeJSONResponse(w, statusCode, response)
}
