package engine

import (
	"context"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/policy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// PolicyService gRPC handlers
// ============================================================================

func (s *Server) ListPolicies(ctx context.Context, req *corev1.ListPoliciesRequest) (*corev1.ListPoliciesResponse, error) {
	defer s.trackOperation()()

	// Get policy service
	policyService := policy.NewService(s.engine.db, s.engine.logger)

	// List policies for the tenant
	policies, err := policyService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list policies: %v", err)
	}

	// Convert to protobuf format
	protoPolicies := make([]*corev1.Policy, len(policies))
	for i, p := range policies {
		protoPolicy, err := s.policyToProto(p)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert policy: %v", err)
		}
		protoPolicies[i] = protoPolicy
	}

	return &corev1.ListPoliciesResponse{
		Policies: protoPolicies,
	}, nil
}

func (s *Server) ShowPolicy(ctx context.Context, req *corev1.ShowPolicyRequest) (*corev1.ShowPolicyResponse, error) {
	defer s.trackOperation()()

	// Get policy service
	policyService := policy.NewService(s.engine.db, s.engine.logger)

	// Get the policy
	p, err := policyService.Get(ctx, req.TenantId, req.PolicyId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "policy not found: %v", err)
	}

	// Convert to protobuf format
	protoPolicy, err := s.policyToProto(p)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert policy: %v", err)
	}

	return &corev1.ShowPolicyResponse{
		Policy: protoPolicy,
	}, nil
}

func (s *Server) AddPolicy(ctx context.Context, req *corev1.AddPolicyRequest) (*corev1.AddPolicyResponse, error) {
	defer s.trackOperation()()

	// Get policy service
	policyService := policy.NewService(s.engine.db, s.engine.logger)

	// Create the policy
	createdPolicy, err := policyService.Create(ctx, req.TenantId, req.PolicyName, req.PolicyDescription, "access", "allow", []string{}, []string{}, map[string]interface{}{}, 0, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create policy: %v", err)
	}

	// Convert to protobuf format
	protoPolicy, err := s.policyToProto(createdPolicy)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert policy: %v", err)
	}

	return &corev1.AddPolicyResponse{
		Message: "Policy created successfully",
		Success: true,
		Policy:  protoPolicy,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyPolicy(ctx context.Context, req *corev1.ModifyPolicyRequest) (*corev1.ModifyPolicyResponse, error) {
	defer s.trackOperation()()

	// Get policy service
	policyService := policy.NewService(s.engine.db, s.engine.logger)

	// Build update map
	updates := make(map[string]interface{})
	if req.PolicyNameNew != nil {
		updates["policy_name"] = *req.PolicyNameNew
	}
	if req.PolicyDescription != nil {
		updates["policy_description"] = *req.PolicyDescription
	}
	if req.PolicyObject != nil {
		jsonBytes, err := req.PolicyObject.MarshalJSON()
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.InvalidArgument, "invalid policy object: %v", err)
		}
		updates["policy_object"] = string(jsonBytes)
	}

	// Update the policy
	updatedPolicy, err := policyService.Update(ctx, req.TenantId, req.PolicyId, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update policy: %v", err)
	}

	// Convert to protobuf format
	protoPolicy, err := s.policyToProto(updatedPolicy)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to convert policy: %v", err)
	}

	return &corev1.ModifyPolicyResponse{
		Message: "Policy updated successfully",
		Success: true,
		Policy:  protoPolicy,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeletePolicy(ctx context.Context, req *corev1.DeletePolicyRequest) (*corev1.DeletePolicyResponse, error) {
	defer s.trackOperation()()

	// Get policy service
	policyService := policy.NewService(s.engine.db, s.engine.logger)

	// Delete the policy
	err := policyService.Delete(ctx, req.TenantId, req.PolicyId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete policy: %v", err)
	}

	return &corev1.DeletePolicyResponse{
		Message: "Policy deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
