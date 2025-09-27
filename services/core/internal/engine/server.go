package engine

import (
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the main gRPC server that implements all v2 service interfaces
type Server struct {
	// Embed all v2 unimplemented servers to satisfy interface requirements
	corev1.UnimplementedMeshServiceServer
	corev1.UnimplementedWorkspaceServiceServer
	corev1.UnimplementedSatelliteServiceServer
	corev1.UnimplementedAnchorServiceServer
	corev1.UnimplementedRegionServiceServer
	corev1.UnimplementedEnvironmentServiceServer
	corev1.UnimplementedInstanceServiceServer
	corev1.UnimplementedDatabaseServiceServer
	corev1.UnimplementedRepoServiceServer
	corev1.UnimplementedBranchServiceServer
	corev1.UnimplementedCommitServiceServer
	corev1.UnimplementedMappingServiceServer
	corev1.UnimplementedRelationshipServiceServer
	corev1.UnimplementedTransformationServiceServer
	corev1.UnimplementedPolicyServiceServer
	corev1.UnimplementedMCPServiceServer
	corev1.UnimplementedTenantServiceServer
	corev1.UnimplementedUserServiceServer
	corev1.UnimplementedTokenServiceServer
	corev1.UnimplementedGroupServiceServer
	corev1.UnimplementedRoleServiceServer
	corev1.UnimplementedPermissionServiceServer
	corev1.UnimplementedAssignmentServiceServer
	corev1.UnimplementedAuthorizationServiceServer
	corev1.UnimplementedTemplateServiceServer
	corev1.UnimplementedAuditServiceServer
	corev1.UnimplementedImportExportServiceServer

	// Engine reference for tracking operations
	engine *Engine
}

// NewServer creates a new gRPC server with v2 interfaces
func NewServer(engine *Engine) *Server {
	return &Server{
		engine: engine,
	}
}

// Helper method to track operations
func (s *Server) trackOperation() func() {
	s.engine.TrackOperation()
	return s.engine.UntrackOperation
}

// Helper method to return "not implemented" error
func (s *Server) notImplemented(method string) error {
	return status.Errorf(codes.Unimplemented, "method %s not implemented in v2 migration", method)
}
