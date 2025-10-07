package engine

import (
	"context"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/relationship"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// RelationshipService gRPC handlers
// ============================================================================

func (s *Server) ListRelationships(ctx context.Context, req *corev1.ListRelationshipsRequest) (*corev1.ListRelationshipsResponse, error) {
	defer s.trackOperation()()

	// Get workspace ID from workspace name
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get relationship service
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// List relationships for the tenant and workspace
	relationships, err := relationshipService.List(ctx, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list relationships: %v", err)
	}

	// Convert to protobuf format
	protoRelationships := make([]*corev1.Relationship, len(relationships))
	for i, r := range relationships {
		protoRelationships[i] = s.relationshipToProto(r)
	}

	return &corev1.ListRelationshipsResponse{
		Relationships: protoRelationships,
	}, nil
}

func (s *Server) ShowRelationship(ctx context.Context, req *corev1.ShowRelationshipRequest) (*corev1.ShowRelationshipResponse, error) {
	defer s.trackOperation()()

	// Get relationship service
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Get the relationship by name (not ID)
	// First, we need to get the workspace ID from the workspace name
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get the relationship by name
	r, err := relationshipService.GetByName(ctx, req.TenantId, workspaceID, req.RelationshipName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "relationship not found: %v", err)
	}

	// Convert to protobuf format
	protoRelationship := s.relationshipToProto(r)

	return &corev1.ShowRelationshipResponse{
		Relationship: protoRelationship,
	}, nil
}

func (s *Server) AddRelationship(ctx context.Context, req *corev1.AddRelationshipRequest) (*corev1.AddRelationshipResponse, error) {
	defer s.trackOperation()()

	// Only support replication type relationships for now
	if req.RelationshipType != "replication" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "only 'replication' relationship type is currently supported")
	}

	// Get workspace ID from workspace name
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Validate that source database exists
	sourceDbExists, err := s.isDatabaseExists(ctx, req.TenantId, workspaceID, req.RelationshipSourceDatabaseId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to validate source database: %v", err)
	}
	if !sourceDbExists {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database %s not found", req.RelationshipSourceDatabaseId)
	}

	// Validate that target database exists
	targetDbExists, err := s.isDatabaseExists(ctx, req.TenantId, workspaceID, req.RelationshipTargetDatabaseId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to validate target database: %v", err)
	}
	if !targetDbExists {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target database %s not found", req.RelationshipTargetDatabaseId)
	}

	// Get relationship service
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Create the relationship with proper source and target types
	createdRelationship, err := relationshipService.Create(ctx, req.TenantId, workspaceID, req.RelationshipName, req.RelationshipDescription, req.RelationshipType, "table", "table", req.RelationshipSourceDatabaseId, req.RelationshipSourceTableName, req.RelationshipTargetDatabaseId, req.RelationshipTargetTableName, req.MappingId, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create relationship: %v", err)
	}

	// Create replication source for the relationship
	anchorClient := s.engine.GetAnchorClient()
	if anchorClient != nil {
		createReplicationReq := &anchorv1.CreateReplicationSourceRequest{
			TenantId:       req.TenantId,
			WorkspaceId:    workspaceID,
			DatabaseId:     req.RelationshipSourceDatabaseId,
			TableNames:     []string{req.RelationshipSourceTableName},
			RelationshipId: createdRelationship.ID,
		}

		_, err = anchorClient.CreateReplicationSource(ctx, createReplicationReq)
		if err != nil {
			s.engine.IncrementErrors()
			// Log error but don't fail the relationship creation
			s.engine.logger.Errorf("Failed to create replication source for relationship %s: %v", createdRelationship.ID, err)
		} else {
			s.engine.logger.Infof("Successfully created replication source for relationship %s", createdRelationship.ID)
		}
	}

	// Convert to protobuf format
	protoRelationship := s.relationshipToProto(createdRelationship)

	return &corev1.AddRelationshipResponse{
		Message:      "Relationship created successfully",
		Success:      true,
		Relationship: protoRelationship,
		Status:       commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyRelationship(ctx context.Context, req *corev1.ModifyRelationshipRequest) (*corev1.ModifyRelationshipResponse, error) {
	defer s.trackOperation()()

	// Get workspace ID from workspace name
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get relationship service
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Build update map - only include non-nil values
	updates := make(map[string]interface{})
	if req.RelationshipNameNew != nil {
		updates["relationship_name"] = *req.RelationshipNameNew
	}
	if req.RelationshipDescription != nil {
		updates["relationship_description"] = *req.RelationshipDescription
	}
	if req.RelationshipType != nil {
		updates["relationship_type"] = *req.RelationshipType
	}
	if req.RelationshipSourceDatabaseId != nil {
		updates["relationship_source_database_id"] = *req.RelationshipSourceDatabaseId
	}
	if req.RelationshipSourceTableName != nil {
		updates["relationship_source_table_name"] = *req.RelationshipSourceTableName
	}
	if req.RelationshipTargetDatabaseId != nil {
		updates["relationship_target_database_id"] = *req.RelationshipTargetDatabaseId
	}
	if req.RelationshipTargetTableName != nil {
		updates["relationship_target_table_name"] = *req.RelationshipTargetTableName
	}
	if req.MappingId != nil {
		updates["mapping_id"] = *req.MappingId
	}
	if req.PolicyId != nil {
		// Note: This would need to be handled differently since policy_ids is an array
		// For now, we'll skip this field
	}

	// Update the relationship by name
	updatedRelationship, err := relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, req.RelationshipName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update relationship: %v", err)
	}

	// Convert to protobuf format
	protoRelationship := s.relationshipToProto(updatedRelationship)

	return &corev1.ModifyRelationshipResponse{
		Message:      "Relationship updated successfully",
		Success:      true,
		Relationship: protoRelationship,
		Status:       commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteRelationship(ctx context.Context, req *corev1.DeleteRelationshipRequest) (*corev1.DeleteRelationshipResponse, error) {
	defer s.trackOperation()()

	// Get workspace ID from workspace name
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get relationship service
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Delete the relationship by name
	err = relationshipService.DeleteByName(ctx, req.TenantId, workspaceID, req.RelationshipName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete relationship: %v", err)
	}

	return &corev1.DeleteRelationshipResponse{
		Message: "Relationship deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}
