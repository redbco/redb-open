package engine

import (
	"context"
	"fmt"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/instance"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ============================================================================
// InstanceService gRPC handlers
// ============================================================================

func (s *Server) ListInstances(ctx context.Context, req *corev1.ListInstancesRequest) (*corev1.ListInstancesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// List instances for the tenant and workspace
	instances, err := instanceService.List(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list instances: %v", err)
	}

	// Convert to protobuf format
	protoInstances := make([]*corev1.Instance, len(instances))
	for i, inst := range instances {
		protoInstances[i] = s.instanceToProto(inst)
	}

	return &corev1.ListInstancesResponse{
		Instances: protoInstances,
	}, nil
}

func (s *Server) ShowInstance(ctx context.Context, req *corev1.ShowInstanceRequest) (*corev1.ShowInstanceResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get the instance
	inst, err := instanceService.Get(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "instance not found: %v", err)
	}

	// Convert to protobuf format
	protoInstance := s.instanceToProto(inst)

	return &corev1.ShowInstanceResponse{
		Instance: protoInstance,
	}, nil
}

func (s *Server) ConnectInstance(ctx context.Context, req *corev1.ConnectInstanceRequest) (*corev1.ConnectInstanceResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Extract optional SSL certificate fields
	var sslCert, sslKey, sslRootCert *string
	if req.SslCert != nil && *req.SslCert != "" {
		sslCert = req.SslCert
	}
	if req.SslKey != nil && *req.SslKey != "" {
		sslKey = req.SslKey
	}
	if req.SslRootCert != nil && *req.SslRootCert != "" {
		sslRootCert = req.SslRootCert
	}

	// Create the instance using available fields from ConnectInstanceRequest
	createdInstance, err := instanceService.Create(ctx, req.TenantId, req.WorkspaceName, req.InstanceName, req.InstanceDescription, req.InstanceType, req.InstanceVendor, req.Host, req.Username, req.Password, req.NodeId, req.Port, req.GetEnabled(), req.GetSsl(), req.GetSslMode(), req.GetEnvironmentId(), req.OwnerId, sslCert, sslKey, sslRootCert, nil)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create instance: %v", err)
	}

	// Call anchor service to connect to the instance
	anchorAddr := "localhost:50055" // Default anchor service address
	if s.engine.config != nil {
		if addr := s.engine.config.Get("services.anchor.grpc_address"); addr != "" {
			anchorAddr = addr
		}
	}

	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)
	anchorReq := &anchorv1.ConnectInstanceRequest{
		TenantId:    req.TenantId,
		WorkspaceId: createdInstance.WorkspaceID,
		InstanceId:  createdInstance.ID,
	}

	anchorResp, err := anchorClient.ConnectInstance(ctx, anchorReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect instance via anchor service: %v", err)
	}

	if !anchorResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to connect instance: %s", anchorResp.Message)
	}

	// Convert to protobuf format
	protoInstance := s.instanceToProto(createdInstance)

	return &corev1.ConnectInstanceResponse{
		Message:  fmt.Sprintf("Instance %s created and connected successfully", createdInstance.Name),
		Success:  true,
		Instance: protoInstance,
		Status:   commonv1.Status_STATUS_CREATED,
	}, nil
}

func (s *Server) ReconnectInstance(ctx context.Context, req *corev1.ReconnectInstanceRequest) (*corev1.ReconnectInstanceResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get anchor service
	anchorAddr := "localhost:50055" // Default anchor service address
	if s.engine.config != nil {
		if addr := s.engine.config.Get("services.anchor.grpc_address"); addr != "" {
			anchorAddr = addr
		}
	}

	// Get the instance
	inst, err := instanceService.Get(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "instance not found: %v", err)
	}

	// Enable the instance
	err = instanceService.Enable(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to enable instance: %v", err)
	}

	// Call anchor service to reconnect the instance
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)
	anchorReq := &anchorv1.ConnectInstanceRequest{
		TenantId:    req.TenantId,
		WorkspaceId: inst.WorkspaceID,
		InstanceId:  inst.ID,
	}

	anchorResp, err := anchorClient.ConnectInstance(ctx, anchorReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to reconnect instance via anchor service: %v", err)
	}

	if !anchorResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to reconnect instance: %s", anchorResp.Message)
	}

	return &corev1.ReconnectInstanceResponse{
		Message: fmt.Sprintf("Instance %s enabled successfully", req.InstanceName),
		Success: true,
		Status:  commonv1.Status_STATUS_PENDING,
	}, nil
}

func (s *Server) ModifyInstance(ctx context.Context, req *corev1.ModifyInstanceRequest) (*corev1.ModifyInstanceResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Build updates map
	updates := make(map[string]interface{})
	if req.InstanceNameNew != nil {
		updates["instance_name"] = *req.InstanceNameNew
	}
	if req.InstanceDescription != nil {
		updates["instance_description"] = *req.InstanceDescription
	}
	if req.Host != nil {
		updates["instance_host"] = *req.Host
	}
	if req.Port != nil {
		updates["instance_port"] = *req.Port
	}
	if req.Username != nil {
		updates["instance_username"] = *req.Username
	}
	if req.Password != nil {
		updates["instance_password"] = *req.Password
	}
	if req.Enabled != nil {
		updates["instance_enabled"] = *req.Enabled
	}

	// Update the instance
	updatedInstance, err := instanceService.Update(ctx, req.TenantId, req.WorkspaceName, req.InstanceName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update instance: %v", err)
	}

	// Convert to protobuf format
	protoInstance := s.instanceToProto(updatedInstance)

	return &corev1.ModifyInstanceResponse{
		Message:  fmt.Sprintf("Instance %s updated successfully", updatedInstance.Name),
		Success:  true,
		Instance: protoInstance,
		Status:   commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) DisconnectInstance(ctx context.Context, req *corev1.DisconnectInstanceRequest) (*corev1.DisconnectInstanceResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get the instance to retrieve instance information
	inst, err := instanceService.Get(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "instance not found: %v", err)
	}

	// Call anchor service to disconnect the instance
	anchorAddr := "localhost:50055" // Default anchor service address
	if s.engine.config != nil {
		if addr := s.engine.config.Get("services.anchor.grpc_address"); addr != "" {
			anchorAddr = addr
		}
	}

	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service at %s: %v", anchorAddr, err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)
	anchorReq := &anchorv1.DisconnectInstanceRequest{
		TenantId:    req.TenantId,
		WorkspaceId: inst.WorkspaceID,
		InstanceId:  inst.ID,
	}

	anchorResp, err := anchorClient.DisconnectInstance(ctx, anchorReq)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to disconnect instance via anchor service: %v", err)
	}

	if !anchorResp.Success {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "anchor service failed to disconnect instance: %s", anchorResp.Message)
	}

	// Handle instance based on delete flag
	var message string
	if req.DeleteInstance != nil && *req.DeleteInstance {
		// Delete the instance if requested
		err := instanceService.Delete(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to delete instance: %v", err)
		}
		message = "Instance disconnected and deleted successfully"
	} else {
		// Disable the instance if not deleted
		err := instanceService.Disable(ctx, req.TenantId, req.WorkspaceName, req.InstanceName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to disable instance: %v", err)
		}
		message = "Instance disconnected and disabled successfully"
	}

	return &corev1.DisconnectInstanceResponse{
		Message: message,
		Success: true,
		Status:  commonv1.Status_STATUS_DISCONNECTED,
	}, nil
}
