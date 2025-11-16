package engine

import (
	"context"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	streamv1 "github.com/redbco/redb-open/api/proto/stream/v1"
	"github.com/redbco/redb-open/services/core/internal/services/stream"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// StreamService implementation

func (s *Server) ListStreams(ctx context.Context, req *corev1.ListStreamsRequest) (*corev1.ListStreamsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	streamService := stream.NewService(s.engine.db.Pool(), s.engine.logger)
	streams, err := streamService.List(ctx, req.TenantId)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ListStreamsResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to list streams: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
			Streams: []*corev1.Stream{}, // Return empty array on error
		}, nil
	}

	// Initialize as empty array instead of nil to ensure proper JSON serialization
	protoStreams := []*corev1.Stream{}
	if len(streams) > 0 {
		protoStreams = make([]*corev1.Stream, len(streams))
		for i, st := range streams {
			protoStreams[i] = streamToProto(st)
		}
	}

	return &corev1.ListStreamsResponse{
		Streams: protoStreams,
		Success: true,
		Message: "Streams listed successfully",
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ShowStream(ctx context.Context, req *corev1.ShowStreamRequest) (*corev1.ShowStreamResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	streamService := stream.NewService(s.engine.db.Pool(), s.engine.logger)
	st, err := streamService.Get(ctx, req.TenantId, req.StreamName)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ShowStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get stream: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	return &corev1.ShowStreamResponse{
		Stream:  streamToProto(st),
		Success: true,
		Message: "Stream retrieved successfully",
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ConnectStream(ctx context.Context, req *corev1.ConnectStreamRequest) (*corev1.ConnectStreamResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get workspace
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	_, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ConnectStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get workspace: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Convert connection config
	connectionConfig := make(map[string]interface{})
	if req.ConnectionConfig != nil {
		connectionConfig = req.ConnectionConfig.AsMap()
	}

	// If no node ID provided, get the local node ID
	nodeID := req.NodeId
	if nodeID == 0 {
		var localNodeID int64
		err := s.engine.db.Pool().QueryRow(ctx, "SELECT identity_id FROM localidentity LIMIT 1").Scan(&localNodeID)
		if err != nil {
			s.engine.IncrementErrors()
			return &corev1.ConnectStreamResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to get local node ID: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
		nodeID = localNodeID
	}

	// Create stream in database
	streamService := stream.NewService(s.engine.db.Pool(), s.engine.logger)
	st, err := streamService.Create(ctx, req.TenantId, req.StreamName, req.StreamDescription,
		req.StreamPlatform, connectionConfig, req.MonitoredTopics, nodeID, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ConnectStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to create stream: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Connect to stream service via gRPC
	streamAddr := s.engine.getServiceAddress("stream")

	if s.engine.logger != nil {
		s.engine.logger.Infof("Connecting to stream service at: %s", streamAddr)
	}

	streamConn, err := grpc.Dial(streamAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ConnectStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to connect to stream service at %s: %v", streamAddr, err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}
	defer streamConn.Close()

	streamClient := streamv1.NewStreamServiceClient(streamConn)
	connectResp, err := streamClient.ConnectStream(ctx, &streamv1.ConnectStreamRequest{
		TenantId: req.TenantId,
		StreamId: st.ID,
	})

	if err != nil || !connectResp.Success {
		s.engine.IncrementErrors()
		return &corev1.ConnectStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to connect stream: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	return &corev1.ConnectStreamResponse{
		Stream:  streamToProto(st),
		Success: true,
		Message: "Stream connected successfully",
		Status:  commonv1.Status_STATUS_CONNECTED,
	}, nil
}

func (s *Server) ReconnectStream(ctx context.Context, req *corev1.ReconnectStreamRequest) (*corev1.ReconnectStreamResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	streamService := stream.NewService(s.engine.db.Pool(), s.engine.logger)
	st, err := streamService.Get(ctx, req.TenantId, req.StreamName)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ReconnectStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get stream: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Reconnect via stream service
	streamAddr := s.engine.getServiceAddress("stream")

	streamConn, err := grpc.Dial(streamAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer streamConn.Close()
		streamClient := streamv1.NewStreamServiceClient(streamConn)
		streamClient.ConnectStream(ctx, &streamv1.ConnectStreamRequest{
			TenantId: req.TenantId,
			StreamId: st.ID,
		})
	}

	return &corev1.ReconnectStreamResponse{
		Stream:  streamToProto(st),
		Success: true,
		Message: "Stream reconnected successfully",
		Status:  commonv1.Status_STATUS_CONNECTED,
	}, nil
}

func (s *Server) ModifyStream(ctx context.Context, req *corev1.ModifyStreamRequest) (*corev1.ModifyStreamResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	streamService := stream.NewService(s.engine.db.Pool(), s.engine.logger)
	st, err := streamService.Get(ctx, req.TenantId, req.StreamName)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.ModifyStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get stream: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// TODO: Implement stream modification logic

	return &corev1.ModifyStreamResponse{
		Stream:  streamToProto(st),
		Success: true,
		Message: "Stream modified successfully",
		Status:  commonv1.Status_STATUS_UPDATED,
	}, nil
}

func (s *Server) DisconnectStream(ctx context.Context, req *corev1.DisconnectStreamRequest) (*corev1.DisconnectStreamResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	streamService := stream.NewService(s.engine.db.Pool(), s.engine.logger)
	st, err := streamService.Get(ctx, req.TenantId, req.StreamName)
	if err != nil {
		s.engine.IncrementErrors()
		return &corev1.DisconnectStreamResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get stream: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// Disconnect via stream service
	streamAddr := s.engine.getServiceAddress("stream")
	streamConn, err := grpc.Dial(streamAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer streamConn.Close()
		streamClient := streamv1.NewStreamServiceClient(streamConn)
		streamClient.DisconnectStream(ctx, &streamv1.DisconnectStreamRequest{
			TenantId: req.TenantId,
			StreamId: st.ID,
		})
	}

	// Delete if requested
	if req.DeleteStream != nil && *req.DeleteStream {
		if err := streamService.Delete(ctx, req.TenantId, req.StreamName); err != nil {
			s.engine.IncrementErrors()
			return &corev1.DisconnectStreamResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to delete stream: %v", err),
				Status:  commonv1.Status_STATUS_ERROR,
			}, nil
		}
	} else {
		streamService.UpdateStatus(ctx, st.ID, "STATUS_DISCONNECTED")
	}

	return &corev1.DisconnectStreamResponse{
		Success: true,
		Message: "Stream disconnected successfully",
		Status:  commonv1.Status_STATUS_DISCONNECTED,
	}, nil
}

func streamToProto(st *stream.Stream) *corev1.Stream {
	configStruct, _ := structpb.NewStruct(st.ConnectionConfig)
	metadataStruct, _ := structpb.NewStruct(st.Metadata)

	regionID := ""
	if st.RegionID != nil {
		regionID = *st.RegionID
	}

	return &corev1.Stream{
		StreamId:          st.ID,
		TenantId:          st.TenantID,
		StreamName:        st.Name,
		StreamDescription: st.Description,
		StreamPlatform:    st.Platform,
		StreamVersion:     st.Version,
		StreamRegionId:    regionID,
		ConnectionConfig:  configStruct,
		CredentialKey:     st.CredentialKey,
		StreamMetadata:    metadataStruct,
		MonitoredTopics:   st.MonitoredTopics,
		ConnectedToNodeId: st.ConnectedToNodeID,
		OwnerId:           st.OwnerID,
		Status:            st.Status,
		Created:           st.Created.Format("2006-01-02T15:04:05Z"),
		Updated:           st.Updated.Format("2006-01-02T15:04:05Z"),
	}
}
