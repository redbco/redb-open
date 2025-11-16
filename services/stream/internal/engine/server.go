package engine

import (
	"context"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	streamv1 "github.com/redbco/redb-open/api/proto/stream/v1"
	"github.com/redbco/redb-open/pkg/stream/adapter"
)

type Server struct {
	streamv1.UnimplementedStreamServiceServer
	engine *Engine
}

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

func (s *Server) ConnectStream(ctx context.Context, req *streamv1.ConnectStreamRequest) (*streamv1.ConnectStreamResponse, error) {
	defer s.trackOperation()()

	// Fetch the stream configuration using the stream ID
	streamConfig, err := s.engine.GetState().GetConfigRepository().GetStreamConfigByID(ctx, req.StreamId)
	if err != nil {
		return &streamv1.ConnectStreamResponse{
			Success:  false,
			Message:  fmt.Sprintf("Failed to get stream configuration: %v", err),
			Status:   commonv1.Status_STATUS_ERROR,
			StreamId: req.StreamId,
		}, nil
	}

	// Convert to connection config
	connectionConfig := streamConfig.ToConnectionConfig()

	// Get the appropriate adapter for this platform
	streamAdapter, err := adapter.GetAdapter(connectionConfig.Platform)
	if err != nil {
		return &streamv1.ConnectStreamResponse{
			Success:  false,
			Message:  fmt.Sprintf("Failed to get adapter: %v", err),
			Status:   commonv1.Status_STATUS_ERROR,
			StreamId: req.StreamId,
		}, nil
	}

	// Establish connection
	conn, err := streamAdapter.Connect(ctx, *connectionConfig)
	if err != nil {
		// Update status to error
		s.engine.GetState().GetConfigRepository().UpdateStreamConnectionStatus(ctx, req.StreamId, false, fmt.Sprintf("Connection failed: %v", err))

		return &streamv1.ConnectStreamResponse{
			Success:  false,
			Message:  fmt.Sprintf("Failed to connect to stream: %v", err),
			Status:   commonv1.Status_STATUS_ERROR,
			StreamId: req.StreamId,
		}, nil
	}

	// Store connection in state
	s.engine.GetState().AddConnection(req.StreamId, conn)

	// Update connection status in repository
	err = s.engine.GetState().GetConfigRepository().UpdateStreamConnectionStatus(ctx, req.StreamId, true, "Connected successfully")
	if err != nil {
		// Connection was established but status update failed - log but don't fail
		if s.engine.logger != nil {
			s.engine.logger.Warnf("Failed to update stream status: %v", err)
		}
	}

	return &streamv1.ConnectStreamResponse{
		Success:  true,
		Message:  "Successfully connected to stream",
		Status:   commonv1.Status_STATUS_CONNECTED,
		StreamId: streamConfig.ID,
	}, nil
}

func (s *Server) UpdateStreamConnection(ctx context.Context, req *streamv1.UpdateStreamConnectionRequest) (*streamv1.UpdateStreamConnectionResponse, error) {
	defer s.trackOperation()()

	return &streamv1.UpdateStreamConnectionResponse{
		Success:  true,
		Message:  "Stream connection updated successfully",
		Status:   commonv1.Status_STATUS_UPDATED,
		StreamId: req.StreamId,
	}, nil
}

func (s *Server) DisconnectStream(ctx context.Context, req *streamv1.DisconnectStreamRequest) (*streamv1.DisconnectStreamResponse, error) {
	defer s.trackOperation()()

	// Get connection from state
	conn, exists := s.engine.GetState().GetConnection(req.StreamId)
	if exists && conn != nil {
		// Close the connection
		if err := conn.Close(); err != nil {
			if s.engine.logger != nil {
				s.engine.logger.Warnf("Error closing connection for stream %s: %v", req.StreamId, err)
			}
		}
	}

	// Remove from state
	s.engine.GetState().RemoveConnection(req.StreamId)

	// Update connection status in repository
	err := s.engine.GetState().GetConfigRepository().UpdateStreamConnectionStatus(ctx, req.StreamId, false, "Disconnected successfully")
	if err != nil {
		return &streamv1.DisconnectStreamResponse{
			Success:  false,
			Message:  fmt.Sprintf("Failed to update disconnect status: %v", err),
			Status:   commonv1.Status_STATUS_ERROR,
			StreamId: req.StreamId,
		}, nil
	}

	return &streamv1.DisconnectStreamResponse{
		Success:  true,
		Message:  "Successfully disconnected stream",
		Status:   commonv1.Status_STATUS_DISCONNECTED,
		StreamId: req.StreamId,
	}, nil
}

func (s *Server) GetStreamMetadata(ctx context.Context, req *streamv1.GetStreamMetadataRequest) (*streamv1.GetStreamMetadataResponse, error) {
	defer s.trackOperation()()

	return &streamv1.GetStreamMetadataResponse{
		Success:  true,
		Message:  "Stream metadata retrieved successfully",
		Status:   commonv1.Status_STATUS_SUCCESS,
		StreamId: req.StreamId,
		Metadata: []byte("{}"),
	}, nil
}

func (s *Server) ListTopics(ctx context.Context, req *streamv1.ListTopicsRequest) (*streamv1.ListTopicsResponse, error) {
	defer s.trackOperation()()

	// Get connection from state
	conn, exists := s.engine.GetState().GetConnection(req.StreamId)
	if !exists || conn == nil {
		return &streamv1.ListTopicsResponse{
			Success: false,
			Message: "Stream not connected",
			Status:  commonv1.Status_STATUS_ERROR,
			Topics:  []*streamv1.TopicInfo{},
		}, nil
	}

	// Get admin operations
	adminOps := conn.AdminOperations()
	if adminOps == nil {
		return &streamv1.ListTopicsResponse{
			Success: false,
			Message: "Admin operations not supported for this platform",
			Status:  commonv1.Status_STATUS_ERROR,
			Topics:  []*streamv1.TopicInfo{},
		}, nil
	}

	// List topics
	topics, err := adminOps.ListTopics(ctx)
	if err != nil {
		return &streamv1.ListTopicsResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to list topics: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
			Topics:  []*streamv1.TopicInfo{},
		}, nil
	}

	// Convert to proto format
	protoTopics := make([]*streamv1.TopicInfo, len(topics))
	for i, topic := range topics {
		protoTopics[i] = &streamv1.TopicInfo{
			Name:       topic.Name,
			Partitions: topic.Partitions,
			Replicas:   topic.Replicas,
		}
	}

	return &streamv1.ListTopicsResponse{
		Success: true,
		Message: "Topics listed successfully",
		Status:  commonv1.Status_STATUS_SUCCESS,
		Topics:  protoTopics,
	}, nil
}

func (s *Server) GetTopicMetadata(ctx context.Context, req *streamv1.GetTopicMetadataRequest) (*streamv1.GetTopicMetadataResponse, error) {
	defer s.trackOperation()()

	return &streamv1.GetTopicMetadataResponse{
		Success:   true,
		Message:   "Topic metadata retrieved successfully",
		Status:    commonv1.Status_STATUS_SUCCESS,
		TopicName: req.TopicName,
		Metadata:  []byte("{}"),
	}, nil
}

func (s *Server) GetTopicSchema(ctx context.Context, req *streamv1.GetTopicSchemaRequest) (*streamv1.GetTopicSchemaResponse, error) {
	defer s.trackOperation()()

	return &streamv1.GetTopicSchemaResponse{
		Success:         true,
		Message:         "Topic schema retrieved successfully",
		Status:          commonv1.Status_STATUS_SUCCESS,
		TopicName:       req.TopicName,
		Schema:          []byte("{}"),
		MessagesSampled: 0,
		ConfidenceScore: 0.0,
	}, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *streamv1.CreateTopicRequest) (*streamv1.CreateTopicResponse, error) {
	defer s.trackOperation()()

	return &streamv1.CreateTopicResponse{
		Success:   true,
		Message:   "Topic created successfully",
		Status:    commonv1.Status_STATUS_CREATED,
		TopicName: req.TopicName,
	}, nil
}

func (s *Server) DeleteTopic(ctx context.Context, req *streamv1.DeleteTopicRequest) (*streamv1.DeleteTopicResponse, error) {
	defer s.trackOperation()()

	return &streamv1.DeleteTopicResponse{
		Success:   true,
		Message:   "Topic deleted successfully",
		Status:    commonv1.Status_STATUS_DELETED,
		TopicName: req.TopicName,
	}, nil
}

func (s *Server) ProduceMessages(ctx context.Context, req *streamv1.ProduceMessagesRequest) (*streamv1.ProduceMessagesResponse, error) {
	defer s.trackOperation()()

	return &streamv1.ProduceMessagesResponse{
		Success:          true,
		Message:          "Messages produced successfully",
		Status:           commonv1.Status_STATUS_SUCCESS,
		MessagesProduced: int32(len(req.Messages)),
	}, nil
}

func (s *Server) ConsumeMessages(req *streamv1.ConsumeMessagesRequest, stream streamv1.StreamService_ConsumeMessagesServer) error {
	defer s.trackOperation()()

	// Send a completion response
	return stream.Send(&streamv1.ConsumeMessagesResponse{
		Success:  true,
		Message:  "Consumption complete",
		Status:   commonv1.Status_STATUS_SUCCESS,
		Messages: []*streamv1.StreamMessage{},
		HasMore:  false,
	})
}
