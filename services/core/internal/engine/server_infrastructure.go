package engine

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/services/core/internal/mesh"
	meshsvc "github.com/redbco/redb-open/services/core/internal/services/mesh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// publishMeshEvent publishes a mesh state event
func (s *Server) publishMeshEvent(ctx context.Context, eventType corev1.MeshEventType, affectedNode uint64, metadata map[string]string) {
	eventManager := s.engine.GetMeshEventManager()
	if eventManager == nil {
		s.engine.logger.Warnf("Cannot publish mesh event %s: event manager not available", eventType)
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		event := &mesh.MeshEvent{
			Type:         eventType,
			AffectedNode: affectedNode,
			Metadata:     metadata,
		}

		if err := eventManager.(*mesh.MeshEventManager).PublishEvent(ctx, event); err != nil {
			s.engine.logger.Errorf("Failed to publish mesh event %s for node %d: %v", eventType, affectedNode, err)
		} else {
			s.engine.logger.Debugf("Successfully published mesh event %s for node %d", eventType, affectedNode)
		}
	}()
}

// === Core Mesh Operations ===

// SeedMesh creates a new mesh without connecting to any other nodes
func (s *Server) SeedMesh(ctx context.Context, req *corev1.SeedMeshRequest) (*corev1.SeedMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.MeshName == "" {
		return nil, status.Error(codes.InvalidArgument, "mesh_name is required")
	}

	// Check if node is already in a mesh
	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Check if node is already part of a mesh
	if localNode.Status == "STATUS_ACTIVE" {
		// Check if node has mesh membership
		existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
		if err == nil && len(existingMeshes) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "node is already part of a mesh")
		}
	}

	s.engine.logger.Infof("Seeding new mesh '%s' with local node: %s (ID: %d)", req.MeshName, localNode.Name, localNode.ID)

	// Create the mesh
	createdMesh, err := meshService.Create(ctx, req.MeshName, req.GetMeshDescription(), true)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create mesh: %v", err)
	}

	// Add local node to the mesh
	if err := meshService.AddNodeToMesh(ctx, createdMesh.ID, localNode.ID); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to add node to mesh: %v", err)
	}

	// Update node status to ACTIVE
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_ACTIVE"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to ACTIVE: %v", err)
	}

	s.engine.logger.Infof("Successfully seeded mesh '%s' with node '%s'", createdMesh.Name, localNode.Name)

	// Publish mesh event for node joining the mesh
	s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_NODE_JOINED, uint64(localNode.ID), map[string]string{
		"mesh_id":     fmt.Sprintf("%d", createdMesh.ID),
		"mesh_name":   createdMesh.Name,
		"node_name":   localNode.Name,
		"operation":   "seed_mesh",
		"description": "Node seeded new mesh",
	})

	// Convert to protobuf format
	protoMesh := s.meshToProtoNew(createdMesh)

	return &corev1.SeedMeshResponse{
		Message: fmt.Sprintf("Successfully seeded mesh '%s' with local node '%s'", createdMesh.Name, localNode.Name),
		Success: true,
		Mesh:    protoMesh,
		Status:  commonv1.Status_STATUS_CREATED,
	}, nil
}

// JoinMesh joins an existing mesh by connecting to a node in that mesh
func (s *Server) JoinMesh(ctx context.Context, req *corev1.JoinMeshRequest) (*corev1.JoinMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.TargetAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "target_address is required")
	}

	// Check if node is already in a mesh
	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify node is clean (not in any mesh)
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err == nil && len(existingMeshes) > 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is already part of a mesh")
	}

	// Set default strategy
	strategy := req.GetStrategy()
	if strategy == corev1.JoinStrategy_JOIN_STRATEGY_UNSPECIFIED {
		strategy = corev1.JoinStrategy_JOIN_STRATEGY_INHERIT
	}

	// Set default timeout
	timeout := req.GetTimeoutSeconds()
	if timeout == 0 {
		timeout = 30
	}

	s.engine.logger.Infof("Joining mesh via %s with strategy %s (timeout: %ds)", req.TargetAddress, strategy, timeout)

	// Update node status to JOINING
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_JOINING"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to JOINING: %v", err)
	}

	// Attempt to connect to the target node
	addSessionResp, err := s.AddMeshSession(ctx, req.TargetAddress, int32(timeout))
	if err != nil {
		// Reset node status to CLEAN on failure
		meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_CLEAN")
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %v", err)
	}

	if !addSessionResp.Success {
		// Reset node status to CLEAN on failure
		meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_CLEAN")
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %s", addSessionResp.Message)
	}

	s.engine.logger.Infof("Mesh session established with node %d, initiating synchronization", addSessionResp.PeerNodeId)

	// Get mesh communication manager
	meshMgrInterface := s.engine.GetMeshManager()
	if meshMgr, ok := meshMgrInterface.(*mesh.MeshCommunicationManager); ok && meshMgr != nil {
		// Wait briefly for the mesh routing state to stabilize after session establishment
		// The Rust SessionManager needs time to update its internal routing tables
		time.Sleep(100 * time.Millisecond)

		// Request mesh sync from seed node via mesh messaging
		syncReq := &mesh.CoreMessage{
			Type:      mesh.MessageTypeMeshSyncRequest,
			Operation: "request_sync",
			Data: map[string]interface{}{
				"include_mesh":   true,
				"include_nodes":  true,
				"include_routes": true,
			},
			Timestamp: time.Now().Unix(),
		}

		// Send sync request (non-blocking)
		go func() {
			s.engine.logger.Infof("Sending mesh sync request to node %d", addSessionResp.PeerNodeId)
			_, err := meshMgr.SendMessage(context.Background(), addSessionResp.PeerNodeId, syncReq)
			if err != nil {
				s.engine.logger.Warnf("Failed to send mesh sync request: %v", err)
			} else {
				s.engine.logger.Infof("Mesh sync request sent to node %d", addSessionResp.PeerNodeId)
			}
		}()

		// Notify seed about local node joining
		joinNotify := &mesh.CoreMessage{
			Type:      mesh.MessageTypeNodeJoinNotify,
			Operation: "node_joined",
			Data: map[string]interface{}{
				"node_id":          localNode.ID,
				"node_name":        localNode.Name,
				"node_description": localNode.Description,
				"node_public_key":  localNode.PublicKey, // Keep as string to avoid double-encoding
				"ip_address":       localNode.IPAddress,
				"port":             localNode.Port,
				"status":           "STATUS_ACTIVE",
				"seed_node":        false,
			},
			Timestamp: time.Now().Unix(),
		}

		// Send join notification (non-blocking)
		go func() {
			s.engine.logger.Infof("Sending join notification to node %d", addSessionResp.PeerNodeId)
			_, err := meshMgr.SendMessage(context.Background(), addSessionResp.PeerNodeId, joinNotify)
			if err != nil {
				s.engine.logger.Warnf("Failed to send join notification: %v", err)
			} else {
				s.engine.logger.Infof("Join notification sent to node %d", addSessionResp.PeerNodeId)
			}
		}()
	} else {
		s.engine.logger.Warn("Mesh communication manager not available - skipping sync")
	}

	// Synchronization happens in background via mesh messaging
	// The handlers will apply the synced mesh, nodes, and routes data
	s.engine.logger.Infof("Mesh join initiated - synchronization in progress via mesh messaging")

	// Update node status to ACTIVE
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_ACTIVE"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to ACTIVE: %v", err)
	}

	s.engine.logger.Infof("Successfully joined mesh via peer node %d at %s", addSessionResp.PeerNodeId, addSessionResp.RemoteAddr)

	// Publish mesh event for node joining the mesh
	s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_NODE_JOINED, uint64(localNode.ID), map[string]string{
		"node_name":     localNode.Name,
		"peer_node_id":  fmt.Sprintf("%d", addSessionResp.PeerNodeId),
		"target_addr":   req.TargetAddress,
		"operation":     "join_mesh",
		"description":   "Node joined existing mesh - synchronization in progress",
		"join_strategy": req.Strategy.String(),
	})

	return &corev1.JoinMeshResponse{
		Message:    fmt.Sprintf("Successfully joined mesh via peer node %d - synchronization in progress", addSessionResp.PeerNodeId),
		Success:    true,
		Mesh:       nil, // Mesh data will be populated by sync
		PeerNodeId: addSessionResp.PeerNodeId,
		RemoteAddr: addSessionResp.RemoteAddr,
		Status:     commonv1.Status_STATUS_CREATED,
	}, nil
}

// ExtendMesh extends the current mesh to a clean node
func (s *Server) ExtendMesh(ctx context.Context, req *corev1.ExtendMeshRequest) (*corev1.ExtendMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Validate required fields
	if req.TargetAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "target_address is required")
	}

	// Check if node is in a mesh
	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	// Set default strategy
	strategy := req.GetStrategy()
	if strategy == corev1.JoinStrategy_JOIN_STRATEGY_UNSPECIFIED {
		strategy = corev1.JoinStrategy_JOIN_STRATEGY_INHERIT
	}

	// Set default timeout
	timeout := req.GetTimeoutSeconds()
	if timeout == 0 {
		timeout = 30
	}

	s.engine.logger.Infof("Extending mesh to %s with strategy %s (timeout: %ds)", req.TargetAddress, strategy, timeout)

	// Attempt to connect to the target node
	addSessionResp, err := s.AddMeshSession(ctx, req.TargetAddress, int32(timeout))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %v", err)
	}

	if !addSessionResp.Success {
		return nil, status.Errorf(codes.Internal, "failed to connect to target node: %s", addSessionResp.Message)
	}

	// TODO: Synchronize mesh configuration to the target node based on strategy
	// For now, skip synchronization - will be handled by background sync
	s.engine.logger.Infof("Skipping data synchronization to target node - will sync in background")

	// TODO: Register the target node with all existing mesh members
	// For now, skip registration - nodes will discover each other via topology updates
	s.engine.logger.Infof("Skipping target node registration - nodes will discover via topology")

	s.engine.logger.Infof("Successfully extended mesh to peer node %d at %s", addSessionResp.PeerNodeId, addSessionResp.RemoteAddr)

	// Publish mesh event for extending mesh to new node
	s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_NODE_JOINED, addSessionResp.PeerNodeId, map[string]string{
		"mesh_id":         fmt.Sprintf("%d", existingMeshes[0].ID),
		"mesh_name":       existingMeshes[0].Name,
		"target_addr":     req.TargetAddress,
		"operation":       "extend_mesh",
		"description":     "Mesh extended to new node",
		"extend_strategy": req.Strategy.String(),
		"local_node":      localNode.Name,
	})

	return &corev1.ExtendMeshResponse{
		Message:    fmt.Sprintf("Successfully extended mesh to peer node %d", addSessionResp.PeerNodeId),
		Success:    true,
		PeerNodeId: addSessionResp.PeerNodeId,
		RemoteAddr: addSessionResp.RemoteAddr,
		Status:     commonv1.Status_STATUS_CREATED,
	}, nil
}

// LeaveMesh removes the current node from its mesh
func (s *Server) LeaveMesh(ctx context.Context, req *corev1.LeaveMeshRequest) (*corev1.LeaveMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	s.engine.logger.Infof("Node %s leaving mesh", localNode.Name)

	// Update node status to LEAVING
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_LEAVING"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to LEAVING: %v", err)
	}

	// Get all current connections
	connectionsDropped := 0
	if s.engine.meshControlClient != nil {
		// Get current sessions from mesh service
		sessionsResp, err := s.engine.meshControlClient.GetSessions(ctx, &meshv1.GetSessionsRequest{})
		if err == nil {
			for _, session := range sessionsResp.Sessions {
				// Drop each connection
				dropResp, err := s.DropMeshSession(ctx, session.PeerNodeId)
				if err != nil {
					s.engine.logger.Warnf("Failed to drop connection to node %d: %v", session.PeerNodeId, err)
					if !req.Force {
						return nil, status.Errorf(codes.Internal, "failed to drop connection to node %d: %v", session.PeerNodeId, err)
					}
				} else if dropResp.Success {
					connectionsDropped++
				}
			}
		}
	}

	// Remove node from mesh
	for _, mesh := range existingMeshes {
		if err := meshService.RemoveNodeFromMesh(ctx, mesh.ID, localNode.ID); err != nil {
			s.engine.logger.Warnf("Failed to remove node from mesh %d: %v", mesh.ID, err)
			if !req.Force {
				return nil, status.Errorf(codes.Internal, "failed to remove node from mesh: %v", err)
			}
		}
	}

	// Update node status to CLEAN
	if err := meshService.UpdateNodeStatus(ctx, localNode.ID, "STATUS_CLEAN"); err != nil {
		s.engine.logger.Warnf("Failed to set node status to CLEAN: %v", err)
	}

	s.engine.logger.Infof("Successfully left mesh, dropped %d connections", connectionsDropped)

	// Publish mesh event for node leaving the mesh
	if len(existingMeshes) > 0 {
		s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_NODE_LEFT, uint64(localNode.ID), map[string]string{
			"mesh_id":             fmt.Sprintf("%d", existingMeshes[0].ID),
			"mesh_name":           existingMeshes[0].Name,
			"node_name":           localNode.Name,
			"operation":           "leave_mesh",
			"description":         "Node left mesh gracefully",
			"connections_dropped": fmt.Sprintf("%d", connectionsDropped),
		})
	}

	return &corev1.LeaveMeshResponse{
		Message:            fmt.Sprintf("Successfully left mesh, dropped %d connections", connectionsDropped),
		Success:            true,
		ConnectionsDropped: int32(connectionsDropped),
		Status:             commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// EvictNode removes another node from the mesh by force
func (s *Server) EvictNode(ctx context.Context, req *corev1.EvictNodeRequest) (*corev1.EvictNodeResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.TargetNodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "target_node_id is required")
	}

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify local node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	s.engine.logger.Infof("Evicting node %d from mesh", req.TargetNodeId)

	// Drop connection to the target node
	targetCleaned := false
	if s.engine.meshControlClient != nil {
		dropResp, err := s.DropMeshSession(ctx, req.TargetNodeId)
		if err != nil {
			s.engine.logger.Warnf("Failed to drop connection to target node %d: %v", req.TargetNodeId, err)
		} else if dropResp.Success {
			s.engine.logger.Infof("Successfully dropped connection to node %d", req.TargetNodeId)
		}

		// TODO: If req.CleanTarget is true, send a command to clean the target node
		// This would require implementing a mesh protocol command
		targetCleaned = req.CleanTarget
	}

	// Remove target node from mesh (local database)
	targetNodeID := int64(req.TargetNodeId)
	for _, mesh := range existingMeshes {
		if err := meshService.RemoveNodeFromMesh(ctx, mesh.ID, targetNodeID); err != nil {
			s.engine.logger.Warnf("Failed to remove target node from mesh %d: %v", mesh.ID, err)
		}
	}

	s.engine.logger.Infof("Successfully evicted node %d from mesh", req.TargetNodeId)

	// Publish mesh event for node eviction
	if len(existingMeshes) > 0 {
		s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_NODE_LEFT, req.TargetNodeId, map[string]string{
			"mesh_id":        fmt.Sprintf("%d", existingMeshes[0].ID),
			"mesh_name":      existingMeshes[0].Name,
			"operation":      "evict_node",
			"description":    "Node evicted from mesh",
			"target_cleaned": fmt.Sprintf("%t", targetCleaned),
			"evicted_by":     localNode.Name,
		})
	}

	return &corev1.EvictNodeResponse{
		Message:       fmt.Sprintf("Successfully evicted node %d from mesh", req.TargetNodeId),
		Success:       true,
		TargetCleaned: targetCleaned,
		Status:        commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// === Connection Management ===

// AddConnection adds a connection to another node in the same mesh
func (s *Server) AddConnection(ctx context.Context, req *corev1.AddConnectionRequest) (*corev1.AddConnectionResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.TargetNodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "target_node_id is required")
	}

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Verify local node is part of a mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "node is not part of any mesh")
	}

	// Get target node information from database
	// Query target node details from nodes table
	var targetIPAddress string
	var targetPort int32
	var targetNodeName string
	var targetStatus string

	nodeQuery := `
		SELECT node_name, ip_address, port, status 
		FROM nodes 
		WHERE node_id = $1
	`

	err = s.engine.db.Pool().QueryRow(ctx, nodeQuery, req.TargetNodeId).Scan(
		&targetNodeName, &targetIPAddress, &targetPort, &targetStatus,
	)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "target node %d not found in database: %v", req.TargetNodeId, err)
	}

	// Verify target node is active
	if targetStatus != "STATUS_ACTIVE" {
		return nil, status.Errorf(codes.FailedPrecondition, "target node %d is not active (status: %s)", req.TargetNodeId, targetStatus)
	}

	// Verify target node is in the same mesh
	targetMeshes, err := meshService.GetMeshesByNodeID(ctx, int64(req.TargetNodeId))
	if err != nil || len(targetMeshes) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "target node %d is not part of any mesh", req.TargetNodeId)
	}

	// Check if target node is in the same mesh as local node
	sameSharedMesh := false
	for _, localMesh := range existingMeshes {
		for _, targetMesh := range targetMeshes {
			if localMesh.ID == targetMesh.ID {
				sameSharedMesh = true
				break
			}
		}
		if sameSharedMesh {
			break
		}
	}

	if !sameSharedMesh {
		return nil, status.Errorf(codes.FailedPrecondition, "target node %d is not in the same mesh as local node", req.TargetNodeId)
	}

	// Construct target address
	targetAddress := fmt.Sprintf("%s:%d", targetIPAddress, targetPort)

	s.engine.logger.Infof("Adding connection to node %d (%s) at %s", req.TargetNodeId, targetNodeName, targetAddress)

	// Set default timeout if not provided
	timeout := req.GetTimeoutSeconds()
	if timeout == 0 {
		timeout = 30
	}

	// Use AddMeshSession to establish the connection
	addSessionResp, err := s.AddMeshSession(ctx, targetAddress, int32(timeout))
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to add connection to node %d: %v", req.TargetNodeId, err)
	}

	var statusCode commonv1.Status
	if addSessionResp.Success {
		statusCode = commonv1.Status_STATUS_SUCCESS
		s.engine.logger.Infof("Successfully added connection to node %d (%s)", req.TargetNodeId, targetNodeName)
	} else {
		statusCode = commonv1.Status_STATUS_ERROR
		s.engine.logger.Warnf("Failed to add connection to node %d (%s): %s", req.TargetNodeId, targetNodeName, addSessionResp.Message)
	}

	// Create connection object for response
	var connection *corev1.Connection
	if addSessionResp.Success {
		connection = &corev1.Connection{
			PeerNodeId:   req.TargetNodeId,
			PeerNodeName: targetNodeName,
			RemoteAddr:   addSessionResp.RemoteAddr,
			Status:       corev1.ConnectionStatus_CONNECTION_STATUS_CONNECTED,
			// TODO: Add RTT, bytes sent/received, TLS info when available
		}
	}

	return &corev1.AddConnectionResponse{
		Message:    addSessionResp.Message,
		Success:    addSessionResp.Success,
		Connection: connection,
		Status:     statusCode,
	}, nil
}

// DropConnection drops a connection to another node
func (s *Server) DropConnection(ctx context.Context, req *corev1.DropConnectionRequest) (*corev1.DropConnectionResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.PeerNodeId == 0 {
		return nil, status.Error(codes.InvalidArgument, "peer_node_id is required")
	}

	s.engine.logger.Infof("Dropping connection to node %d", req.PeerNodeId)

	// Use the existing DropMeshSession method
	dropResp, err := s.DropMeshSession(ctx, req.PeerNodeId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to drop connection: %v", err)
	}

	var statusCode commonv1.Status
	if dropResp.Success {
		statusCode = commonv1.Status_STATUS_SUCCESS
	} else {
		statusCode = commonv1.Status_STATUS_ERROR
	}

	return &corev1.DropConnectionResponse{
		Message: dropResp.Message,
		Success: dropResp.Success,
		Status:  statusCode,
	}, nil
}

// ListConnections lists all active connections
func (s *Server) ListConnections(ctx context.Context, req *corev1.ListConnectionsRequest) (*corev1.ListConnectionsResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	var connections []*corev1.Connection

	if s.engine.meshControlClient != nil {
		// Get current sessions from mesh service
		sessionsResp, err := s.engine.meshControlClient.GetSessions(ctx, &meshv1.GetSessionsRequest{})
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to get sessions: %v", err)
		}

		// Convert mesh sessions to connection objects
		for _, session := range sessionsResp.Sessions {
			connection := &corev1.Connection{
				PeerNodeId:      session.PeerNodeId,
				PeerNodeName:    fmt.Sprintf("node-%d", session.PeerNodeId), // TODO: Get actual name
				RemoteAddr:      session.RemoteAddr,
				Status:          corev1.ConnectionStatus_CONNECTION_STATUS_CONNECTED, // TODO: Map actual status
				RttMicroseconds: session.RttMicroseconds,
				BytesSent:       session.BytesSent,
				BytesReceived:   session.BytesReceived,
				IsTls:           session.IsTls,
				ConnectedAt:     time.Now().Unix(), // TODO: Get actual connection time
			}
			connections = append(connections, connection)
		}
	}

	return &corev1.ListConnectionsResponse{
		Connections: connections,
	}, nil
}

// === Information and Status ===

// ShowMesh shows the current mesh information
func (s *Server) ShowMesh(ctx context.Context, req *corev1.ShowMeshRequest) (*corev1.ShowMeshResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Get meshes for local node
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.NotFound, "node is not part of any mesh")
	}

	// Return the first mesh (assuming single mesh membership for now)
	mesh := existingMeshes[0]
	protoMesh := s.meshToProto(mesh)

	return &corev1.ShowMeshResponse{
		Mesh: protoMesh,
	}, nil
}

// ListNodes lists all nodes in the current mesh
func (s *Server) ListNodes(ctx context.Context, req *corev1.ListNodesRequest) (*corev1.ListNodesResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	// Get meshes for local node
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err != nil || len(existingMeshes) == 0 {
		return nil, status.Error(codes.NotFound, "node is not part of any mesh")
	}

	// Get nodes for the first mesh
	mesh := existingMeshes[0]
	nodes, err := meshService.GetNodes(ctx, mesh.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get nodes: %v", err)
	}

	// Convert to protobuf format
	var protoNodes []*corev1.Node
	for _, node := range nodes {
		protoNode := s.nodeToProtoNew(node)
		protoNodes = append(protoNodes, protoNode)
	}

	return &corev1.ListNodesResponse{
		Nodes: protoNodes,
	}, nil
}

// ShowNode shows information about a specific node
func (s *Server) ShowNode(ctx context.Context, req *corev1.ShowNodeRequest) (*corev1.ShowNodeResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)

	var node *meshsvc.Node
	var err error

	if req.NodeId != nil && *req.NodeId != 0 {
		// Show specific node
		node, err = meshService.GetNodeByID(ctx, int64(*req.NodeId))
	} else {
		// Show local node
		node, err = meshService.GetLocalNode(ctx)
	}

	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get node: %v", err)
	}

	protoNode := s.nodeToProtoNew(node)

	return &corev1.ShowNodeResponse{
		Node: protoNode,
	}, nil
}

// GetNodeStatus gets the status of the current node
func (s *Server) GetNodeStatus(ctx context.Context, req *corev1.GetNodeStatusRequest) (*corev1.GetNodeStatusResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)
	localNode, err := meshService.GetLocalNode(ctx)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get local node: %v", err)
	}

	protoNode := s.nodeToProtoNew(localNode)

	// Get connections
	connectionsResp, err := s.ListConnections(ctx, &corev1.ListConnectionsRequest{})
	if err != nil {
		s.engine.logger.Warnf("Failed to get connections: %v", err)
		connectionsResp = &corev1.ListConnectionsResponse{Connections: []*corev1.Connection{}}
	}

	// Get mesh information if node is in a mesh
	var protoMesh *corev1.Mesh
	existingMeshes, err := meshService.GetMeshesByNodeID(ctx, localNode.ID)
	if err == nil && len(existingMeshes) > 0 {
		mesh := existingMeshes[0]
		protoMesh = s.meshToProtoNew(mesh)
	}

	return &corev1.GetNodeStatusResponse{
		Node:        protoNode,
		Connections: connectionsResp.Connections,
		Mesh:        protoMesh,
	}, nil
}

// === Helper Methods ===

// meshToProtoNew converts a mesh service object to new protobuf format
func (s *Server) meshToProtoNew(mesh *meshsvc.Mesh) *corev1.Mesh {
	// Convert enum to boolean: 'OPEN' -> true, others -> false
	allowJoin := mesh.AllowJoin == "OPEN"

	return &corev1.Mesh{
		MeshId:          fmt.Sprintf("%d", mesh.ID),
		MeshName:        mesh.Name,
		MeshDescription: mesh.Description,
		AllowJoin:       allowJoin,
		NodeCount:       mesh.NodeCount,
		ConnectionCount: 0, // TODO: Calculate actual connection count
		Status:          s.convertStatus(mesh.Status),
		CreatedAt:       mesh.Created.Unix(),
		UpdatedAt:       mesh.Updated.Unix(),
	}
}

// nodeToProtoNew converts a node service object to new protobuf format
func (s *Server) nodeToProtoNew(node *meshsvc.Node) *corev1.Node {
	// Map status to NodeStatus enum
	var nodeStatus corev1.NodeStatus
	switch node.Status {
	case "STATUS_CLEAN":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_CLEAN
	case "STATUS_JOINING":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_JOINING
	case "STATUS_ACTIVE":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_ACTIVE
	case "STATUS_LEAVING":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_LEAVING
	case "STATUS_OFFLINE":
		nodeStatus = corev1.NodeStatus_NODE_STATUS_OFFLINE
	default:
		nodeStatus = corev1.NodeStatus_NODE_STATUS_UNSPECIFIED
	}

	protoNode := &corev1.Node{
		NodeId:          fmt.Sprintf("%d", node.ID),
		NodeName:        node.Name,
		NodeDescription: node.Description,
		NodePlatform:    node.Platform,
		NodeVersion:     node.Version,
		RegionId:        node.RegionID,
		RegionName:      node.RegionName,
		IpAddress:       node.IPAddress,
		Port:            node.Port,
		NodeStatus:      nodeStatus,
		CreatedAt:       node.Created.Unix(),
		UpdatedAt:       node.Updated.Unix(),
	}

	// TODO: Set mesh_id if node is part of a mesh

	return protoNode
}

// convertStatus converts a string status to protobuf Status
func (s *Server) convertStatus(status string) commonv1.Status {
	switch status {
	case "STATUS_CREATED":
		return commonv1.Status_STATUS_CREATED
	case "STATUS_ACTIVE":
		return commonv1.Status_STATUS_STARTED
	case "STATUS_PENDING":
		return commonv1.Status_STATUS_PENDING
	case "STATUS_ERROR":
		return commonv1.Status_STATUS_ERROR
	default:
		return commonv1.Status_STATUS_UNKNOWN
	}
}

// === Mesh Synchronization Helper Functions ===

// MeshInfo represents discovered mesh information
type MeshInfo struct {
	ID          string
	Name        string
	Description string
	Nodes       []*meshsvc.Node
}

// discoverMeshFromPeer discovers mesh information from a connected peer
func (s *Server) discoverMeshFromPeer(ctx context.Context, peerNodeID uint64) (*MeshInfo, error) {
	s.engine.logger.Debugf("Discovering mesh information from peer node %d", peerNodeID)

	// Use the database sync manager to request mesh information
	syncManager := s.engine.GetDatabaseSyncManager()
	if syncManager == nil {
		return nil, fmt.Errorf("database sync manager not available")
	}

	// Request mesh table data from the peer
	meshResponse, err := s.engine.GetMeshManager().(*mesh.MeshCommunicationManager).RequestDatabaseSync(ctx, &meshv1.DatabaseSyncRequest{
		TableName:        "mesh",
		LastKnownVersion: 0,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sync mesh data: %w", err)
	}

	if len(meshResponse.Records) == 0 {
		return nil, fmt.Errorf("no mesh data received from peer")
	}

	// Parse the first mesh record (assuming single mesh)
	meshRecord := meshResponse.Records[0]
	meshInfo := &MeshInfo{
		ID:          meshRecord.Data["mesh_id"],
		Name:        meshRecord.Data["mesh_name"],
		Description: meshRecord.Data["mesh_description"],
	}

	// Request nodes table data
	nodesResponse, err := s.engine.GetMeshManager().(*mesh.MeshCommunicationManager).RequestDatabaseSync(ctx, &meshv1.DatabaseSyncRequest{
		TableName:        "nodes",
		LastKnownVersion: 0,
	})
	if err != nil {
		s.engine.logger.Warnf("Failed to sync nodes data: %v", err)
		// Continue without nodes data
		return meshInfo, nil
	}

	// Parse nodes data
	for _, nodeRecord := range nodesResponse.Records {
		port, _ := strconv.Atoi(nodeRecord.Data["port"])
		nodeID, _ := strconv.ParseInt(nodeRecord.Data["node_id"], 10, 64)
		node := &meshsvc.Node{
			ID:        nodeID,
			Name:      nodeRecord.Data["node_name"],
			IPAddress: nodeRecord.Data["ip_address"],
			Port:      int32(port),
			Status:    nodeRecord.Data["status"],
		}
		meshInfo.Nodes = append(meshInfo.Nodes, node)
	}

	s.engine.logger.Infof("Discovered mesh '%s' with %d nodes", meshInfo.Name, len(meshInfo.Nodes))
	return meshInfo, nil
}

// synchronizeDataFromMesh synchronizes data from the mesh based on strategy
func (s *Server) synchronizeDataFromMesh(ctx context.Context, strategy corev1.JoinStrategy, peerNodeID uint64, meshID int64) error {
	s.engine.logger.Infof("Synchronizing data from mesh using strategy: %s", strategy)

	syncManager := s.engine.GetDatabaseSyncManager()
	if syncManager == nil {
		return fmt.Errorf("database sync manager not available")
	}

	switch strategy {
	case corev1.JoinStrategy_JOIN_STRATEGY_INHERIT:
		// Inherit: Replace local data with mesh data
		return s.inheritDataFromMesh(ctx, peerNodeID, meshID)

	case corev1.JoinStrategy_JOIN_STRATEGY_MERGE:
		// Merge: Combine local and mesh data
		return s.mergeDataWithMesh(ctx, peerNodeID, meshID)

	case corev1.JoinStrategy_JOIN_STRATEGY_OVERWRITE:
		// Overwrite: Push local data to mesh (handled by mesh members)
		return s.overwriteMeshData(ctx, peerNodeID, meshID)

	default:
		return fmt.Errorf("unsupported join strategy: %s", strategy)
	}
}

// synchronizeDataToNode synchronizes data to a target node based on strategy
func (s *Server) synchronizeDataToNode(ctx context.Context, strategy corev1.JoinStrategy, targetNodeID uint64, meshID int64) error {
	s.engine.logger.Infof("Synchronizing data to node %d using strategy: %s", targetNodeID, strategy)

	// For extend operations, we always push our mesh data to the target node
	// The strategy determines how the target node handles the data

	syncManager := s.engine.GetDatabaseSyncManager()
	if syncManager == nil {
		return fmt.Errorf("database sync manager not available")
	}

	// Get all mesh-related data to sync
	tablesToSync := []string{"mesh", "nodes", "mesh_node_membership"}

	for _, table := range tablesToSync {
		if err := s.syncTableToNode(ctx, table, targetNodeID); err != nil {
			s.engine.logger.Warnf("Failed to sync table %s to node %d: %v", table, targetNodeID, err)
		}
	}

	return nil
}

// registerNodeWithMeshMembers registers a node with all existing mesh members
func (s *Server) registerNodeWithMeshMembers(ctx context.Context, meshID int64, node *meshsvc.Node) error {
	s.engine.logger.Infof("Registering node %s with all mesh members", node.Name)

	meshService := meshsvc.NewService(s.engine.db, s.engine.logger)

	// Add the new node to our local database (nodes are managed via mesh membership)
	// The AddNode method doesn't exist, nodes are added via mesh membership

	// Add node to mesh membership
	if err := meshService.AddNodeToMesh(ctx, meshID, node.ID); err != nil {
		s.engine.logger.Warnf("Failed to add node to mesh membership: %v", err)
	}

	// Broadcast node addition to all existing mesh members
	nodeAddedEvent := &mesh.MeshEvent{
		Type:         corev1.MeshEventType_MESH_EVENT_NODE_JOINED,
		AffectedNode: uint64(node.ID),
		Metadata: map[string]string{
			"mesh_id":     fmt.Sprintf("%d", meshID),
			"node_name":   node.Name,
			"ip_address":  node.IPAddress,
			"port":        fmt.Sprintf("%d", node.Port),
			"operation":   "node_registration",
			"description": "New node registered with mesh",
		},
	}

	eventManager := s.engine.GetMeshEventManager()
	if eventManager != nil {
		if err := eventManager.(*mesh.MeshEventManager).PublishEvent(ctx, nodeAddedEvent); err != nil {
			s.engine.logger.Warnf("Failed to broadcast node registration event: %v", err)
		}
	}

	return nil
}

// Helper functions for data synchronization strategies

func (s *Server) inheritDataFromMesh(ctx context.Context, peerNodeID uint64, meshID int64) error {
	// Request all data from mesh and replace local data
	tablesToSync := []string{"workspaces", "satellites", "environments", "instances", "databases"}

	for _, table := range tablesToSync {
		if err := s.syncTableFromPeer(ctx, table, peerNodeID); err != nil {
			s.engine.logger.Warnf("Failed to inherit %s data: %v", table, err)
		}
	}

	return nil
}

func (s *Server) mergeDataWithMesh(ctx context.Context, peerNodeID uint64, meshID int64) error {
	// Implement merge logic - combine local and remote data
	// This is complex and would require conflict resolution
	s.engine.logger.Infof("Merge strategy not fully implemented - using inherit strategy")
	return s.inheritDataFromMesh(ctx, peerNodeID, meshID)
}

func (s *Server) overwriteMeshData(ctx context.Context, peerNodeID uint64, meshID int64) error {
	// Push local data to mesh - this would be handled by the mesh members
	s.engine.logger.Infof("Overwrite strategy: local data will be pushed to mesh members")
	return nil
}

func (s *Server) syncTableToNode(ctx context.Context, tableName string, targetNodeID uint64) error {
	// Use mesh manager to send table data to target node
	meshManager := s.engine.GetMeshManager()
	if meshManager == nil {
		return fmt.Errorf("mesh manager not available")
	}

	// This would use the database sync request mechanism
	s.engine.logger.Debugf("Syncing table %s to node %d", tableName, targetNodeID)
	return nil
}

func (s *Server) syncTableFromPeer(ctx context.Context, tableName string, peerNodeID uint64) error {
	// Request table data from peer node
	meshManager := s.engine.GetMeshManager()
	if meshManager == nil {
		return fmt.Errorf("mesh manager not available")
	}

	s.engine.logger.Debugf("Syncing table %s from peer %d", tableName, peerNodeID)
	return nil
}

// Utility functions

// resolveAddress resolves a hostname:port address to IP:port format
// This is needed because the Rust mesh service requires IP addresses, not hostnames
func resolveAddress(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid address format: %w", err)
	}

	// Check if it's already an IP address
	if ip := net.ParseIP(host); ip != nil {
		return addr, nil
	}

	// Resolve hostname to IP address
	ips, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("failed to resolve hostname %s: %w", host, err)
	}

	if len(ips) == 0 {
		return "", fmt.Errorf("no IP addresses found for hostname %s", host)
	}

	// Prefer IPv4 addresses
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			return net.JoinHostPort(ipv4.String(), port), nil
		}
	}

	// Fallback to first IP (IPv6)
	return net.JoinHostPort(ips[0].String(), port), nil
}

func extractIPFromAddress(addr string) string {
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

func extractPortFromAddress(addr string) int {
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		if port, err := strconv.Atoi(addr[idx+1:]); err == nil {
			return port
		}
	}
	return 0
}

// === Mesh Integration Functions ===

// AddMeshSession adds a new session to another node
func (s *Server) AddMeshSession(ctx context.Context, targetAddress string, timeoutSeconds int32) (*meshv1.AddSessionResponse, error) {
	if s.engine.meshControlClient == nil {
		return nil, fmt.Errorf("mesh control client not available")
	}

	// Resolve hostname to IP address if needed (mesh service requires IP:port format)
	resolvedAddress, err := resolveAddress(targetAddress)
	if err != nil {
		s.engine.logger.Errorf("Failed to resolve address %s: %v", targetAddress, err)
		return nil, fmt.Errorf("failed to resolve address: %w", err)
	}

	s.engine.logger.Infof("Adding mesh session to %s (resolved to %s) with timeout %ds", targetAddress, resolvedAddress, timeoutSeconds)

	// Create context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	// Call mesh service to add session (now returns quickly with polling-based detection)
	resp, err := s.engine.meshControlClient.AddSession(ctxWithTimeout, &meshv1.AddSessionRequest{
		Addr:           resolvedAddress,
		TimeoutSeconds: uint32(timeoutSeconds),
	})
	if err != nil {
		s.engine.logger.Errorf("Failed to add mesh session to %s: %v", resolvedAddress, err)
		return nil, err
	}

	if resp.Success {
		s.engine.logger.Infof("Successfully added mesh session to node %d at %s", resp.PeerNodeId, resp.RemoteAddr)

		// Publish mesh event for session added
		s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_SESSION_ADDED, resp.PeerNodeId, map[string]string{
			"target_address": targetAddress,
			"remote_addr":    resp.RemoteAddr,
			"operation":      "add_session",
			"description":    "Mesh session added successfully",
		})
	} else {
		s.engine.logger.Warnf("Failed to add mesh session to %s: %s", targetAddress, resp.Message)
	}

	return resp, nil
}

// DropMeshSession drops an existing session with another node
func (s *Server) DropMeshSession(ctx context.Context, peerNodeID uint64) (*meshv1.DropSessionResponse, error) {
	if s.engine.meshControlClient == nil {
		return nil, fmt.Errorf("mesh control client not available")
	}

	s.engine.logger.Infof("Dropping mesh session with node %d", peerNodeID)

	// Call mesh service to drop session
	resp, err := s.engine.meshControlClient.DropSession(ctx, &meshv1.DropSessionRequest{
		PeerNodeId: peerNodeID,
	})
	if err != nil {
		s.engine.logger.Errorf("Failed to drop mesh session with node %d: %v", peerNodeID, err)
		return nil, err
	}

	if resp.Success {
		s.engine.logger.Infof("Successfully dropped mesh session with node %d", peerNodeID)

		// Publish mesh event for session dropped
		s.publishMeshEvent(ctx, corev1.MeshEventType_MESH_EVENT_SESSION_REMOVED, peerNodeID, map[string]string{
			"operation":   "drop_session",
			"description": "Mesh session dropped successfully",
		})
	} else {
		s.engine.logger.Warnf("Failed to drop mesh session with node %d: %s", peerNodeID, resp.Message)
	}

	return resp, nil
}

// HandleStateEvent handles state events received from the mesh service
func (s *Server) HandleStateEvent(ctx context.Context, req *corev1.HandleStateEventRequest) (*corev1.HandleStateEventResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	if req.Event == nil {
		return &corev1.HandleStateEventResponse{
			Success: false,
			Message: "event is required",
		}, nil
	}

	s.engine.logger.Debugf("Handling state event %s from node %d", req.Event.EventType, req.SourceNode)

	eventManager := s.engine.GetMeshEventManager()
	if eventManager == nil {
		return &corev1.HandleStateEventResponse{
			Success: false,
			Message: "event manager not available",
		}, nil
	}

	// Convert protobuf event to internal event
	event := &mesh.MeshEvent{
		Type:           req.Event.EventType,
		OriginatorNode: req.Event.OriginatorNode,
		AffectedNode:   req.Event.AffectedNode,
		Sequence:       req.Event.SequenceNumber,
		Timestamp:      time.Unix(int64(req.Event.Timestamp), 0),
		Metadata:       req.Event.Metadata,
		Payload:        req.Event.Payload,
	}

	if err := eventManager.(*mesh.MeshEventManager).HandleReceivedEvent(ctx, event, req.SourceNode); err != nil {
		s.engine.logger.Errorf("Failed to handle received state event: %v", err)
		return &corev1.HandleStateEventResponse{
			Success: false,
			Message: fmt.Sprintf("failed to handle event: %v", err),
		}, nil
	}

	return &corev1.HandleStateEventResponse{
		Success: true,
		Message: "event processed successfully",
	}, nil
}

// HandleDatabaseSyncRequest handles database sync requests from the mesh service
func (s *Server) HandleDatabaseSyncRequest(ctx context.Context, req *corev1.HandleDatabaseSyncRequestMessage) (*corev1.HandleDatabaseSyncResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	s.engine.logger.Debugf("Handling database sync request for table %s from node %d", req.TableName, req.RequestingNode)

	syncManager := s.engine.GetDatabaseSyncManager()
	if syncManager == nil {
		return &corev1.HandleDatabaseSyncResponse{
			Success: false,
			Message: "database sync manager not available",
		}, nil
	}

	// Forward the request to the sync manager
	response, err := syncManager.(*mesh.DatabaseSyncManager).HandleSyncRequest(ctx, req)
	if err != nil {
		s.engine.logger.Errorf("Failed to handle database sync request: %v", err)
		return &corev1.HandleDatabaseSyncResponse{
			Success: false,
			Message: fmt.Sprintf("sync request failed: %v", err),
		}, nil
	}

	return response, nil
}
