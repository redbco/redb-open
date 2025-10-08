package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MeshHandlers contains the mesh endpoint handlers
type MeshHandlers struct {
	engine *Engine
}

// NewMeshHandlers creates a new instance of MeshHandlers
func NewMeshHandlers(engine *Engine) *MeshHandlers {
	return &MeshHandlers{
		engine: engine,
	}
}

// === Core Mesh Operations ===

// SeedMesh handles POST /api/v1/mesh/seed
func (mh *MeshHandlers) SeedMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Parse request body
	var req SeedMeshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse seed mesh request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.MeshName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_name is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Seed mesh request: %s", req.MeshName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.SeedMeshRequest{
		MeshName: req.MeshName,
	}

	// Set optional fields
	if req.MeshDescription != "" {
		grpcReq.MeshDescription = &req.MeshDescription
	}

	grpcResp, err := mh.engine.meshClient.SeedMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to seed mesh")
		return
	}

	// Convert gRPC response to REST response
	mesh := mh.meshFromProto(grpcResp.Mesh)

	response := SeedMeshResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mesh:    mesh,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully seeded mesh: %s", req.MeshName)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// JoinMesh handles POST /api/v1/mesh/join
func (mh *MeshHandlers) JoinMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Parse request body
	var req JoinMeshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse join mesh request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.TargetAddress == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_address is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Join mesh request to: %s", req.TargetAddress)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second) // Longer timeout for mesh operations
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.JoinMeshRequest{
		TargetAddress: req.TargetAddress,
	}

	// Set optional fields
	if req.Strategy != nil {
		switch *req.Strategy {
		case JoinStrategyInherit:
			grpcReq.Strategy = &[]corev1.JoinStrategy{corev1.JoinStrategy_JOIN_STRATEGY_INHERIT}[0]
		case JoinStrategyMerge:
			grpcReq.Strategy = &[]corev1.JoinStrategy{corev1.JoinStrategy_JOIN_STRATEGY_MERGE}[0]
		case JoinStrategyOverwrite:
			grpcReq.Strategy = &[]corev1.JoinStrategy{corev1.JoinStrategy_JOIN_STRATEGY_OVERWRITE}[0]
		}
	}

	if req.TimeoutSeconds != nil {
		grpcReq.TimeoutSeconds = req.TimeoutSeconds
	}

	grpcResp, err := mh.engine.meshClient.JoinMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to join mesh")
		return
	}

	// Convert gRPC response to REST response
	// Note: Mesh may be nil during background sync
	var mesh Mesh
	if grpcResp.Mesh != nil {
		mesh = mh.meshFromProto(grpcResp.Mesh)
	}

	response := JoinMeshResponse{
		Message:    grpcResp.Message,
		Success:    grpcResp.Success,
		Mesh:       mesh,
		PeerNodeID: grpcResp.PeerNodeId,
		RemoteAddr: grpcResp.RemoteAddr,
		Status:     convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully joined mesh via: %s", req.TargetAddress)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ExtendMesh handles POST /api/v1/mesh/extend
func (mh *MeshHandlers) ExtendMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Parse request body
	var req ExtendMeshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse extend mesh request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.TargetAddress == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_address is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Extend mesh request to: %s", req.TargetAddress)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ExtendMeshRequest{
		TargetAddress: req.TargetAddress,
	}

	// Set optional fields
	if req.Strategy != nil {
		switch *req.Strategy {
		case JoinStrategyInherit:
			grpcReq.Strategy = &[]corev1.JoinStrategy{corev1.JoinStrategy_JOIN_STRATEGY_INHERIT}[0]
		case JoinStrategyMerge:
			grpcReq.Strategy = &[]corev1.JoinStrategy{corev1.JoinStrategy_JOIN_STRATEGY_MERGE}[0]
		case JoinStrategyOverwrite:
			grpcReq.Strategy = &[]corev1.JoinStrategy{corev1.JoinStrategy_JOIN_STRATEGY_OVERWRITE}[0]
		}
	}

	if req.TimeoutSeconds != nil {
		grpcReq.TimeoutSeconds = req.TimeoutSeconds
	}

	grpcResp, err := mh.engine.meshClient.ExtendMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to extend mesh")
		return
	}

	response := ExtendMeshResponse{
		Message:    grpcResp.Message,
		Success:    grpcResp.Success,
		PeerNodeID: grpcResp.PeerNodeId,
		RemoteAddr: grpcResp.RemoteAddr,
		Status:     convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully extended mesh to: %s", req.TargetAddress)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// LeaveMesh handles POST /api/v1/mesh/leave
func (mh *MeshHandlers) LeaveMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Parse request body
	var req LeaveMeshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse leave mesh request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Leave mesh request (force: %t)", req.Force)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.LeaveMeshRequest{
		Force: req.Force,
	}

	grpcResp, err := mh.engine.meshClient.LeaveMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to leave mesh")
		return
	}

	response := LeaveMeshResponse{
		Message:            grpcResp.Message,
		Success:            grpcResp.Success,
		ConnectionsDropped: grpcResp.ConnectionsDropped,
		Status:             convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully left mesh")
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// EvictNode handles POST /api/v1/mesh/evict
func (mh *MeshHandlers) EvictNode(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Parse request body
	var req EvictNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse evict node request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.TargetNodeID == 0 {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Evict node request: %d", req.TargetNodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.EvictNodeRequest{
		TargetNodeId: req.TargetNodeID,
		CleanTarget:  req.CleanTarget,
	}

	grpcResp, err := mh.engine.meshClient.EvictNode(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to evict node")
		return
	}

	response := EvictNodeResponse{
		Message:       grpcResp.Message,
		Success:       grpcResp.Success,
		TargetCleaned: grpcResp.TargetCleaned,
		Status:        convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully evicted node: %d", req.TargetNodeID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// === Connection Management ===

// AddConnection handles POST /api/v1/mesh/connections
func (mh *MeshHandlers) AddConnection(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Parse request body
	var req AddConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse add connection request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.TargetNodeID == 0 {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add connection request to node: %d", req.TargetNodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.AddConnectionRequest{
		TargetNodeId: req.TargetNodeID,
	}

	if req.TimeoutSeconds != nil {
		grpcReq.TimeoutSeconds = req.TimeoutSeconds
	}

	grpcResp, err := mh.engine.meshClient.AddConnection(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to add connection")
		return
	}

	// Convert gRPC response to REST response
	connection := mh.connectionFromProto(grpcResp.Connection)

	response := AddConnectionResponse{
		Message:    grpcResp.Message,
		Success:    grpcResp.Success,
		Connection: connection,
		Status:     convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully added connection to node: %d", req.TargetNodeID)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// DropConnection handles DELETE /api/v1/mesh/connections/{peer_node_id}
func (mh *MeshHandlers) DropConnection(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	peerNodeIDStr := vars["peer_node_id"]

	if peerNodeIDStr == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "peer_node_id is required", "")
		return
	}

	peerNodeID, err := strconv.ParseUint(peerNodeIDStr, 10, 64)
	if err != nil {
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid peer_node_id format", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Drop connection request for node: %d", peerNodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.DropConnectionRequest{
		PeerNodeId: peerNodeID,
	}

	grpcResp, err := mh.engine.meshClient.DropConnection(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to drop connection")
		return
	}

	response := DropConnectionResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully dropped connection to node: %d", peerNodeID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ListConnections handles GET /api/v1/mesh/connections
func (mh *MeshHandlers) ListConnections(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("List connections request")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	grpcResp, err := mh.engine.meshClient.ListConnections(ctx, &corev1.ListConnectionsRequest{})
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to list connections")
		return
	}

	// Convert gRPC response to REST response
	connections := make([]Connection, len(grpcResp.Connections))
	for i, conn := range grpcResp.Connections {
		connections[i] = mh.connectionFromProto(conn)
	}

	response := ListConnectionsResponse{
		Connections: connections,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully listed %d connections", len(connections))
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// === Information and Status ===

// ShowMesh handles GET /api/v1/mesh
func (mh *MeshHandlers) ShowMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Show mesh request")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	grpcResp, err := mh.engine.meshClient.ShowMesh(ctx, &corev1.ShowMeshRequest{})
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show mesh")
		return
	}

	// Convert gRPC response to REST response
	mesh := mh.meshFromProto(grpcResp.Mesh)

	response := ShowMeshResponse{
		Mesh: mesh,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed mesh: %s", mesh.MeshName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ListNodes handles GET /api/v1/mesh/nodes
func (mh *MeshHandlers) ListNodes(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("List nodes request")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	grpcResp, err := mh.engine.meshClient.ListNodes(ctx, &corev1.ListNodesRequest{})
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to list nodes")
		return
	}

	// Convert gRPC response to REST response
	nodes := make([]Node, len(grpcResp.Nodes))
	for i, node := range grpcResp.Nodes {
		nodes[i] = mh.nodeFromProto(node)
	}

	response := ListNodesResponse{
		Nodes: nodes,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully listed %d nodes", len(nodes))
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowNode handles GET /api/v1/mesh/nodes/{node_id} or GET /api/v1/mesh/nodes (for current node)
func (mh *MeshHandlers) ShowNode(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters (optional)
	vars := mux.Vars(r)
	nodeIDStr := vars["node_id"]

	var nodeID *uint64
	if nodeIDStr != "" {
		id, err := strconv.ParseUint(nodeIDStr, 10, 64)
		if err != nil {
			mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid node_id format", "")
			return
		}
		nodeID = &id
	}

	// Log request
	if mh.engine.logger != nil {
		if nodeID != nil {
			mh.engine.logger.Infof("Show node request for node: %d", *nodeID)
		} else {
			mh.engine.logger.Infof("Show current node request")
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ShowNodeRequest{}
	if nodeID != nil {
		grpcReq.NodeId = nodeID
	}

	grpcResp, err := mh.engine.meshClient.ShowNode(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show node")
		return
	}

	// Convert gRPC response to REST response
	node := mh.nodeFromProto(grpcResp.Node)

	response := ShowNodeResponse{
		Node: node,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed node: %s", node.NodeName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// GetNodeStatus handles GET /api/v1/node/status
func (mh *MeshHandlers) GetNodeStatus(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Get node status request")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	grpcResp, err := mh.engine.meshClient.GetNodeStatus(ctx, &corev1.GetNodeStatusRequest{})
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to get node status")
		return
	}

	// Convert gRPC response to REST response
	node := mh.nodeFromProto(grpcResp.Node)

	connections := make([]Connection, len(grpcResp.Connections))
	for i, conn := range grpcResp.Connections {
		connections[i] = mh.connectionFromProto(conn)
	}

	var mesh *Mesh
	if grpcResp.Mesh != nil {
		meshData := mh.meshFromProto(grpcResp.Mesh)
		mesh = &meshData
	}

	response := GetNodeStatusResponse{
		Node:        node,
		Connections: connections,
		Mesh:        mesh,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully got node status for: %s", node.NodeName)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// === Helper Methods ===

// meshFromProto converts a protobuf mesh to REST model
func (mh *MeshHandlers) meshFromProto(protoMesh *corev1.Mesh) Mesh {
	return Mesh{
		MeshID:          protoMesh.MeshId,
		MeshName:        protoMesh.MeshName,
		MeshDescription: protoMesh.MeshDescription,
		AllowJoin:       protoMesh.AllowJoin,
		NodeCount:       protoMesh.NodeCount,
		ConnectionCount: protoMesh.ConnectionCount,
		Status:          convertStatus(protoMesh.Status),
		CreatedAt:       protoMesh.CreatedAt,
		UpdatedAt:       protoMesh.UpdatedAt,
	}
}

// nodeFromProto converts a protobuf node to REST model
func (mh *MeshHandlers) nodeFromProto(protoNode *corev1.Node) Node {
	// Convert string node ID back to uint64
	nodeID, _ := strconv.ParseUint(protoNode.NodeId, 10, 64)

	// Convert NodeStatus enum to string
	var nodeStatus NodeStatus
	switch protoNode.NodeStatus {
	case corev1.NodeStatus_NODE_STATUS_CLEAN:
		nodeStatus = NodeStatusClean
	case corev1.NodeStatus_NODE_STATUS_JOINING:
		nodeStatus = NodeStatusJoining
	case corev1.NodeStatus_NODE_STATUS_ACTIVE:
		nodeStatus = NodeStatusActive
	case corev1.NodeStatus_NODE_STATUS_LEAVING:
		nodeStatus = NodeStatusLeaving
	case corev1.NodeStatus_NODE_STATUS_OFFLINE:
		nodeStatus = NodeStatusOffline
	default:
		nodeStatus = NodeStatusClean
	}

	node := Node{
		NodeID:          nodeID,
		NodeName:        protoNode.NodeName,
		NodeDescription: protoNode.NodeDescription,
		NodePlatform:    protoNode.NodePlatform,
		NodeVersion:     protoNode.NodeVersion,
		RegionID:        protoNode.RegionId,
		RegionName:      protoNode.RegionName,
		IPAddress:       protoNode.IpAddress,
		Port:            protoNode.Port,
		NodeStatus:      nodeStatus,
		CreatedAt:       protoNode.CreatedAt,
		UpdatedAt:       protoNode.UpdatedAt,
	}

	// Set mesh ID if available
	if protoNode.MeshId != nil {
		node.MeshID = *protoNode.MeshId
	}

	return node
}

// connectionFromProto converts a protobuf connection to REST model
func (mh *MeshHandlers) connectionFromProto(protoConn *corev1.Connection) Connection {
	// Convert ConnectionStatus enum to string
	var connStatus ConnectionStatus
	switch protoConn.Status {
	case corev1.ConnectionStatus_CONNECTION_STATUS_CONNECTING:
		connStatus = ConnectionStatusConnecting
	case corev1.ConnectionStatus_CONNECTION_STATUS_CONNECTED:
		connStatus = ConnectionStatusConnected
	case corev1.ConnectionStatus_CONNECTION_STATUS_DISCONNECTING:
		connStatus = ConnectionStatusDisconnecting
	case corev1.ConnectionStatus_CONNECTION_STATUS_FAILED:
		connStatus = ConnectionStatusFailed
	default:
		connStatus = ConnectionStatusFailed
	}

	return Connection{
		PeerNodeID:      protoConn.PeerNodeId,
		PeerNodeName:    protoConn.PeerNodeName,
		RemoteAddr:      protoConn.RemoteAddr,
		Status:          connStatus,
		RTTMicroseconds: protoConn.RttMicroseconds,
		BytesSent:       protoConn.BytesSent,
		BytesReceived:   protoConn.BytesReceived,
		IsTLS:           protoConn.IsTls,
		ConnectedAt:     protoConn.ConnectedAt,
	}
}

// handleGRPCError handles gRPC errors and converts them to HTTP responses
func (mh *MeshHandlers) handleGRPCError(w http.ResponseWriter, err error, defaultMessage string) {
	if mh.engine.logger != nil {
		mh.engine.logger.Errorf("gRPC error: %v", err)
	}

	st, ok := status.FromError(err)
	if !ok {
		mh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		mh.writeErrorResponse(w, http.StatusNotFound, "Resource not found", st.Message())
	case codes.AlreadyExists:
		mh.writeErrorResponse(w, http.StatusConflict, "Resource already exists", st.Message())
	case codes.InvalidArgument:
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request", st.Message())
	case codes.PermissionDenied:
		mh.writeErrorResponse(w, http.StatusForbidden, "Permission denied", st.Message())
	case codes.Unauthenticated:
		mh.writeErrorResponse(w, http.StatusUnauthorized, "Authentication required", st.Message())
	case codes.Unavailable:
		mh.writeErrorResponse(w, http.StatusServiceUnavailable, "Service unavailable", st.Message())
	case codes.DeadlineExceeded:
		mh.writeErrorResponse(w, http.StatusRequestTimeout, "Request timeout", st.Message())
	case codes.FailedPrecondition:
		mh.writeErrorResponse(w, http.StatusPreconditionFailed, "Precondition failed", st.Message())
	default:
		mh.writeErrorResponse(w, http.StatusInternalServerError, defaultMessage, st.Message())
	}
}

// writeJSONResponse writes a JSON response
func (mh *MeshHandlers) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to encode JSON response: %v", err)
		}
	}
}

// writeErrorResponse writes an error response
func (mh *MeshHandlers) writeErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	response := ErrorResponse{
		Error:   message,
		Message: details,
		Status:  StatusError,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to encode error response: %v", err)
		}
	}
}
