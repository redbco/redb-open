package engine

import (
	"context"
	"encoding/json"
	"net/http"
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

// SeedMesh handles POST /{tenant_url}/api/v1/mesh/seed
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
		mh.engine.logger.Infof("Seed mesh request: %s with node: %s", req.MeshName, req.NodeName)
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
	if req.AllowJoin != nil {
		grpcReq.AllowJoin = req.AllowJoin
	}
	if req.JoinKey != "" {
		grpcReq.JoinKey = &req.JoinKey
	}

	grpcResp, err := mh.engine.meshClient.SeedMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to seed mesh")
		return
	}

	// Convert gRPC response to REST response
	mesh := Mesh{
		MeshID:          grpcResp.Mesh.MeshId,
		MeshName:        grpcResp.Mesh.MeshName,
		MeshDescription: grpcResp.Mesh.MeshDescription,
		PublicKey:       grpcResp.Mesh.PublicKey,
		AllowJoin:       grpcResp.Mesh.AllowJoin,
		NodeCount:       grpcResp.Mesh.NodeCount,
		Status:          convertStatus(grpcResp.Mesh.Status),
	}

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

// JoinMesh handles POST /{tenant_url}/api/v1/mesh/join
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
	if req.MeshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}
	if req.NodeName == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "node_name is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Join mesh request: %s with node: %s", req.MeshID, req.NodeName)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.JoinMeshRequest{
		MeshId:   req.MeshID,
		NodeName: req.NodeName,
	}

	// Set optional fields
	if req.NodeDescription != "" {
		grpcReq.NodeDescription = &req.NodeDescription
	}
	if req.JoinKey != "" {
		grpcReq.JoinKey = &req.JoinKey
	}

	grpcResp, err := mh.engine.meshClient.JoinMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to join mesh")
		return
	}

	// Convert gRPC response to REST response
	mesh := Mesh{
		MeshID:          grpcResp.Mesh.MeshId,
		MeshName:        grpcResp.Mesh.MeshName,
		MeshDescription: grpcResp.Mesh.MeshDescription,
		PublicKey:       grpcResp.Mesh.PublicKey,
		AllowJoin:       grpcResp.Mesh.AllowJoin,
		NodeCount:       grpcResp.Mesh.NodeCount,
		Status:          convertStatus(grpcResp.Mesh.Status),
	}

	response := JoinMeshResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mesh:    mesh,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully joined mesh: %s", req.MeshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// LeaveMesh handles POST /{tenant_url}/api/v1/mesh/{mesh_id}/leave
func (mh *MeshHandlers) LeaveMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	// Parse request body
	var req LeaveMeshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse leave mesh request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.NodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Leave mesh request: %s with node: %s", meshID, req.NodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.LeaveMeshRequest{
		MeshId: meshID,
		NodeId: req.NodeID,
	}

	grpcResp, err := mh.engine.meshClient.LeaveMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to leave mesh")
		return
	}

	response := LeaveMeshResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully left mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowMesh handles GET /{tenant_url}/api/v1/mesh/{mesh_id}
func (mh *MeshHandlers) ShowMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Show mesh request for mesh: %s", meshID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowMeshRequest{
		MeshId: meshID,
	}

	grpcResp, err := mh.engine.meshClient.ShowMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show mesh")
		return
	}

	// Convert gRPC response to REST response
	mesh := Mesh{
		MeshID:          grpcResp.Mesh.MeshId,
		MeshName:        grpcResp.Mesh.MeshName,
		MeshDescription: grpcResp.Mesh.MeshDescription,
		PublicKey:       grpcResp.Mesh.PublicKey,
		AllowJoin:       grpcResp.Mesh.AllowJoin,
		NodeCount:       grpcResp.Mesh.NodeCount,
		Status:          convertStatus(grpcResp.Mesh.Status),
	}

	response := ShowMeshResponse{
		Mesh: mesh,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ListNodes handles GET /{tenant_url}/api/v1/mesh/{mesh_id}/nodes
func (mh *MeshHandlers) ListNodes(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("List nodes request for mesh: %s", meshID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ListNodesRequest{
		MeshId: meshID,
	}

	grpcResp, err := mh.engine.meshClient.ListNodes(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to list nodes")
		return
	}

	// Convert gRPC response to REST response
	nodes := make([]Node, len(grpcResp.Nodes))
	for i, node := range grpcResp.Nodes {
		nodes[i] = Node{
			NodeID:          node.NodeId,
			NodeName:        node.NodeName,
			NodeDescription: node.NodeDescription,
			NodePlatform:    node.NodePlatform,
			NodeVersion:     node.NodeVersion,
			RegionID:        node.RegionId,
			RegionName:      node.RegionName,
			PublicKey:       node.PublicKey,
			PrivateKey:      node.PrivateKey,
			IPAddress:       node.IpAddress,
			Port:            node.Port,
			Status:          convertStatus(node.Status),
		}
	}

	response := ListNodesResponse{
		Nodes: nodes,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully listed %d nodes for mesh: %s", len(nodes), meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowNode handles GET /{tenant_url}/api/v1/mesh/{mesh_id}/nodes/{node_id}
func (mh *MeshHandlers) ShowNode(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]
	nodeID := vars["node_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	if nodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Show node request for mesh: %s, node: %s", meshID, nodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowNodeRequest{
		MeshId: meshID,
		NodeId: nodeID,
	}

	grpcResp, err := mh.engine.meshClient.ShowNode(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show node")
		return
	}

	// Convert gRPC response to REST response
	node := Node{
		NodeID:          grpcResp.Node.NodeId,
		NodeName:        grpcResp.Node.NodeName,
		NodeDescription: grpcResp.Node.NodeDescription,
		NodePlatform:    grpcResp.Node.NodePlatform,
		NodeVersion:     grpcResp.Node.NodeVersion,
		RegionID:        grpcResp.Node.RegionId,
		RegionName:      grpcResp.Node.RegionName,
		PublicKey:       grpcResp.Node.PublicKey,
		PrivateKey:      grpcResp.Node.PrivateKey,
		IPAddress:       grpcResp.Node.IpAddress,
		Port:            grpcResp.Node.Port,
		Status:          convertStatus(grpcResp.Node.Status),
	}

	response := ShowNodeResponse{
		Node: node,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed node: %s for mesh: %s", nodeID, meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ShowTopology handles GET /{tenant_url}/api/v1/mesh/{mesh_id}/topology
func (mh *MeshHandlers) ShowTopology(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Show topology request for mesh: %s", meshID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.ShowTopologyRequest{
		MeshId: meshID,
	}

	grpcResp, err := mh.engine.meshClient.ShowTopology(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to show topology")
		return
	}

	// Convert gRPC response to REST response
	topologies := make([]Topology, len(grpcResp.Topologies))
	for i, topo := range grpcResp.Topologies {
		topologies[i] = Topology{
			RouteID:            topo.RouteId,
			SourceNodeID:       topo.SourceNodeId,
			SourceNodeName:     topo.SourceNodeName,
			SourceRegionName:   topo.SourceRegionName,
			TargetNodeID:       topo.TargetNodeId,
			TargetNodeName:     topo.TargetNodeName,
			TargetRegionName:   topo.TargetRegionName,
			RouteBidirectional: topo.RouteBidirectional,
			RouteLatency:       topo.RouteLatency,
			RouteBandwidth:     topo.RouteBandwidth,
			RouteCost:          topo.RouteCost,
			Status:             convertStatus(topo.Status),
		}
	}

	response := ShowTopologyResponse{
		Topologies: topologies,
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully showed topology for mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ModifyMesh handles PUT /{tenant_url}/api/v1/mesh/{mesh_id}
func (mh *MeshHandlers) ModifyMesh(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	// Parse request body
	var req ModifyMeshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse modify mesh request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Modify mesh request for mesh: %s", meshID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifyMeshRequest{
		MeshId: meshID,
	}

	// Set optional fields
	if req.MeshName != "" {
		grpcReq.MeshName = &req.MeshName
	}
	if req.MeshDescription != "" {
		grpcReq.MeshDescription = &req.MeshDescription
	}
	if req.AllowJoin != nil {
		grpcReq.AllowJoin = req.AllowJoin
	}
	if req.JoinKey != "" {
		grpcReq.JoinKey = &req.JoinKey
	}

	grpcResp, err := mh.engine.meshClient.ModifyMesh(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to modify mesh")
		return
	}

	// Convert gRPC response to REST response
	mesh := Mesh{
		MeshID:          grpcResp.Mesh.MeshId,
		MeshName:        grpcResp.Mesh.MeshName,
		MeshDescription: grpcResp.Mesh.MeshDescription,
		PublicKey:       grpcResp.Mesh.PublicKey,
		AllowJoin:       grpcResp.Mesh.AllowJoin,
		NodeCount:       grpcResp.Mesh.NodeCount,
		Status:          convertStatus(grpcResp.Mesh.Status),
	}

	response := ModifyMeshResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Mesh:    mesh,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully modified mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// ModifyNode handles PUT /{tenant_url}/api/v1/mesh/{mesh_id}/nodes/{node_id}
func (mh *MeshHandlers) ModifyNode(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]
	nodeID := vars["node_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	if nodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "node_id is required", "")
		return
	}

	// Parse request body
	var req ModifyNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse modify node request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Modify node request for mesh: %s, node: %s", meshID, nodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifyNodeRequest{
		MeshId: meshID,
		NodeId: nodeID,
	}

	// Set optional fields
	if req.NodeName != "" {
		grpcReq.NodeName = &req.NodeName
	}
	if req.NodeDescription != "" {
		grpcReq.NodeDescription = &req.NodeDescription
	}
	if req.RegionID != "" {
		grpcReq.RegionId = &req.RegionID
	}

	grpcResp, err := mh.engine.meshClient.ModifyNode(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to modify node")
		return
	}

	// Convert gRPC response to REST response
	node := Node{
		NodeID:          grpcResp.Node.NodeId,
		NodeName:        grpcResp.Node.NodeName,
		NodeDescription: grpcResp.Node.NodeDescription,
		NodePlatform:    grpcResp.Node.NodePlatform,
		NodeVersion:     grpcResp.Node.NodeVersion,
		RegionID:        grpcResp.Node.RegionId,
		RegionName:      grpcResp.Node.RegionName,
		PublicKey:       grpcResp.Node.PublicKey,
		PrivateKey:      grpcResp.Node.PrivateKey,
		IPAddress:       grpcResp.Node.IpAddress,
		Port:            grpcResp.Node.Port,
		Status:          convertStatus(grpcResp.Node.Status),
	}

	response := ModifyNodeResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Node:    node,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully modified node: %s for mesh: %s", nodeID, meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// EvictNode handles DELETE /{tenant_url}/api/v1/mesh/{mesh_id}/nodes/{node_id}
func (mh *MeshHandlers) EvictNode(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]
	nodeID := vars["node_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	if nodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Evict node request for mesh: %s, node: %s", meshID, nodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.EvictNodeRequest{
		MeshId: meshID,
		NodeId: nodeID,
	}

	grpcResp, err := mh.engine.meshClient.EvictNode(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to evict node")
		return
	}

	response := EvictNodeResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully evicted node: %s from mesh: %s", nodeID, meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// AddMeshRoute handles POST /{tenant_url}/api/v1/mesh/{mesh_id}/routes
func (mh *MeshHandlers) AddMeshRoute(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	// Parse request body
	var req AddMeshRouteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse add mesh route request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Validate required fields
	if req.SourceNodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "source_node_id is required", "")
		return
	}
	if req.TargetNodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Add mesh route request for mesh: %s from %s to %s", meshID, req.SourceNodeID, req.TargetNodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.AddMeshRouteRequest{
		MeshId:             meshID,
		SourceNodeId:       req.SourceNodeID,
		TargetNodeId:       req.TargetNodeID,
		RouteBidirectional: req.RouteBidirectional,
	}

	// Set optional fields
	if req.RouteLatency != nil {
		grpcReq.RouteLatency = req.RouteLatency
	}
	if req.RouteBandwidth != nil {
		grpcReq.RouteBandwidth = req.RouteBandwidth
	}
	if req.RouteCost != nil {
		grpcReq.RouteCost = req.RouteCost
	}

	grpcResp, err := mh.engine.meshClient.AddMeshRoute(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to add mesh route")
		return
	}

	// Convert gRPC response to REST response
	route := Topology{
		RouteID:            grpcResp.Route.RouteId,
		SourceNodeID:       grpcResp.Route.SourceNodeId,
		SourceNodeName:     grpcResp.Route.SourceNodeName,
		SourceRegionName:   grpcResp.Route.SourceRegionName,
		TargetNodeID:       grpcResp.Route.TargetNodeId,
		TargetNodeName:     grpcResp.Route.TargetNodeName,
		TargetRegionName:   grpcResp.Route.TargetRegionName,
		RouteBidirectional: grpcResp.Route.RouteBidirectional,
		RouteLatency:       grpcResp.Route.RouteLatency,
		RouteBandwidth:     grpcResp.Route.RouteBandwidth,
		RouteCost:          grpcResp.Route.RouteCost,
		Status:             convertStatus(grpcResp.Route.Status),
	}

	response := AddMeshRouteResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Route:   route,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully added mesh route for mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusCreated, response)
}

// ModifyMeshRoute handles PUT /{tenant_url}/api/v1/mesh/{mesh_id}/routes/{source_node_id}/{target_node_id}
func (mh *MeshHandlers) ModifyMeshRoute(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]
	sourceNodeID := vars["source_node_id"]
	targetNodeID := vars["target_node_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	if sourceNodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "source_node_id is required", "")
		return
	}

	if targetNodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_node_id is required", "")
		return
	}

	// Parse request body
	var req ModifyMeshRouteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if mh.engine.logger != nil {
			mh.engine.logger.Errorf("Failed to parse modify mesh route request body: %v", err)
		}
		mh.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Modify mesh route request for mesh: %s from %s to %s", meshID, sourceNodeID, targetNodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Prepare gRPC request
	grpcReq := &corev1.ModifyMeshRouteRequest{
		MeshId:       meshID,
		SourceNodeId: sourceNodeID,
		TargetNodeId: targetNodeID,
	}

	// Set optional fields
	if req.RouteBidirectional != nil {
		grpcReq.RouteBidirectional = req.RouteBidirectional
	}
	if req.RouteLatency != nil {
		grpcReq.RouteLatency = req.RouteLatency
	}
	if req.RouteBandwidth != nil {
		grpcReq.RouteBandwidth = req.RouteBandwidth
	}
	if req.RouteCost != nil {
		grpcReq.RouteCost = req.RouteCost
	}

	grpcResp, err := mh.engine.meshClient.ModifyMeshRoute(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to modify mesh route")
		return
	}

	// Convert gRPC response to REST response
	route := Topology{
		RouteID:            grpcResp.Route.RouteId,
		SourceNodeID:       grpcResp.Route.SourceNodeId,
		SourceNodeName:     grpcResp.Route.SourceNodeName,
		SourceRegionName:   grpcResp.Route.SourceRegionName,
		TargetNodeID:       grpcResp.Route.TargetNodeId,
		TargetNodeName:     grpcResp.Route.TargetNodeName,
		TargetRegionName:   grpcResp.Route.TargetRegionName,
		RouteBidirectional: grpcResp.Route.RouteBidirectional,
		RouteLatency:       grpcResp.Route.RouteLatency,
		RouteBandwidth:     grpcResp.Route.RouteBandwidth,
		RouteCost:          grpcResp.Route.RouteCost,
		Status:             convertStatus(grpcResp.Route.Status),
	}

	response := ModifyMeshRouteResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Route:   route,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully modified mesh route for mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// DeleteMeshRoute handles DELETE /{tenant_url}/api/v1/mesh/{mesh_id}/routes/{source_node_id}/{target_node_id}
func (mh *MeshHandlers) DeleteMeshRoute(w http.ResponseWriter, r *http.Request) {
	mh.engine.TrackOperation()
	defer mh.engine.UntrackOperation()

	// Extract path parameters
	vars := mux.Vars(r)
	meshID := vars["mesh_id"]
	sourceNodeID := vars["source_node_id"]
	targetNodeID := vars["target_node_id"]

	if meshID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "mesh_id is required", "")
		return
	}

	if sourceNodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "source_node_id is required", "")
		return
	}

	if targetNodeID == "" {
		mh.writeErrorResponse(w, http.StatusBadRequest, "target_node_id is required", "")
		return
	}

	// Log request
	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Delete mesh route request for mesh: %s from %s to %s", meshID, sourceNodeID, targetNodeID)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Call core service gRPC
	grpcReq := &corev1.DeleteMeshRouteRequest{
		MeshId:       meshID,
		SourceNodeId: sourceNodeID,
		TargetNodeId: targetNodeID,
	}

	grpcResp, err := mh.engine.meshClient.DeleteMeshRoute(ctx, grpcReq)
	if err != nil {
		mh.handleGRPCError(w, err, "Failed to delete mesh route")
		return
	}

	response := DeleteMeshRouteResponse{
		Message: grpcResp.Message,
		Success: grpcResp.Success,
		Status:  convertStatus(grpcResp.Status),
	}

	if mh.engine.logger != nil {
		mh.engine.logger.Infof("Successfully deleted mesh route for mesh: %s", meshID)
	}

	mh.writeJSONResponse(w, http.StatusOK, response)
}

// Helper methods

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
