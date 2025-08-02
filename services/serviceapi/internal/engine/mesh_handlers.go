package engine

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
)

type MeshHandlers struct {
	engine *Engine
}

func NewMeshHandlers(engine *Engine) *MeshHandlers {
	return &MeshHandlers{
		engine: engine,
	}
}

// SeedMesh handles POST /api/v1/mesh/seed
func (h *MeshHandlers) SeedMesh(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	var req struct {
		MeshName        string `json:"mesh_name"`
		MeshDescription string `json:"mesh_description,omitempty"`
		NodeName        string `json:"node_name"`
		NodeDescription string `json:"node_description,omitempty"`
		AllowJoin       bool   `json:"allow_join"`
		JoinKey         string `json:"join_key,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.MeshName == "" {
		http.Error(w, "mesh_name is required", http.StatusBadRequest)
		return
	}

	// Call core service
	coreReq := &corev1.SeedMeshRequest{
		MeshName:        req.MeshName,
		MeshDescription: &req.MeshDescription,
		AllowJoin:       &req.AllowJoin,
		JoinKey:         &req.JoinKey,
	}

	resp, err := h.engine.meshClient.SeedMesh(r.Context(), coreReq)
	if err != nil {
		if h.engine.logger != nil {
			h.engine.logger.Errorf("Failed to seed mesh: %v", err)
		}
		http.Error(w, "Failed to seed mesh", http.StatusInternalServerError)
		return
	}

	// Convert response
	response := map[string]interface{}{
		"success": resp.Success,
		"message": resp.Message,
		"status":  resp.Status.String(),
	}

	if resp.Mesh != nil {
		response["mesh"] = map[string]interface{}{
			"mesh_id":          resp.Mesh.MeshId,
			"mesh_name":        resp.Mesh.MeshName,
			"mesh_description": resp.Mesh.MeshDescription,
			"allow_join":       resp.Mesh.AllowJoin,
			"node_count":       resp.Mesh.NodeCount,
			"status":           resp.Mesh.Status.String(),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// JoinMesh handles POST /api/v1/mesh/join
func (h *MeshHandlers) JoinMesh(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	var req struct {
		MeshID          string `json:"mesh_id"`
		NodeName        string `json:"node_name"`
		NodeDescription string `json:"node_description,omitempty"`
		JoinKey         string `json:"join_key,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.MeshID == "" {
		http.Error(w, "mesh_id is required", http.StatusBadRequest)
		return
	}
	if req.NodeName == "" {
		http.Error(w, "node_name is required", http.StatusBadRequest)
		return
	}

	// Call core service
	coreReq := &corev1.JoinMeshRequest{
		MeshId:          req.MeshID,
		NodeName:        req.NodeName,
		NodeDescription: &req.NodeDescription,
		JoinKey:         &req.JoinKey,
	}

	resp, err := h.engine.meshClient.JoinMesh(r.Context(), coreReq)
	if err != nil {
		if h.engine.logger != nil {
			h.engine.logger.Errorf("Failed to join mesh: %v", err)
		}
		http.Error(w, "Failed to join mesh", http.StatusInternalServerError)
		return
	}

	// Convert response
	response := map[string]interface{}{
		"success": resp.Success,
		"message": resp.Message,
		"status":  resp.Status.String(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// ShowMesh handles GET /api/v1/mesh/{mesh_id}
func (h *MeshHandlers) ShowMesh(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		http.Error(w, "mesh_id is required", http.StatusBadRequest)
		return
	}

	// Call core service
	coreReq := &corev1.ShowMeshRequest{
		MeshId: meshID,
	}

	resp, err := h.engine.meshClient.ShowMesh(r.Context(), coreReq)
	if err != nil {
		if h.engine.logger != nil {
			h.engine.logger.Errorf("Failed to show mesh: %v", err)
		}
		http.Error(w, "Failed to show mesh", http.StatusInternalServerError)
		return
	}

	// Convert response
	response := map[string]interface{}{
		"mesh": map[string]interface{}{
			"mesh_id":          resp.Mesh.MeshId,
			"mesh_name":        resp.Mesh.MeshName,
			"mesh_description": resp.Mesh.MeshDescription,
			"allow_join":       resp.Mesh.AllowJoin,
			"node_count":       resp.Mesh.NodeCount,
			"status":           resp.Mesh.Status.String(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ListNodes handles GET /api/v1/mesh/{mesh_id}/nodes
func (h *MeshHandlers) ListNodes(w http.ResponseWriter, r *http.Request) {
	h.engine.TrackOperation()
	defer h.engine.UntrackOperation()

	vars := mux.Vars(r)
	meshID := vars["mesh_id"]

	if meshID == "" {
		http.Error(w, "mesh_id is required", http.StatusBadRequest)
		return
	}

	// Call core service
	coreReq := &corev1.ListNodesRequest{
		MeshId: meshID,
	}

	resp, err := h.engine.meshClient.ListNodes(r.Context(), coreReq)
	if err != nil {
		if h.engine.logger != nil {
			h.engine.logger.Errorf("Failed to list nodes: %v", err)
		}
		http.Error(w, "Failed to list nodes", http.StatusInternalServerError)
		return
	}

	// Convert response
	nodes := make([]map[string]interface{}, len(resp.Nodes))
	for i, node := range resp.Nodes {
		nodes[i] = map[string]interface{}{
			"node_id":          node.NodeId,
			"node_name":        node.NodeName,
			"node_description": node.NodeDescription,
			"node_platform":    node.NodePlatform,
			"node_version":     node.NodeVersion,
			"region_id":        node.RegionId,
			"region_name":      node.RegionName,
			"public_key":       node.PublicKey,
			"private_key":      node.PrivateKey,
			"ip_address":       node.IpAddress,
			"port":             node.Port,
			"status":           node.Status.String(),
		}
	}

	response := map[string]interface{}{
		"nodes": nodes,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
