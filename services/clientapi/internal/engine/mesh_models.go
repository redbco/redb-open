package engine

// Mesh represents a mesh
type Mesh struct {
	MeshID          string `json:"mesh_id"`
	MeshName        string `json:"mesh_name"`
	MeshDescription string `json:"mesh_description,omitempty"`
	PublicKey       string `json:"public_key"`
	AllowJoin       bool   `json:"allow_join"`
	NodeCount       int32  `json:"node_count"`
	Status          Status `json:"status"`
}

// Node represents a node in a mesh
type Node struct {
	NodeID          string `json:"node_id"`
	NodeName        string `json:"node_name"`
	NodeDescription string `json:"node_description,omitempty"`
	NodePlatform    string `json:"node_platform"`
	NodeVersion     string `json:"node_version"`
	RegionID        string `json:"region_id"`
	RegionName      string `json:"region_name"`
	PublicKey       string `json:"public_key"`
	PrivateKey      string `json:"private_key"`
	IPAddress       string `json:"ip_address"`
	Port            int32  `json:"port"`
	Status          Status `json:"status"`
}

// Topology represents mesh topology/routing
type Topology struct {
	RouteID            string  `json:"route_id"`
	SourceNodeID       string  `json:"source_node_id"`
	SourceNodeName     string  `json:"source_node_name"`
	SourceRegionName   string  `json:"source_region_name"`
	TargetNodeID       string  `json:"target_node_id"`
	TargetNodeName     string  `json:"target_node_name"`
	TargetRegionName   string  `json:"target_region_name"`
	RouteBidirectional bool    `json:"route_bidirectional"`
	RouteLatency       float64 `json:"route_latency"`
	RouteBandwidth     float64 `json:"route_bandwidth"`
	RouteCost          int32   `json:"route_cost"`
	Status             Status  `json:"status"`
}

// Mesh API endpoints

type SeedMeshRequest struct {
	MeshName        string `json:"mesh_name" validate:"required"`
	MeshDescription string `json:"mesh_description,omitempty"`
	NodeName        string `json:"node_name" validate:"required"`
	NodeDescription string `json:"node_description,omitempty"`
	AllowJoin       *bool  `json:"allow_join,omitempty"`
	JoinKey         string `json:"join_key,omitempty"`
}

type SeedMeshResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Mesh    Mesh   `json:"mesh"`
	Status  Status `json:"status"`
}

type JoinMeshRequest struct {
	MeshID          string `json:"mesh_id" validate:"required"`
	NodeName        string `json:"node_name" validate:"required"`
	NodeDescription string `json:"node_description,omitempty"`
	JoinKey         string `json:"join_key,omitempty"`
}

type JoinMeshResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Mesh    Mesh   `json:"mesh"`
	Status  Status `json:"status"`
}

type LeaveMeshRequest struct {
	MeshID string `json:"mesh_id" validate:"required"`
	NodeID string `json:"node_id" validate:"required"`
}

type LeaveMeshResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type ShowMeshResponse struct {
	Mesh Mesh `json:"mesh"`
}

type ListNodesResponse struct {
	Nodes []Node `json:"nodes"`
}

type ShowNodeResponse struct {
	Node Node `json:"node"`
}

type ShowTopologyResponse struct {
	Topologies []Topology `json:"topologies"`
}

type ModifyMeshRequest struct {
	MeshName        string `json:"mesh_name,omitempty"`
	MeshDescription string `json:"mesh_description,omitempty"`
	AllowJoin       *bool  `json:"allow_join,omitempty"`
	JoinKey         string `json:"join_key,omitempty"`
}

type ModifyMeshResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Mesh    Mesh   `json:"mesh"`
	Status  Status `json:"status"`
}

type ModifyNodeRequest struct {
	NodeName        string `json:"node_name,omitempty"`
	NodeDescription string `json:"node_description,omitempty"`
	RegionID        string `json:"region_id,omitempty"`
}

type ModifyNodeResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Node    Node   `json:"node"`
	Status  Status `json:"status"`
}

type EvictNodeRequest struct {
	NodeID string `json:"node_id" validate:"required"`
}

type EvictNodeResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type AddMeshRouteRequest struct {
	SourceNodeID       string   `json:"source_node_id" validate:"required"`
	TargetNodeID       string   `json:"target_node_id" validate:"required"`
	RouteBidirectional bool     `json:"route_bidirectional"`
	RouteLatency       *float64 `json:"route_latency,omitempty"`
	RouteBandwidth     *float64 `json:"route_bandwidth,omitempty"`
	RouteCost          *int32   `json:"route_cost,omitempty"`
}

type AddMeshRouteResponse struct {
	Message string   `json:"message"`
	Success bool     `json:"success"`
	Route   Topology `json:"route"`
	Status  Status   `json:"status"`
}

type ModifyMeshRouteRequest struct {
	RouteBidirectional *bool    `json:"route_bidirectional,omitempty"`
	RouteLatency       *float64 `json:"route_latency,omitempty"`
	RouteBandwidth     *float64 `json:"route_bandwidth,omitempty"`
	RouteCost          *int32   `json:"route_cost,omitempty"`
}

type ModifyMeshRouteResponse struct {
	Message string   `json:"message"`
	Success bool     `json:"success"`
	Route   Topology `json:"route"`
	Status  Status   `json:"status"`
}

type DeleteMeshRouteResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
