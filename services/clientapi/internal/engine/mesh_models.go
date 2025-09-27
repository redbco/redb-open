package engine

// JoinStrategy represents the strategy for joining/extending mesh operations
type JoinStrategy string

const (
	JoinStrategyInherit   JoinStrategy = "inherit"   // Default: inherit configuration from mesh
	JoinStrategyMerge     JoinStrategy = "merge"     // Merge configurations
	JoinStrategyOverwrite JoinStrategy = "overwrite" // Overwrite mesh configuration
)

// NodeStatus represents the status of a node
type NodeStatus string

const (
	NodeStatusClean   NodeStatus = "clean"   // Node has identity but no mesh membership
	NodeStatusJoining NodeStatus = "joining" // Node is joining a mesh
	NodeStatusActive  NodeStatus = "active"  // Node is active in mesh
	NodeStatusLeaving NodeStatus = "leaving" // Node is leaving mesh
	NodeStatusOffline NodeStatus = "offline" // Node is offline but still in mesh
)

// ConnectionStatus represents the status of a connection
type ConnectionStatus string

const (
	ConnectionStatusConnecting    ConnectionStatus = "connecting"
	ConnectionStatusConnected     ConnectionStatus = "connected"
	ConnectionStatusDisconnecting ConnectionStatus = "disconnecting"
	ConnectionStatusFailed        ConnectionStatus = "failed"
)

// Mesh represents a mesh network
type Mesh struct {
	MeshID          string `json:"mesh_id"`
	MeshName        string `json:"mesh_name"`
	MeshDescription string `json:"mesh_description,omitempty"`
	AllowJoin       bool   `json:"allow_join"`
	NodeCount       int32  `json:"node_count"`
	ConnectionCount int32  `json:"connection_count"`
	Status          Status `json:"status"`
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
}

// Node represents a node in the system
type Node struct {
	NodeID          uint64     `json:"node_id"`
	NodeName        string     `json:"node_name"`
	NodeDescription string     `json:"node_description,omitempty"`
	NodePlatform    string     `json:"node_platform"`
	NodeVersion     string     `json:"node_version"`
	RegionID        string     `json:"region_id,omitempty"`
	RegionName      string     `json:"region_name,omitempty"`
	IPAddress       string     `json:"ip_address"`
	Port            int32      `json:"port"`
	NodeStatus      NodeStatus `json:"node_status"`
	MeshID          string     `json:"mesh_id,omitempty"` // Only set if node is part of a mesh
	CreatedAt       int64      `json:"created_at"`
	UpdatedAt       int64      `json:"updated_at"`
}

// Connection represents a connection between two nodes
type Connection struct {
	PeerNodeID      uint64           `json:"peer_node_id"`
	PeerNodeName    string           `json:"peer_node_name"`
	RemoteAddr      string           `json:"remote_addr"`
	Status          ConnectionStatus `json:"status"`
	RTTMicroseconds uint64           `json:"rtt_microseconds"`
	BytesSent       uint64           `json:"bytes_sent"`
	BytesReceived   uint64           `json:"bytes_received"`
	IsTLS           bool             `json:"is_tls"`
	ConnectedAt     int64            `json:"connected_at"`
}

// === Core Mesh Operations ===

// SeedMeshRequest represents a request to seed a new mesh
type SeedMeshRequest struct {
	MeshName        string `json:"mesh_name" validate:"required"`
	MeshDescription string `json:"mesh_description,omitempty"`
}

// SeedMeshResponse represents the response from seeding a mesh
type SeedMeshResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Mesh    Mesh   `json:"mesh"`
	Status  Status `json:"status"`
}

// JoinMeshRequest represents a request to join an existing mesh
type JoinMeshRequest struct {
	TargetAddress   string        `json:"target_address" validate:"required"`
	Strategy        *JoinStrategy `json:"strategy,omitempty"`        // Default: inherit
	TimeoutSeconds  *uint32       `json:"timeout_seconds,omitempty"` // Default: 30
}

// JoinMeshResponse represents the response from joining a mesh
type JoinMeshResponse struct {
	Message      string `json:"message"`
	Success      bool   `json:"success"`
	Mesh         Mesh   `json:"mesh"`
	PeerNodeID   uint64 `json:"peer_node_id"`
	RemoteAddr   string `json:"remote_addr"`
	Status       Status `json:"status"`
}

// ExtendMeshRequest represents a request to extend mesh to a clean node
type ExtendMeshRequest struct {
	TargetAddress   string        `json:"target_address" validate:"required"`
	Strategy        *JoinStrategy `json:"strategy,omitempty"`        // Default: inherit
	TimeoutSeconds  *uint32       `json:"timeout_seconds,omitempty"` // Default: 30
}

// ExtendMeshResponse represents the response from extending mesh
type ExtendMeshResponse struct {
	Message      string `json:"message"`
	Success      bool   `json:"success"`
	PeerNodeID   uint64 `json:"peer_node_id"`
	RemoteAddr   string `json:"remote_addr"`
	Status       Status `json:"status"`
}

// LeaveMeshRequest represents a request to leave a mesh
type LeaveMeshRequest struct {
	Force bool `json:"force,omitempty"` // Force leave even if connections exist
}

// LeaveMeshResponse represents the response from leaving a mesh
type LeaveMeshResponse struct {
	Message            string `json:"message"`
	Success            bool   `json:"success"`
	ConnectionsDropped int32  `json:"connections_dropped"`
	Status             Status `json:"status"`
}

// EvictNodeRequest represents a request to evict a node from mesh
type EvictNodeRequest struct {
	TargetNodeID uint64 `json:"target_node_id" validate:"required"`
	CleanTarget  bool   `json:"clean_target,omitempty"` // Whether to clean the target node's configuration
}

// EvictNodeResponse represents the response from evicting a node
type EvictNodeResponse struct {
	Message       string `json:"message"`
	Success       bool   `json:"success"`
	TargetCleaned bool   `json:"target_cleaned"`
	Status        Status `json:"status"`
}

// === Connection Management ===

// AddConnectionRequest represents a request to add a connection
type AddConnectionRequest struct {
	TargetNodeID   uint64  `json:"target_node_id" validate:"required"`
	TimeoutSeconds *uint32 `json:"timeout_seconds,omitempty"` // Default: 30
}

// AddConnectionResponse represents the response from adding a connection
type AddConnectionResponse struct {
	Message    string     `json:"message"`
	Success    bool       `json:"success"`
	Connection Connection `json:"connection"`
	Status     Status     `json:"status"`
}

// DropConnectionRequest represents a request to drop a connection
type DropConnectionRequest struct {
	PeerNodeID uint64 `json:"peer_node_id" validate:"required"`
}

// DropConnectionResponse represents the response from dropping a connection
type DropConnectionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// ListConnectionsResponse represents the response from listing connections
type ListConnectionsResponse struct {
	Connections []Connection `json:"connections"`
}

// === Information and Status ===

// ShowMeshResponse represents the response from showing mesh details
type ShowMeshResponse struct {
	Mesh Mesh `json:"mesh"`
}

// ListNodesResponse represents the response from listing nodes
type ListNodesResponse struct {
	Nodes []Node `json:"nodes"`
}

// ShowNodeRequest represents a request to show node details
type ShowNodeRequest struct {
	NodeID *uint64 `json:"node_id,omitempty"` // If not provided, shows current node
}

// ShowNodeResponse represents the response from showing node details
type ShowNodeResponse struct {
	Node Node `json:"node"`
}

// GetNodeStatusResponse represents the response from getting node status
type GetNodeStatusResponse struct {
	Node        Node         `json:"node"`
	Connections []Connection `json:"connections"`
	Mesh        *Mesh        `json:"mesh,omitempty"` // Only set if node is in a mesh
}
