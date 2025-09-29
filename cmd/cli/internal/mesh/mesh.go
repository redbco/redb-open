package mesh

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

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
	Status          string `json:"status"`
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

// === Request/Response Types ===

type SeedMeshRequest struct {
	MeshName        string `json:"mesh_name"`
	MeshDescription string `json:"mesh_description,omitempty"`
}

type SeedMeshResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Mesh    Mesh   `json:"mesh"`
	Status  string `json:"status"`
}

type JoinMeshRequest struct {
	TargetAddress  string        `json:"target_address"`
	Strategy       *JoinStrategy `json:"strategy,omitempty"`
	TimeoutSeconds *uint32       `json:"timeout_seconds,omitempty"`
}

type JoinMeshResponse struct {
	Message    string `json:"message"`
	Success    bool   `json:"success"`
	Mesh       Mesh   `json:"mesh"`
	PeerNodeID uint64 `json:"peer_node_id"`
	RemoteAddr string `json:"remote_addr"`
	Status     string `json:"status"`
}

type ExtendMeshRequest struct {
	TargetAddress  string        `json:"target_address"`
	Strategy       *JoinStrategy `json:"strategy,omitempty"`
	TimeoutSeconds *uint32       `json:"timeout_seconds,omitempty"`
}

type ExtendMeshResponse struct {
	Message    string `json:"message"`
	Success    bool   `json:"success"`
	PeerNodeID uint64 `json:"peer_node_id"`
	RemoteAddr string `json:"remote_addr"`
	Status     string `json:"status"`
}

type LeaveMeshRequest struct {
	Force bool `json:"force,omitempty"`
}

type LeaveMeshResponse struct {
	Message            string `json:"message"`
	Success            bool   `json:"success"`
	ConnectionsDropped int32  `json:"connections_dropped"`
	Status             string `json:"status"`
}

type EvictNodeRequest struct {
	TargetNodeID uint64 `json:"target_node_id"`
	CleanTarget  bool   `json:"clean_target,omitempty"`
}

type EvictNodeResponse struct {
	Message       string `json:"message"`
	Success       bool   `json:"success"`
	TargetCleaned bool   `json:"target_cleaned"`
	Status        string `json:"status"`
}

type AddConnectionRequest struct {
	TargetNodeID   uint64  `json:"target_node_id"`
	TimeoutSeconds *uint32 `json:"timeout_seconds,omitempty"`
}

type AddConnectionResponse struct {
	Message    string     `json:"message"`
	Success    bool       `json:"success"`
	Connection Connection `json:"connection"`
	Status     string     `json:"status"`
}

type DropConnectionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

type ListConnectionsResponse struct {
	Connections []Connection `json:"connections"`
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

type GetNodeStatusResponse struct {
	Node        Node         `json:"node"`
	Connections []Connection `json:"connections"`
	Mesh        *Mesh        `json:"mesh,omitempty"`
}

// === Core Mesh Operations ===

// SeedMesh creates a new mesh network
func SeedMesh() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	reader := bufio.NewReader(os.Stdin)

	// Get mesh name
	fmt.Print("Mesh name: ")
	meshName, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read mesh name: %v", err)
	}
	meshName = strings.TrimSpace(meshName)

	if meshName == "" {
		return fmt.Errorf("mesh name cannot be empty")
	}

	// Get mesh description (optional)
	fmt.Print("Mesh description (optional): ")
	meshDescription, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read mesh description: %v", err)
	}
	meshDescription = strings.TrimSpace(meshDescription)

	// Create the request
	seedReq := SeedMeshRequest{
		MeshName:        meshName,
		MeshDescription: meshDescription,
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/seed")

	var response SeedMeshResponse
	if err := client.Post(url, seedReq, &response); err != nil {
		return fmt.Errorf("failed to seed mesh: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to seed mesh: %s", response.Message)
	}

	fmt.Printf("✅ Successfully seeded mesh network!\n\n")
	fmt.Printf("Mesh ID: %s\n", response.Mesh.MeshID)
	fmt.Printf("Mesh Name: %s\n", response.Mesh.MeshName)
	fmt.Printf("Description: %s\n", response.Mesh.MeshDescription)
	fmt.Printf("Allow Join: %t\n", response.Mesh.AllowJoin)
	fmt.Printf("Node Count: %d\n", response.Mesh.NodeCount)
	fmt.Printf("Connection Count: %d\n", response.Mesh.ConnectionCount)
	fmt.Printf("Status: %s\n", response.Mesh.Status)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// JoinMesh joins an existing mesh network
func JoinMesh(targetAddress, strategy string, timeout uint32) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	if targetAddress == "" {
		return fmt.Errorf("target address is required")
	}

	// Validate strategy
	var joinStrategy *JoinStrategy
	if strategy != "" {
		switch strings.ToLower(strategy) {
		case "inherit":
			s := JoinStrategyInherit
			joinStrategy = &s
		case "merge":
			s := JoinStrategyMerge
			joinStrategy = &s
		case "overwrite":
			s := JoinStrategyOverwrite
			joinStrategy = &s
		default:
			return fmt.Errorf("invalid strategy: %s (must be inherit, merge, or overwrite)", strategy)
		}
	}

	// Create the request
	joinReq := JoinMeshRequest{
		TargetAddress: targetAddress,
		Strategy:      joinStrategy,
	}

	if timeout > 0 {
		joinReq.TimeoutSeconds = &timeout
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/join")

	var response JoinMeshResponse
	if err := client.Post(url, joinReq, &response); err != nil {
		return fmt.Errorf("failed to join mesh: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to join mesh: %s", response.Message)
	}

	fmt.Printf("✅ Successfully joined mesh network!\n\n")
	fmt.Printf("Target Address: %s\n", targetAddress)
	fmt.Printf("Connected to Peer Node ID: %d\n", response.PeerNodeID)
	fmt.Printf("Remote Address: %s\n", response.RemoteAddr)
	fmt.Printf("Mesh ID: %s\n", response.Mesh.MeshID)
	fmt.Printf("Mesh Name: %s\n", response.Mesh.MeshName)
	fmt.Printf("Node Count: %d\n", response.Mesh.NodeCount)
	fmt.Printf("Connection Count: %d\n", response.Mesh.ConnectionCount)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// ExtendMesh extends the current mesh to a clean node
func ExtendMesh(targetAddress, strategy string, timeout uint32) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	if targetAddress == "" {
		return fmt.Errorf("target address is required")
	}

	// Validate strategy
	var joinStrategy *JoinStrategy
	if strategy != "" {
		switch strings.ToLower(strategy) {
		case "inherit":
			s := JoinStrategyInherit
			joinStrategy = &s
		case "merge":
			s := JoinStrategyMerge
			joinStrategy = &s
		case "overwrite":
			s := JoinStrategyOverwrite
			joinStrategy = &s
		default:
			return fmt.Errorf("invalid strategy: %s (must be inherit, merge, or overwrite)", strategy)
		}
	}

	// Create the request
	extendReq := ExtendMeshRequest{
		TargetAddress: targetAddress,
		Strategy:      joinStrategy,
	}

	if timeout > 0 {
		extendReq.TimeoutSeconds = &timeout
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/extend")

	var response ExtendMeshResponse
	if err := client.Post(url, extendReq, &response); err != nil {
		return fmt.Errorf("failed to extend mesh: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to extend mesh: %s", response.Message)
	}

	fmt.Printf("✅ Successfully extended mesh to clean node!\n\n")
	fmt.Printf("Target Address: %s\n", targetAddress)
	fmt.Printf("Connected to Peer Node ID: %d\n", response.PeerNodeID)
	fmt.Printf("Remote Address: %s\n", response.RemoteAddr)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// LeaveMesh removes the current node from its mesh
func LeaveMesh(force bool) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Create the request
	leaveReq := LeaveMeshRequest{
		Force: force,
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/leave")

	var response LeaveMeshResponse
	if err := client.Post(url, leaveReq, &response); err != nil {
		return fmt.Errorf("failed to leave mesh: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to leave mesh: %s", response.Message)
	}

	fmt.Printf("✅ Successfully left mesh network!\n\n")
	fmt.Printf("Connections Dropped: %d\n", response.ConnectionsDropped)
	fmt.Printf("Force: %t\n", force)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// EvictNode removes another node from the mesh
func EvictNode(nodeIDStr string, clean bool) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Parse node ID
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID: %s", nodeIDStr)
	}

	// Create the request
	evictReq := EvictNodeRequest{
		TargetNodeID: nodeID,
		CleanTarget:  clean,
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/evict")

	var response EvictNodeResponse
	if err := client.Post(url, evictReq, &response); err != nil {
		return fmt.Errorf("failed to evict node: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to evict node: %s", response.Message)
	}

	fmt.Printf("✅ Successfully evicted node from mesh!\n\n")
	fmt.Printf("Target Node ID: %d\n", nodeID)
	fmt.Printf("Target Cleaned: %t\n", response.TargetCleaned)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// === Connection Management ===

// AddConnection adds a connection to another node
func AddConnection(nodeIDStr string, timeout uint32) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Parse node ID
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID: %s", nodeIDStr)
	}

	// Create the request
	connectReq := AddConnectionRequest{
		TargetNodeID: nodeID,
	}

	if timeout > 0 {
		connectReq.TimeoutSeconds = &timeout
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/connections")

	var response AddConnectionResponse
	if err := client.Post(url, connectReq, &response); err != nil {
		return fmt.Errorf("failed to add connection: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to add connection: %s", response.Message)
	}

	fmt.Printf("✅ Successfully added connection!\n\n")
	fmt.Printf("Peer Node ID: %d\n", response.Connection.PeerNodeID)
	fmt.Printf("Peer Node Name: %s\n", response.Connection.PeerNodeName)
	fmt.Printf("Remote Address: %s\n", response.Connection.RemoteAddr)
	fmt.Printf("Status: %s\n", response.Connection.Status)
	fmt.Printf("TLS: %t\n", response.Connection.IsTLS)
	fmt.Printf("\nMessage: %s\n", response.Message)

	return nil
}

// DropConnection drops a connection to another node
func DropConnection(nodeIDStr string) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Parse node ID
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid node ID: %s", nodeIDStr)
	}

	url := fmt.Sprintf("%s/mesh/connections/%d", common.BuildGlobalAPIURL(profileInfo, ""), nodeID)

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to drop connection: %v", err)
	}

	fmt.Printf("✅ Successfully dropped connection!\n\n")
	fmt.Printf("Peer Node ID: %d\n", nodeID)

	return nil
}

// ListConnections lists all active connections
func ListConnections() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/connections")

	var response ListConnectionsResponse
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list connections: %v", err)
	}

	connections := response.Connections

	if len(connections) == 0 {
		fmt.Printf("No active connections found\n")
		return nil
	}

	fmt.Printf("Active connections:\n\n")

	// Create a table writer
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "PEER NODE ID\tPEER NAME\tREMOTE ADDRESS\tSTATUS\tRTT (μs)\tBYTES SENT\tBYTES RECV\tTLS")
	fmt.Fprintln(w, "------------\t---------\t--------------\t------\t--------\t----------\t----------\t---")

	for _, conn := range connections {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%d\t%d\t%t\n",
			conn.PeerNodeID,
			conn.PeerNodeName,
			conn.RemoteAddr,
			conn.Status,
			conn.RTTMicroseconds,
			conn.BytesSent,
			conn.BytesReceived,
			conn.IsTLS,
		)
	}

	_ = w.Flush()

	return nil
}

// === Information and Status ===

// ShowMesh displays information about the current mesh
func ShowMesh() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh")

	var response ShowMeshResponse
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get mesh details: %v", err)
	}

	mesh := response.Mesh

	fmt.Printf("Current Mesh Details:\n\n")
	fmt.Printf("ID: %s\n", mesh.MeshID)
	fmt.Printf("Name: %s\n", mesh.MeshName)
	fmt.Printf("Description: %s\n", mesh.MeshDescription)
	fmt.Printf("Allow Join: %t\n", mesh.AllowJoin)
	fmt.Printf("Node Count: %d\n", mesh.NodeCount)
	fmt.Printf("Connection Count: %d\n", mesh.ConnectionCount)
	fmt.Printf("Status: %s\n", mesh.Status)
	fmt.Printf("Created At: %d\n", mesh.CreatedAt)
	fmt.Printf("Updated At: %d\n", mesh.UpdatedAt)

	return nil
}

// ListNodes displays all nodes in the current mesh
func ListNodes() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/mesh/nodes")

	var response ListNodesResponse
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get nodes: %v", err)
	}

	nodes := response.Nodes

	if len(nodes) == 0 {
		fmt.Printf("No nodes found in current mesh\n")
		return nil
	}

	fmt.Printf("Nodes in current mesh:\n\n")

	// Create a table writer
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NODE ID\tNAME\tDESCRIPTION\tPLATFORM\tVERSION\tIP\tPORT\tSTATUS")
	fmt.Fprintln(w, "-------\t----\t-----------\t--------\t-------\t--\t----\t------")

	for _, node := range nodes {
		description := node.NodeDescription
		if description == "" {
			description = "-"
		}

		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
			node.NodeID,
			node.NodeName,
			description,
			node.NodePlatform,
			node.NodeVersion,
			node.IPAddress,
			node.Port,
			node.NodeStatus,
		)
	}

	_ = w.Flush()

	return nil
}

// ShowNode displays information about a specific node or current node
func ShowNode(nodeIDStr string) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	var url string
	if nodeIDStr == "" {
		// Show current node
		url = common.BuildGlobalAPIURL(profileInfo, "/mesh/nodes")
	} else {
		// Show specific node
		nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid node ID: %s", nodeIDStr)
		}
		url = fmt.Sprintf("%s/mesh/nodes/%d", common.BuildGlobalAPIURL(profileInfo, ""), nodeID)
	}

	var response ShowNodeResponse
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get node details: %v", err)
	}

	node := response.Node

	fmt.Printf("Node Details:\n\n")
	fmt.Printf("ID: %d\n", node.NodeID)
	fmt.Printf("Name: %s\n", node.NodeName)
	fmt.Printf("Description: %s\n", node.NodeDescription)
	fmt.Printf("Platform: %s\n", node.NodePlatform)
	fmt.Printf("Version: %s\n", node.NodeVersion)
	fmt.Printf("Region ID: %s\n", node.RegionID)
	fmt.Printf("Region Name: %s\n", node.RegionName)
	fmt.Printf("IP Address: %s\n", node.IPAddress)
	fmt.Printf("Port: %d\n", node.Port)
	fmt.Printf("Status: %s\n", node.NodeStatus)
	fmt.Printf("Mesh ID: %s\n", node.MeshID)
	fmt.Printf("Created At: %d\n", node.CreatedAt)
	fmt.Printf("Updated At: %d\n", node.UpdatedAt)

	return nil
}

// GetNodeStatus displays comprehensive status for the current node
func GetNodeStatus() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url := common.BuildGlobalAPIURL(profileInfo, "/node/status")

	var response GetNodeStatusResponse
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get node status: %v", err)
	}

	node := response.Node
	connections := response.Connections
	mesh := response.Mesh

	fmt.Printf("Node Status:\n\n")
	fmt.Printf("=== Node Information ===\n")
	fmt.Printf("ID: %d\n", node.NodeID)
	fmt.Printf("Name: %s\n", node.NodeName)
	fmt.Printf("Description: %s\n", node.NodeDescription)
	fmt.Printf("Platform: %s\n", node.NodePlatform)
	fmt.Printf("Version: %s\n", node.NodeVersion)
	fmt.Printf("IP Address: %s\n", node.IPAddress)
	fmt.Printf("Port: %d\n", node.Port)
	fmt.Printf("Status: %s\n", node.NodeStatus)

	if mesh != nil {
		fmt.Printf("\n=== Mesh Information ===\n")
		fmt.Printf("Mesh ID: %s\n", mesh.MeshID)
		fmt.Printf("Mesh Name: %s\n", mesh.MeshName)
		fmt.Printf("Description: %s\n", mesh.MeshDescription)
		fmt.Printf("Node Count: %d\n", mesh.NodeCount)
		fmt.Printf("Connection Count: %d\n", mesh.ConnectionCount)
		fmt.Printf("Status: %s\n", mesh.Status)
	} else {
		fmt.Printf("\n=== Mesh Information ===\n")
		fmt.Printf("Node is not part of any mesh (status: %s)\n", node.NodeStatus)
	}

	fmt.Printf("\n=== Active Connections ===\n")
	if len(connections) == 0 {
		fmt.Printf("No active connections\n")
	} else {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "PEER ID\tPEER NAME\tREMOTE ADDRESS\tSTATUS\tRTT (μs)\tTLS")
		fmt.Fprintln(w, "-------\t---------\t--------------\t------\t--------\t---")

		for _, conn := range connections {
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%t\n",
				conn.PeerNodeID,
				conn.PeerNodeName,
				conn.RemoteAddr,
				conn.Status,
				conn.RTTMicroseconds,
				conn.IsTLS,
			)
		}

		_ = w.Flush()
	}

	return nil
}
