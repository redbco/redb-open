package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/mesh"
	"github.com/spf13/cobra"
)

// meshCmd represents the mesh command
var meshCmd = &cobra.Command{
	Use:   "mesh",
	Short: "Manage mesh networks",
	Long:  `Commands for managing mesh networks with the new mesh management approach.`,
}

// nodeCmd represents the node command
var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Manage node information",
	Long:  `Commands for managing node information and status.`,
}

// === Core Mesh Operations ===

// seedMeshCmd represents the seed command
var seedMeshCmd = &cobra.Command{
	Use:   "seed",
	Short: "Seed a new mesh network",
	Long:  `Create a new mesh network without connecting to any other nodes. The local node becomes the first member.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.SeedMesh()
	},
}

// joinMeshCmd represents the join command
var joinMeshCmd = &cobra.Command{
	Use:   "join <target_address>",
	Short: "Join an existing mesh network",
	Long:  `Join an existing mesh network by connecting to a node in that mesh. The local node must be clean (not part of any mesh).`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		strategy, _ := cmd.Flags().GetString("strategy")
		timeout, _ := cmd.Flags().GetUint32("timeout")
		return mesh.JoinMesh(args[0], strategy, timeout)
	},
}

// extendMeshCmd represents the extend command
var extendMeshCmd = &cobra.Command{
	Use:   "extend <target_address>",
	Short: "Extend mesh to a clean node",
	Long:  `Extend the current mesh to a clean node. The local node must be part of a mesh.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		strategy, _ := cmd.Flags().GetString("strategy")
		timeout, _ := cmd.Flags().GetUint32("timeout")
		return mesh.ExtendMesh(args[0], strategy, timeout)
	},
}

// leaveMeshCmd represents the leave command
var leaveMeshCmd = &cobra.Command{
	Use:   "leave",
	Short: "Leave the current mesh",
	Long:  `Remove the current node from its mesh gracefully.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		force, _ := cmd.Flags().GetBool("force")
		return mesh.LeaveMesh(force)
	},
}

// evictNodeCmd represents the evict command
var evictNodeCmd = &cobra.Command{
	Use:   "evict <node_id>",
	Short: "Evict a node from the mesh",
	Long:  `Forcefully remove another node from the mesh.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		clean, _ := cmd.Flags().GetBool("clean")
		return mesh.EvictNode(args[0], clean)
	},
}

// === Connection Management ===

// connectCmd represents the connect command
var connectCmd = &cobra.Command{
	Use:   "connect <node_id>",
	Short: "Add connection to a node",
	Long:  `Add a connection to another node in the same mesh.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		timeout, _ := cmd.Flags().GetUint32("timeout")
		return mesh.AddConnection(args[0], timeout)
	},
}

// disconnectCmd represents the disconnect command
var disconnectCmd = &cobra.Command{
	Use:   "disconnect <node_id>",
	Short: "Drop connection to a node",
	Long:  `Drop a connection to another node.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.DropConnection(args[0])
	},
}

// connectionsCmd represents the connections command
var connectionsCmd = &cobra.Command{
	Use:   "connections",
	Short: "List active connections",
	Long:  `List all active connections for the current node.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.ListConnections()
	},
}

// === Information and Status ===

// showMeshCmd represents the show command
var showMeshCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current mesh details",
	Long:  `Display detailed information about the current mesh (if node is part of one).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.ShowMesh()
	},
}

// listNodesCmd represents the nodes command
var listNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "List nodes in current mesh",
	Long:  `Display a list of all nodes in the current mesh.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.ListNodes()
	},
}

// showNodeCmd represents the show-node command
var showNodeCmd = &cobra.Command{
	Use:   "show-node [node_id]",
	Short: "Show node details",
	Long:  `Display detailed information about a specific node or the current node (if no ID provided).`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var nodeID string
		if len(args) > 0 {
			nodeID = args[0]
		}
		return mesh.ShowNode(nodeID)
	},
}

// nodeStatusCmd represents the node status command
var nodeStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show node status",
	Long:  `Display comprehensive status information for the current node.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.GetNodeStatus()
	},
}

func init() {
	// Add main commands
	rootCmd.AddCommand(meshCmd)
	rootCmd.AddCommand(nodeCmd)

	// === Core Mesh Operations ===
	meshCmd.AddCommand(seedMeshCmd)
	meshCmd.AddCommand(joinMeshCmd)
	meshCmd.AddCommand(extendMeshCmd)
	meshCmd.AddCommand(leaveMeshCmd)
	meshCmd.AddCommand(evictNodeCmd)

	// === Connection Management ===
	meshCmd.AddCommand(connectCmd)
	meshCmd.AddCommand(disconnectCmd)
	meshCmd.AddCommand(connectionsCmd)

	// === Information and Status ===
	meshCmd.AddCommand(showMeshCmd)
	meshCmd.AddCommand(listNodesCmd)
	meshCmd.AddCommand(showNodeCmd)

	// Node status command
	nodeCmd.AddCommand(nodeStatusCmd)

	// === Flags ===

	// Join mesh flags
	joinMeshCmd.Flags().String("strategy", "inherit", "Join strategy: inherit, merge, overwrite")
	joinMeshCmd.Flags().Uint32("timeout", 30, "Connection timeout in seconds")

	// Extend mesh flags
	extendMeshCmd.Flags().String("strategy", "inherit", "Extend strategy: inherit, merge, overwrite")
	extendMeshCmd.Flags().Uint32("timeout", 30, "Connection timeout in seconds")

	// Leave mesh flags
	leaveMeshCmd.Flags().Bool("force", false, "Force leave even if connections exist")

	// Evict node flags
	evictNodeCmd.Flags().Bool("clean", false, "Clean the target node's configuration")

	// Connect flags
	connectCmd.Flags().Uint32("timeout", 30, "Connection timeout in seconds")
}
