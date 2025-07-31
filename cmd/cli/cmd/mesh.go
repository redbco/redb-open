package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/mesh"
	"github.com/spf13/cobra"
)

// meshCmd represents the mesh command
var meshCmd = &cobra.Command{
	Use:   "mesh",
	Short: "Manage mesh networks",
	Long:  `Commands for managing mesh networks including seeding, joining, and viewing mesh information.`,
}

// seedMeshCmd represents the seed command
var seedMeshCmd = &cobra.Command{
	Use:   "seed",
	Short: "Seed a new mesh network",
	Long:  `Create a new mesh network by seeding it with an initial node.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.SeedMesh(args)
	},
}

// joinMeshCmd represents the join command
var joinMeshCmd = &cobra.Command{
	Use:   "join [mesh-id]",
	Short: "Join an existing mesh network",
	Long:  `Join an existing mesh network by providing the mesh ID and node details.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.JoinMesh(args[0])
	},
}

// showMeshCmd represents the show command
var showMeshCmd = &cobra.Command{
	Use:   "show [mesh-id]",
	Short: "Show mesh network details",
	Long:  `Display detailed information about a specific mesh network.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.ShowMesh(args[0])
	},
}

// listNodesCmd represents the nodes command
var listNodesCmd = &cobra.Command{
	Use:   "nodes [mesh-id]",
	Short: "List nodes in mesh network",
	Long:  `Display a list of all nodes in a specific mesh network.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mesh.ListNodes(args[0])
	},
}

func init() {
	rootCmd.AddCommand(meshCmd)
	meshCmd.AddCommand(seedMeshCmd)
	meshCmd.AddCommand(joinMeshCmd)
	meshCmd.AddCommand(showMeshCmd)
	meshCmd.AddCommand(listNodesCmd)
}