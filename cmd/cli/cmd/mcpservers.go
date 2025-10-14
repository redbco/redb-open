package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/mcpservers"
	"github.com/spf13/cobra"
)

// mcpserversCmd represents the mcpservers command
var mcpserversCmd = &cobra.Command{
	Use:   "mcpservers",
	Short: "Manage MCP servers",
	Long:  `Commands for managing MCP (Model Context Protocol) servers including listing, showing details, and managing server configuration.`,
}

// listMCPServersCmd represents the list command
var listMCPServersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all MCP servers",
	Long:  `Display a formatted list of all MCP servers in the active workspace with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcpservers.ListMCPServers()
	},
}

// showMCPServerCmd represents the show command
var showMCPServerCmd = &cobra.Command{
	Use:   "show [server-name]",
	Short: "Show MCP server details",
	Long:  `Display detailed information about a specific MCP server.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcpservers.ShowMCPServer(args[0])
	},
}

// addMCPServerCmd represents the add command
var addMCPServerCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new MCP server",
	Long: `Add a new MCP server with specified configuration.

Examples:
  # Add MCP server with defaults (local node, auto description)
  redb mcpservers add --name my-mcp-server --port 9000
  
  # Add MCP server on specific nodes
  redb mcpservers add --name my-mcp-server --description "My MCP Server" --port 9000 --nodes node_123,node_456
  
  # Add MCP server with policies
  redb mcpservers add --name secure-mcp --description "Secure MCP Server" --port 9001 --policy-ids policy_001,policy_002`,
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		description, _ := cmd.Flags().GetString("description")
		port, _ := cmd.Flags().GetInt32("port")
		nodes, _ := cmd.Flags().GetStringSlice("nodes")
		enabled, _ := cmd.Flags().GetBool("enabled")
		policyIDs, _ := cmd.Flags().GetStringSlice("policy-ids")

		return mcpservers.AddMCPServer(name, description, port, nodes, enabled, policyIDs)
	},
}

// modifyMCPServerCmd represents the modify command
var modifyMCPServerCmd = &cobra.Command{
	Use:   "modify [server-name]",
	Short: "Modify an existing MCP server",
	Long: `Modify an existing MCP server's configuration.

Examples:
  # Change port
  redb mcpservers modify my-mcp-server --port 9002
  
  # Enable/disable server
  redb mcpservers modify my-mcp-server --enabled=false
  
  # Update description and add nodes
  redb mcpservers modify my-mcp-server --description "Updated description" --nodes node_123,node_789`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		description, _ := cmd.Flags().GetString("description")
		port, _ := cmd.Flags().GetInt32("port")
		nodes, _ := cmd.Flags().GetStringSlice("nodes")
		enabled, _ := cmd.Flags().GetBool("enabled")
		policyIDs, _ := cmd.Flags().GetStringSlice("policy-ids")

		return mcpservers.ModifyMCPServer(args[0], description, port, nodes, enabled, policyIDs)
	},
}

// deleteMCPServerCmd represents the delete command
var deleteMCPServerCmd = &cobra.Command{
	Use:   "delete [server-name]",
	Short: "Delete an MCP server",
	Long:  `Delete an MCP server and stop it from serving.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcpservers.DeleteMCPServer(args[0])
	},
}

func init() {
	// Add flags to addMCPServerCmd
	addMCPServerCmd.Flags().String("name", "", "MCP server name (required)")
	addMCPServerCmd.Flags().String("description", "", "MCP server description (optional, auto-generated if not provided)")
	addMCPServerCmd.Flags().Int32("port", 9000, "Port number for MCP server")
	addMCPServerCmd.Flags().StringSlice("nodes", []string{}, "Node IDs where this server should run (optional, defaults to local node)")
	addMCPServerCmd.Flags().Bool("enabled", true, "Enable the MCP server")
	addMCPServerCmd.Flags().StringSlice("policy-ids", []string{}, "Policy IDs (comma-separated, optional)")

	// Mark required flags
	addMCPServerCmd.MarkFlagRequired("name")

	// Add flags to modifyMCPServerCmd
	modifyMCPServerCmd.Flags().String("description", "", "MCP server description")
	modifyMCPServerCmd.Flags().Int32("port", 0, "Port number for MCP server")
	modifyMCPServerCmd.Flags().StringSlice("nodes", []string{}, "Node IDs where this server should run (comma-separated)")
	modifyMCPServerCmd.Flags().Bool("enabled", true, "Enable/disable the MCP server")
	modifyMCPServerCmd.Flags().StringSlice("policy-ids", []string{}, "Policy IDs (comma-separated)")

	// Add subcommands to mcpservers command
	mcpserversCmd.AddCommand(listMCPServersCmd)
	mcpserversCmd.AddCommand(showMCPServerCmd)
	mcpserversCmd.AddCommand(addMCPServerCmd)
	mcpserversCmd.AddCommand(modifyMCPServerCmd)
	mcpserversCmd.AddCommand(deleteMCPServerCmd)
}
