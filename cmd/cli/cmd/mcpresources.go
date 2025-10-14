package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/mcpresources"
	"github.com/spf13/cobra"
)

// mcpresourcesCmd represents the mcpresources command
var mcpresourcesCmd = &cobra.Command{
	Use:   "mcpresources",
	Short: "Manage MCP resources",
	Long:  `Commands for managing MCP resources including listing, creating, and attaching resources to MCP servers.`,
}

// listMCPResourcesCmd represents the list command
var listMCPResourcesCmd = &cobra.Command{
	Use:   "list",
	Short: "List all MCP resources",
	Long:  `Display a formatted list of all MCP resources in the active workspace.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcpresources.ListMCPResources()
	},
}

// showMCPResourceCmd represents the show command
var showMCPResourceCmd = &cobra.Command{
	Use:   "show [resource-name]",
	Short: "Show MCP resource details",
	Long:  `Display detailed information about a specific MCP resource.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcpresources.ShowMCPResource(args[0])
	},
}

// addMCPResourceCmd represents the add command
var addMCPResourceCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new MCP resource",
	Long: `Add a new MCP resource with specified configuration.

Examples:
  # Add MCP resource for a direct table
  redb mcpresources add --name users_resource --description "Users table resource" --mapping users_mapping --config '{"type":"direct_table","database_id":"mydb","table_name":"users"}'
  
  # Add MCP resource with policies
  redb mcpresources add --name secure_resource --description "Secure resource" --mapping my_mapping --config '{"type":"mapped_table","database_id":"db1","table_name":"data"}' --policy-ids policy_001`,
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		description, _ := cmd.Flags().GetString("description")
		mapping, _ := cmd.Flags().GetString("mapping")
		config, _ := cmd.Flags().GetString("config")
		policyIDs, _ := cmd.Flags().GetStringSlice("policy-ids")

		return mcpresources.AddMCPResource(name, description, mapping, config, policyIDs)
	},
}

// attachMCPResourceCmd represents the attach command
var attachMCPResourceCmd = &cobra.Command{
	Use:   "attach",
	Short: "Attach MCP resource to server",
	Long: `Attach an MCP resource to an MCP server.

Examples:
  # Attach resource to server
  redb mcpresources attach --resource users_resource --server my-mcp-server`,
	RunE: func(cmd *cobra.Command, args []string) error {
		resource, _ := cmd.Flags().GetString("resource")
		server, _ := cmd.Flags().GetString("server")

		return mcpresources.AttachMCPResource(resource, server)
	},
}

// detachMCPResourceCmd represents the detach command
var detachMCPResourceCmd = &cobra.Command{
	Use:   "detach",
	Short: "Detach MCP resource from server",
	Long: `Detach an MCP resource from an MCP server.

Examples:
  # Detach resource from server
  redb mcpresources detach --resource users_resource --server my-mcp-server`,
	RunE: func(cmd *cobra.Command, args []string) error {
		resource, _ := cmd.Flags().GetString("resource")
		server, _ := cmd.Flags().GetString("server")

		return mcpresources.DetachMCPResource(resource, server)
	},
}

// deleteMCPResourceCmd represents the delete command
var deleteMCPResourceCmd = &cobra.Command{
	Use:   "delete [resource-name]",
	Short: "Delete an MCP resource",
	Long:  `Delete an MCP resource.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcpresources.DeleteMCPResource(args[0])
	},
}

func init() {
	// Add flags to addMCPResourceCmd
	addMCPResourceCmd.Flags().String("name", "", "MCP resource name (required)")
	addMCPResourceCmd.Flags().String("description", "", "MCP resource description (optional, auto-generated if not provided)")
	addMCPResourceCmd.Flags().String("mapping", "", "Mapping name (required)")
	addMCPResourceCmd.Flags().String("config", "", "Resource configuration JSON (optional, auto-generated if not provided)")
	addMCPResourceCmd.Flags().StringSlice("policy-ids", []string{}, "Policy IDs (comma-separated, optional)")

	// Mark required flags
	addMCPResourceCmd.MarkFlagRequired("name")
	addMCPResourceCmd.MarkFlagRequired("mapping")

	// Add flags to attachMCPResourceCmd
	attachMCPResourceCmd.Flags().String("resource", "", "MCP resource name (required)")
	attachMCPResourceCmd.Flags().String("server", "", "MCP server name (required)")
	attachMCPResourceCmd.MarkFlagRequired("resource")
	attachMCPResourceCmd.MarkFlagRequired("server")

	// Add flags to detachMCPResourceCmd
	detachMCPResourceCmd.Flags().String("resource", "", "MCP resource name (required)")
	detachMCPResourceCmd.Flags().String("server", "", "MCP server name (required)")
	detachMCPResourceCmd.MarkFlagRequired("resource")
	detachMCPResourceCmd.MarkFlagRequired("server")

	// Add subcommands to mcpresources command
	mcpresourcesCmd.AddCommand(listMCPResourcesCmd)
	mcpresourcesCmd.AddCommand(showMCPResourceCmd)
	mcpresourcesCmd.AddCommand(addMCPResourceCmd)
	mcpresourcesCmd.AddCommand(attachMCPResourceCmd)
	mcpresourcesCmd.AddCommand(detachMCPResourceCmd)
	mcpresourcesCmd.AddCommand(deleteMCPResourceCmd)
}
