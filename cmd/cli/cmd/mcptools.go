package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/mcptools"
	"github.com/spf13/cobra"
)

// mcptoolsCmd represents the mcptools command
var mcptoolsCmd = &cobra.Command{
	Use:   "mcptools",
	Short: "Manage MCP tools",
	Long:  `Commands for managing MCP tools including listing, creating, and attaching tools to MCP servers.`,
}

// listMCPToolsCmd represents the list command
var listMCPToolsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all MCP tools",
	Long:  `Display a formatted list of all MCP tools in the active workspace.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcptools.ListMCPTools()
	},
}

// showMCPToolCmd represents the show command
var showMCPToolCmd = &cobra.Command{
	Use:   "show [tool-name]",
	Short: "Show MCP tool details",
	Long:  `Display detailed information about a specific MCP tool.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcptools.ShowMCPTool(args[0])
	},
}

// addMCPToolCmd represents the add command
var addMCPToolCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new MCP tool",
	Long: `Add a new MCP tool with specified configuration.

Examples:
  # Add MCP tool for querying database
  redb mcptools add --name query_tool --description "Query database tool" --mapping db_mapping --config '{"operation":"query_database","input_schema":{"type":"object","properties":{"database_id":{"type":"string"},"table_name":{"type":"string"}},"required":["database_id","table_name"]}}'
  
  # Add MCP tool with policies
  redb mcptools add --name secure_tool --description "Secure tool" --mapping my_mapping --config '{"operation":"get_schema"}' --policy-ids policy_001`,
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		description, _ := cmd.Flags().GetString("description")
		mapping, _ := cmd.Flags().GetString("mapping")
		config, _ := cmd.Flags().GetString("config")
		policyIDs, _ := cmd.Flags().GetStringSlice("policy-ids")

		return mcptools.AddMCPTool(name, description, mapping, config, policyIDs)
	},
}

// attachMCPToolCmd represents the attach command
var attachMCPToolCmd = &cobra.Command{
	Use:   "attach",
	Short: "Attach MCP tool to server",
	Long: `Attach an MCP tool to an MCP server.

Examples:
  # Attach tool to server
  redb mcptools attach --tool query_tool --server my-mcp-server`,
	RunE: func(cmd *cobra.Command, args []string) error {
		tool, _ := cmd.Flags().GetString("tool")
		server, _ := cmd.Flags().GetString("server")

		return mcptools.AttachMCPTool(tool, server)
	},
}

// detachMCPToolCmd represents the detach command
var detachMCPToolCmd = &cobra.Command{
	Use:   "detach",
	Short: "Detach MCP tool from server",
	Long: `Detach an MCP tool from an MCP server.

Examples:
  # Detach tool from server
  redb mcptools detach --tool query_tool --server my-mcp-server`,
	RunE: func(cmd *cobra.Command, args []string) error {
		tool, _ := cmd.Flags().GetString("tool")
		server, _ := cmd.Flags().GetString("server")

		return mcptools.DetachMCPTool(tool, server)
	},
}

// deleteMCPToolCmd represents the delete command
var deleteMCPToolCmd = &cobra.Command{
	Use:   "delete [tool-name]",
	Short: "Delete an MCP tool",
	Long:  `Delete an MCP tool.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcptools.DeleteMCPTool(args[0])
	},
}

func init() {
	// Add flags to addMCPToolCmd
	addMCPToolCmd.Flags().String("name", "", "MCP tool name (required)")
	addMCPToolCmd.Flags().String("description", "", "MCP tool description (optional, auto-generated if not provided)")
	addMCPToolCmd.Flags().String("mapping", "", "Mapping name (required)")
	addMCPToolCmd.Flags().String("config", "", "Tool configuration JSON (optional, auto-generated if not provided)")
	addMCPToolCmd.Flags().StringSlice("policy-ids", []string{}, "Policy IDs (comma-separated, optional)")

	// Mark required flags
	addMCPToolCmd.MarkFlagRequired("name")
	addMCPToolCmd.MarkFlagRequired("mapping")

	// Add flags to attachMCPToolCmd
	attachMCPToolCmd.Flags().String("tool", "", "MCP tool name (required)")
	attachMCPToolCmd.Flags().String("server", "", "MCP server name (required)")
	attachMCPToolCmd.MarkFlagRequired("tool")
	attachMCPToolCmd.MarkFlagRequired("server")

	// Add flags to detachMCPToolCmd
	detachMCPToolCmd.Flags().String("tool", "", "MCP tool name (required)")
	detachMCPToolCmd.Flags().String("server", "", "MCP server name (required)")
	detachMCPToolCmd.MarkFlagRequired("tool")
	detachMCPToolCmd.MarkFlagRequired("server")

	// Add subcommands to mcptools command
	mcptoolsCmd.AddCommand(listMCPToolsCmd)
	mcptoolsCmd.AddCommand(showMCPToolCmd)
	mcptoolsCmd.AddCommand(addMCPToolCmd)
	mcptoolsCmd.AddCommand(attachMCPToolCmd)
	mcptoolsCmd.AddCommand(detachMCPToolCmd)
	mcptoolsCmd.AddCommand(deleteMCPToolCmd)
}
