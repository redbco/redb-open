package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/workspaces"
	"github.com/spf13/cobra"
)

// workspacesCmd represents the workspaces command
var workspacesCmd = &cobra.Command{
	Use:   "workspaces",
	Short: "Manage workspaces",
	Long: "Commands for managing workspaces including listing, showing details, adding, modifying, " +
		"and deleting workspaces.",
}

// listWorkspacesCmd represents the list command
var listWorkspacesCmd = &cobra.Command{
	Use:   "list",
	Short: "List all workspaces",
	Long:  `Display a formatted list of all workspaces with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return workspaces.ListWorkspaces()
	},
}

// showWorkspaceCmd represents the show command
var showWorkspaceCmd = &cobra.Command{
	Use:   "show [workspace-name]",
	Short: "Show workspace details",
	Long:  `Display detailed information about a specific workspace.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return workspaces.ShowWorkspace(args[0])
	},
}

// addWorkspaceCmd represents the add command
var addWorkspaceCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new workspace",
	Long:  `Add a new workspace by providing workspace details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return workspaces.AddWorkspace(args)
	},
}

// modifyWorkspaceCmd represents the modify command
var modifyWorkspaceCmd = &cobra.Command{
	Use:   "modify [workspace-name]",
	Short: "Modify an existing workspace",
	Long:  `Modify an existing workspace by providing the workspace name and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return workspaces.ModifyWorkspace(args[0], args[1:])
	},
}

// deleteWorkspaceCmd represents the delete command
var deleteWorkspaceCmd = &cobra.Command{
	Use:   "delete [workspace-name]",
	Short: "Delete a workspace",
	Long:  `Delete a workspace by providing the workspace name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return workspaces.DeleteWorkspace(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to workspaces command
	workspacesCmd.AddCommand(listWorkspacesCmd)
	workspacesCmd.AddCommand(showWorkspaceCmd)
	workspacesCmd.AddCommand(addWorkspaceCmd)
	workspacesCmd.AddCommand(modifyWorkspaceCmd)
	workspacesCmd.AddCommand(deleteWorkspaceCmd)
}
