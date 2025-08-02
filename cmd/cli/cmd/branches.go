package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/branches"
	"github.com/spf13/cobra"
)

// branchesCmd represents the branches command
var branchesCmd = &cobra.Command{
	Use:   "branches",
	Short: "Manage branches",
	Long: "Commands for managing branches including showing details, modifying, attaching, detaching, " +
		"and deleting branches.",
}

// showBranchCmd represents the show command
var showBranchCmd = &cobra.Command{
	Use:   "show [repo/branch]",
	Short: "Show branch details",
	Long:  `Display detailed information about a specific branch in the format repo/branch.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return branches.ShowBranch(args[0])
	},
}

// modifyBranchCmd represents the modify command
var modifyBranchCmd = &cobra.Command{
	Use:   "modify [repo/branch]",
	Short: "Modify an existing branch",
	Long:  `Modify an existing branch by providing the repo/branch format and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return branches.ModifyBranch(args[0], args[1:])
	},
}

// attachBranchCmd represents the attach command
var attachBranchCmd = &cobra.Command{
	Use:   "attach [repo/branch]",
	Short: "Attach a branch to a connected database",
	Long:  `Attach a branch to a connected database by providing the repo/branch format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return branches.AttachBranch(args[0], args[1:])
	},
}

// detachBranchCmd represents the detach command
var detachBranchCmd = &cobra.Command{
	Use:   "detach [repo/branch]",
	Short: "Detach a branch from an attached database",
	Long:  `Detach a branch from an attached database by providing the repo/branch format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return branches.DetachBranch(args[0], args[1:])
	},
}

// deleteBranchCmd represents the delete command
var deleteBranchCmd = &cobra.Command{
	Use:   "delete [repo/branch]",
	Short: "Delete a branch",
	Long:  `Delete a branch by providing the repo/branch format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return branches.DeleteBranch(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to branches command
	branchesCmd.AddCommand(showBranchCmd)
	branchesCmd.AddCommand(modifyBranchCmd)
	branchesCmd.AddCommand(attachBranchCmd)
	branchesCmd.AddCommand(detachBranchCmd)
	branchesCmd.AddCommand(deleteBranchCmd)
}
