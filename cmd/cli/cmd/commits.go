package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/commits"
	"github.com/spf13/cobra"
)

// commitsCmd represents the commits command
var commitsCmd = &cobra.Command{
	Use:   "commits",
	Short: "Manage commits",
	Long:  `Commands for managing commits including showing details, branching, merging, and deploying commits.`,
}

// showCommitCmd represents the show command
var showCommitCmd = &cobra.Command{
	Use:   "show [repo/branch/commit]",
	Short: "Show commit details",
	Long:  `Display detailed information about a specific commit in the format repo/branch/commit.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return commits.ShowCommit(args[0])
	},
}

// branchCommitCmd represents the branch command
var branchCommitCmd = &cobra.Command{
	Use:   "branch [repo/branch/commit]",
	Short: "Branch a commit into a new branch",
	Long:  `Branch a commit into a new branch by providing the repo/branch/commit format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return commits.BranchCommit(args[0], args[1:])
	},
}

// mergeCommitCmd represents the merge command
var mergeCommitCmd = &cobra.Command{
	Use:   "merge [repo/branch/commit]",
	Short: "Merge a commit to the parent branch",
	Long:  `Merge a commit to the parent branch by providing the repo/branch/commit format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return commits.MergeCommit(args[0], args[1:])
	},
}

// deployCommitCmd represents the deploy command
var deployCommitCmd = &cobra.Command{
	Use:   "deploy [repo/branch/commit]",
	Short: "Deploy the commit to the database attached to the branch",
	Long:  `Deploy the commit to the database attached to the branch by providing the repo/branch/commit format.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return commits.DeployCommit(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to commits command
	commitsCmd.AddCommand(showCommitCmd)
	commitsCmd.AddCommand(branchCommitCmd)
	commitsCmd.AddCommand(mergeCommitCmd)
	commitsCmd.AddCommand(deployCommitCmd)
}
