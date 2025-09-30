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

// deploySchemaCmd represents the deploy-schema command
var deploySchemaCmd = &cobra.Command{
	Use:   "deploy-schema [repo/branch/commit]",
	Short: "Deploy schema from a specific commit to a target database",
	Long: `Deploy schema from a historical commit to a target database.
	
Examples:
  # Deploy to new database on instance
  redb commits deploy-schema myrepo/main/abc123 --instance prod-mysql --db-name new_app_db
  
  # Deploy to existing database (wipe first)
  redb commits deploy-schema myrepo/main/abc123 --database existing_db --wipe
  
  # Deploy to existing database (merge)
  redb commits deploy-schema myrepo/main/abc123 --database existing_db --merge`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return commits.DeploySchema(args[0], cmd.Flags())
	},
}

func init() {
	// Add flags to deploySchemaCmd
	// Target options (mutually exclusive)
	deploySchemaCmd.Flags().String("instance", "", "Target instance name for new database")
	deploySchemaCmd.Flags().String("db-name", "", "New database name (required with --instance)")
	deploySchemaCmd.Flags().String("database", "", "Existing target database name")

	// Deployment options
	deploySchemaCmd.Flags().Bool("wipe", false, "Wipe target database before deployment")
	deploySchemaCmd.Flags().Bool("merge", false, "Merge with existing schema")

	// Cross-node options
	deploySchemaCmd.Flags().Uint64("source-node", 0, "Source node ID (for cross-node operations)")
	deploySchemaCmd.Flags().Uint64("target-node", 0, "Target node ID (for cross-node operations)")

	// Add subcommands to commits command
	commitsCmd.AddCommand(showCommitCmd)
	commitsCmd.AddCommand(branchCommitCmd)
	commitsCmd.AddCommand(mergeCommitCmd)
	commitsCmd.AddCommand(deployCommitCmd)
	commitsCmd.AddCommand(deploySchemaCmd)
}
