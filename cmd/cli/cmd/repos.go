package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/repos"
	"github.com/spf13/cobra"
)

// reposCmd represents the repos command
var reposCmd = &cobra.Command{
	Use:   "repos",
	Short: "Manage repositories",
	Long:  `Commands for managing repositories including listing, showing details, adding, modifying, cloning, and deleting repositories.`,
}

// listReposCmd represents the list command
var listReposCmd = &cobra.Command{
	Use:   "list",
	Short: "List all repositories",
	Long:  `Display a formatted list of all repositories with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repos.ListRepos()
	},
}

// showRepoCmd represents the show command
var showRepoCmd = &cobra.Command{
	Use:   "show [repo-name]",
	Short: "Show repository details",
	Long:  `Display detailed information about a specific repository.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return repos.ShowRepo(args[0])
	},
}

// addRepoCmd represents the add command
var addRepoCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new repository",
	Long:  `Add a new repository by providing repository details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return repos.AddRepo(args)
	},
}

// modifyRepoCmd represents the modify command
var modifyRepoCmd = &cobra.Command{
	Use:   "modify [repo-name]",
	Short: "Modify an existing repository",
	Long:  `Modify an existing repository by providing the repository name and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return repos.ModifyRepo(args[0], args[1:])
	},
}

// deleteRepoCmd represents the delete command
var deleteRepoCmd = &cobra.Command{
	Use:   "delete [repo-name]",
	Short: "Delete a repository",
	Long:  `Delete a repository by providing the repository name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return repos.DeleteRepo(args[0], args[1:])
	},
}

// cloneRepoCmd represents the clone command
var cloneRepoCmd = &cobra.Command{
	Use:   "clone [repo-name]",
	Short: "Clone an existing repository",
	Long:  `Clone an existing repository by providing the repository name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return repos.CloneRepo(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to repos command
	reposCmd.AddCommand(listReposCmd)
	reposCmd.AddCommand(showRepoCmd)
	reposCmd.AddCommand(addRepoCmd)
	reposCmd.AddCommand(modifyRepoCmd)
	reposCmd.AddCommand(deleteRepoCmd)
	reposCmd.AddCommand(cloneRepoCmd)
}
