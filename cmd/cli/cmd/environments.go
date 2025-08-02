package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/environments"
	"github.com/spf13/cobra"
)

// environmentsCmd represents the environments command
var environmentsCmd = &cobra.Command{
	Use:   "environments",
	Short: "Manage environments",
	Long: "Commands for managing environments including listing, showing details, adding, modifying, " +
		"and deleting environments.",
}

// listEnvironmentsCmd represents the list command
var listEnvironmentsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all environments",
	Long:  `Display a formatted list of all environments with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return environments.ListEnvironments()
	},
}

// showEnvironmentCmd represents the show command
var showEnvironmentCmd = &cobra.Command{
	Use:   "show [environment-name]",
	Short: "Show environment details",
	Long:  `Display detailed information about a specific environment.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return environments.ShowEnvironment(args[0])
	},
}

// addEnvironmentCmd represents the add command
var addEnvironmentCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new environment",
	Long:  `Add a new environment by providing environment details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return environments.AddEnvironment(args)
	},
}

// modifyEnvironmentCmd represents the modify command
var modifyEnvironmentCmd = &cobra.Command{
	Use:   "modify [environment-name]",
	Short: "Modify an existing environment",
	Long:  `Modify an existing environment by providing the environment name and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return environments.ModifyEnvironment(args[0], args[1:])
	},
}

// deleteEnvironmentCmd represents the delete command
var deleteEnvironmentCmd = &cobra.Command{
	Use:   "delete [environment-name]",
	Short: "Delete an environment",
	Long:  `Delete an environment by providing the environment name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return environments.DeleteEnvironment(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to environments command
	environmentsCmd.AddCommand(listEnvironmentsCmd)
	environmentsCmd.AddCommand(showEnvironmentCmd)
	environmentsCmd.AddCommand(addEnvironmentCmd)
	environmentsCmd.AddCommand(modifyEnvironmentCmd)
	environmentsCmd.AddCommand(deleteEnvironmentCmd)
}
