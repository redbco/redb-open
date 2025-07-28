package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/users"
	"github.com/spf13/cobra"
)

// usersCmd represents the users command
var usersCmd = &cobra.Command{
	Use:   "users",
	Short: "Manage users",
	Long:  `Commands for managing users including listing, showing details, adding, modifying, and deleting users.`,
}

// listUsersCmd represents the list command
var listUsersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all users",
	Long:  `Display a formatted list of all users with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return users.ListUsers()
	},
}

// showUserCmd represents the show command
var showUserCmd = &cobra.Command{
	Use:   "show [user-id]",
	Short: "Show user details",
	Long:  `Display detailed information about a specific user.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return users.ShowUser(args[0])
	},
}

// addUserCmd represents the add command
var addUserCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new user",
	Long:  `Add a new user by providing user details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return users.AddUser(args)
	},
}

// modifyUserCmd represents the modify command
var modifyUserCmd = &cobra.Command{
	Use:   "modify [user-id]",
	Short: "Modify an existing user",
	Long:  `Modify an existing user by providing the user ID and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return users.ModifyUser(args[0], args[1:])
	},
}

// deleteUserCmd represents the delete command
var deleteUserCmd = &cobra.Command{
	Use:   "delete [user-id]",
	Short: "Delete a user",
	Long:  `Delete a user by providing the user ID.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return users.DeleteUser(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to users command
	usersCmd.AddCommand(listUsersCmd)
	usersCmd.AddCommand(showUserCmd)
	usersCmd.AddCommand(addUserCmd)
	usersCmd.AddCommand(modifyUserCmd)
	usersCmd.AddCommand(deleteUserCmd)
}
