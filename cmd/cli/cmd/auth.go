package main

import (
	"errors"
	"github.com/redbco/redb-open/cmd/cli/internal/auth"
	"github.com/spf13/cobra"
)

// authCmd represents the auth command
var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Authentication commands",
	Long:  `Commands for managing authentication, sessions, and user profile.`,
}

// loginCmd represents the login command
var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to reDB",
	Long:  `Login to reDB by providing username, password, hostname, and optionally tenant.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.Login(args)
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// logoutCmd represents the logout command
var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Logout from reDB",
	Long:  `Logout from reDB and clear authentication tokens.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.Logout()
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// statusCmd represents the status command
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show authentication status",
	Long:  `Display the current authentication status and token information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.Status()
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// profileCmd represents the profile command
var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Show user profile",
	Long:  `Display the current user's profile information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.Profile()
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// passwordCmd represents the password command
var passwordCmd = &cobra.Command{
	Use:   "password",
	Short: "Change user password",
	Long:  `Change the current user's password by providing old and new passwords.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.ChangePassword(args)
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// sessionsCmd represents the sessions command
var sessionsCmd = &cobra.Command{
	Use:   "sessions",
	Short: "List all active sessions",
	Long:  `Display all active sessions for the current user.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.ListSessions()
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// logoutSessionCmd represents the logout-session command
var logoutSessionCmd = &cobra.Command{
	Use:   "logout-session [session-id]",
	Short: "Logout a specific session by ID",
	Long:  `Logout a specific session by providing the session ID.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.LogoutSession(args[0])
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// logoutAllCmd represents the logout-all command
var logoutAllCmd = &cobra.Command{
	Use:   "logout-all",
	Short: "Logout all sessions",
	Long:  `Logout all sessions for the current user.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		excludeCurrent, _ := cmd.Flags().GetBool("keep-current")
		err := auth.LogoutAllSessions(excludeCurrent)
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// updateSessionCmd represents the update-session command
var updateSessionCmd = &cobra.Command{
	Use:   "update-session [session-id] [new-name]",
	Short: "Update session name",
	Long:  `Update the name of a specific session by providing session ID and new name.`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.UpdateSessionName(args[0], args[1])
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// selectWorkspaceCmd represents the select workspace command
var selectWorkspaceCmd = &cobra.Command{
	Use:   "select workspace [workspace-name]",
	Short: "Select active workspace",
	Long:  `Select an active workspace by providing the workspace name.`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] == "workspace" {
			err := auth.SelectWorkspace(args[1])
			// Check if it's an AuthError and suppress usage help
			var authError auth.AuthError
			if errors.As(err, &authError) {
				cmd.SilenceUsage = true
			}
			return err
		}
		return nil
	},
}

// changePasswordCmd represents the change password command (legacy command)
var changePasswordCmd = &cobra.Command{
	Use:   "change password",
	Short: "Change user password",
	Long:  `Change the current user's password by providing old and new passwords.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		err := auth.ChangePassword(args)
		// Check if it's an AuthError and suppress usage help
		var authError auth.AuthError
		if errors.As(err, &authError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

func init() {
	// Add flags to logout-all command
	logoutAllCmd.Flags().Bool("keep-current", false, "Keep the current session active")

	// Add subcommands to auth command
	authCmd.AddCommand(loginCmd)
	authCmd.AddCommand(logoutCmd)
	authCmd.AddCommand(statusCmd)
	authCmd.AddCommand(profileCmd)
	authCmd.AddCommand(passwordCmd)
	authCmd.AddCommand(sessionsCmd)
	authCmd.AddCommand(logoutSessionCmd)
	authCmd.AddCommand(logoutAllCmd)
	authCmd.AddCommand(updateSessionCmd)

	// Add select workspace command to root (since it's not under auth)
	rootCmd.AddCommand(selectWorkspaceCmd)

	// Add legacy change password command to root
	rootCmd.AddCommand(changePasswordCmd)
}
