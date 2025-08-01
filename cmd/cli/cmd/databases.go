package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/databases"
	"github.com/spf13/cobra"
)

// databasesCmd represents the databases command
var databasesCmd = &cobra.Command{
	Use:   "databases",
	Short: "Manage databases",
	Long: "Commands for managing databases including listing, showing details, creating, modifying, connecting, " +
		"reconnecting, disconnecting, wiping, dropping, and cloning table data.",
}

// listDatabasesCmd represents the list command
var listDatabasesCmd = &cobra.Command{
	Use:   "list",
	Short: "List all databases",
	Long:  `Display a formatted list of all databases with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.ListDatabases()
	},
}

// showDatabaseCmd represents the show command
var showDatabaseCmd = &cobra.Command{
	Use:   "show [database-name]",
	Short: "Show database details",
	Long:  `Display detailed information about a specific database.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Build args slice to pass to ShowDatabase function
		var flags []string

		// Check if --schema flag is set
		if cmd.Flags().Lookup("schema").Changed {
			flags = append(flags, "--schema")
		}

		// Check if --tables flag is set
		if cmd.Flags().Lookup("tables").Changed {
			flags = append(flags, "--tables")
		}

		return databases.ShowDatabase(args[0], flags)
	},
}

// createDatabaseCmd represents the create command
var createDatabaseCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new database",
	Long:  `Create a new database by providing database details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.CreateDatabase(args)
	},
}

// modifyDatabaseCmd represents the modify command
var modifyDatabaseCmd = &cobra.Command{
	Use:   "modify [database-name]",
	Short: "Modify an existing database",
	Long:  `Modify an existing database by providing the database name and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.ModifyDatabase(args[0], args[1:])
	},
}

// deleteDatabaseCmd represents the delete command
var deleteDatabaseCmd = &cobra.Command{
	Use:   "delete [database-name]",
	Short: "Delete a database",
	Long:  `Delete a database by providing the database name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.DeleteDatabase(args[0], args[1:])
	},
}

// connectDatabaseCmd represents the connect command
var connectDatabaseCmd = &cobra.Command{
	Use:   "connect [database-name]",
	Short: "Connect a database",
	Long:  `Connect a database by providing the database name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.ConnectDatabase(args[0], args[1:])
	},
}

// reconnectDatabaseCmd represents the reconnect command
var reconnectDatabaseCmd = &cobra.Command{
	Use:   "reconnect [database-name]",
	Short: "Reconnect a database",
	Long:  `Reconnect a database by providing the database name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.ReconnectDatabase(args[0], args[1:])
	},
}

// disconnectDatabaseCmd represents the disconnect command
var disconnectDatabaseCmd = &cobra.Command{
	Use:   "disconnect [database-name]",
	Short: "Disconnect a database",
	Long:  `Disconnect a database by providing the database name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.DisconnectDatabase(args[0], args[1:])
	},
}

// wipeDatabaseCmd represents the wipe command
var wipeDatabaseCmd = &cobra.Command{
	Use:   "wipe [database-name]",
	Short: "Wipe a database",
	Long:  `Wipe a database by providing the database name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.WipeDatabase(args[0], args[1:])
	},
}

// dropDatabaseCmd represents the drop command
var dropDatabaseCmd = &cobra.Command{
	Use:   "drop [database-name]",
	Short: "Drop a database",
	Long:  `Drop a database by providing the database name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.DropDatabase(args[0], args[1:])
	},
}

// cloneTableDataCmd represents the clone table-data command
var cloneTableDataCmd = &cobra.Command{
	Use:   "clone table-data [mapping-name]",
	Short: "Clone data from one table to another using a mapping",
	Long:  `Clone data from one table to another using a mapping by providing the mapping name.`,
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return databases.CloneTableData(args[1], args[2:])
	},
}

func init() {
	// Add flags to showDatabaseCmd
	showDatabaseCmd.Flags().Bool("schema", false, "Show database schema information")
	showDatabaseCmd.Flags().Bool("tables", false, "Show database tables information")

	// Add subcommands to databases command
	databasesCmd.AddCommand(listDatabasesCmd)
	databasesCmd.AddCommand(showDatabaseCmd)
	databasesCmd.AddCommand(createDatabaseCmd)
	databasesCmd.AddCommand(modifyDatabaseCmd)
	databasesCmd.AddCommand(deleteDatabaseCmd)
	databasesCmd.AddCommand(connectDatabaseCmd)
	databasesCmd.AddCommand(reconnectDatabaseCmd)
	databasesCmd.AddCommand(disconnectDatabaseCmd)
	databasesCmd.AddCommand(wipeDatabaseCmd)
	databasesCmd.AddCommand(dropDatabaseCmd)
	databasesCmd.AddCommand(cloneTableDataCmd)
}
