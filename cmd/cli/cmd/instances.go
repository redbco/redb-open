package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/instances"
	"github.com/spf13/cobra"
)

// instancesCmd represents the instances command
var instancesCmd = &cobra.Command{
	Use:   "instances",
	Short: "Manage instances",
	Long: "Commands for managing instances including listing, showing details, connecting, modifying, reconnecting, " +
		"and disconnecting instances.",
}

// listInstancesCmd represents the list command
var listInstancesCmd = &cobra.Command{
	Use:   "list",
	Short: "List all instances",
	Long:  `Display a formatted list of all instances with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return instances.ListInstances()
	},
}

// showInstanceCmd represents the show command
var showInstanceCmd = &cobra.Command{
	Use:   "show [instance-name]",
	Short: "Show instance details",
	Long:  `Display detailed information about a specific instance.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return instances.ShowInstance(args[0])
	},
}

// connectInstanceCmd represents the connect command
var connectInstanceCmd = &cobra.Command{
	Use:   "connect",
	Short: "Connect a new instance",
	Long: `Connect a new instance by providing instance details interactively or using a connection string.

Examples:
  # Interactive mode
  redb instances connect

  # Using connection string
  redb instances connect --string "postgresql://user:pass@localhost:5432/postgres" --name "my-postgres"
  redb instances connect --string "mysql://root:password@localhost:3306/mysql" --name "my-mysql"
  redb instances connect --string "mongodb://user:pass@localhost:27017/admin" --name "my-mongo"`,
	RunE: func(cmd *cobra.Command, args []string) error {
		connectionString, _ := cmd.Flags().GetString("string")
		instanceName, _ := cmd.Flags().GetString("name")
		description, _ := cmd.Flags().GetString("description")
		nodeID, _ := cmd.Flags().GetString("node-id")
		environmentID, _ := cmd.Flags().GetString("environment-id")
		enabled, _ := cmd.Flags().GetBool("enabled")

		if connectionString != "" {
			return instances.ConnectInstanceString(connectionString, instanceName, description, nodeID, environmentID, enabled)
		}
		return instances.ConnectInstance(args)
	},
}

// modifyInstanceCmd represents the modify command
var modifyInstanceCmd = &cobra.Command{
	Use:   "modify [instance-name]",
	Short: "Modify an existing instance",
	Long:  `Modify an existing instance by providing the instance name and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return instances.ModifyInstance(args[0], args[1:])
	},
}

// reconnectInstanceCmd represents the reconnect command
var reconnectInstanceCmd = &cobra.Command{
	Use:   "reconnect [instance-name]",
	Short: "Reconnect an instance",
	Long:  `Reconnect an instance by providing the instance name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return instances.ReconnectInstance(args[0], args[1:])
	},
}

// disconnectInstanceCmd represents the disconnect command
var disconnectInstanceCmd = &cobra.Command{
	Use:   "disconnect [instance-name]",
	Short: "Disconnect an instance",
	Long:  `Disconnect an instance by providing the instance name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return instances.DisconnectInstance(args[0], args[1:])
	},
}

func init() {
	// Add flags to connectInstanceCmd
	connectInstanceCmd.Flags().String("string", "", "Connection string (e.g., postgresql://user:pass@host:port/db)")
	connectInstanceCmd.Flags().String("name", "", "Instance name (required when using --string)")
	connectInstanceCmd.Flags().String("description", "", "Instance description")
	connectInstanceCmd.Flags().String("node-id", "", "Node ID")
	connectInstanceCmd.Flags().String("environment-id", "", "Environment ID")
	connectInstanceCmd.Flags().Bool("enabled", true, "Enable the instance")

	// Add subcommands to instances command
	instancesCmd.AddCommand(listInstancesCmd)
	instancesCmd.AddCommand(showInstanceCmd)
	instancesCmd.AddCommand(connectInstanceCmd)
	instancesCmd.AddCommand(modifyInstanceCmd)
	instancesCmd.AddCommand(reconnectInstanceCmd)
	instancesCmd.AddCommand(disconnectInstanceCmd)
}
