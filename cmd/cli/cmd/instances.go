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
	Long:  `Connect a new instance by providing instance details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
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
	// Add subcommands to instances command
	instancesCmd.AddCommand(listInstancesCmd)
	instancesCmd.AddCommand(showInstanceCmd)
	instancesCmd.AddCommand(connectInstanceCmd)
	instancesCmd.AddCommand(modifyInstanceCmd)
	instancesCmd.AddCommand(reconnectInstanceCmd)
	instancesCmd.AddCommand(disconnectInstanceCmd)
}
