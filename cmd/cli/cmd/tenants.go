package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/tenants"
	"github.com/spf13/cobra"
)

// tenantsCmd represents the tenants command
var tenantsCmd = &cobra.Command{
	Use:   "tenants",
	Short: "Manage tenants",
	Long:  `Commands for managing tenants including listing, showing details, adding, modifying, and deleting tenants.`,
}

// listTenantsCmd represents the list command
var listTenantsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all tenants",
	Long:  `Display a formatted list of all tenants with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return tenants.ListTenants()
	},
}

// showTenantCmd represents the show command
var showTenantCmd = &cobra.Command{
	Use:   "show [tenant-id]",
	Short: "Show tenant details",
	Long:  `Display detailed information about a specific tenant.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return tenants.ShowTenant(args[0])
	},
}

// addTenantCmd represents the add command
var addTenantCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new tenant",
	Long:  `Add a new tenant by providing tenant details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return tenants.AddTenant(args)
	},
}

// modifyTenantCmd represents the modify command
var modifyTenantCmd = &cobra.Command{
	Use:   "modify [tenant-id]",
	Short: "Modify an existing tenant",
	Long:  `Modify an existing tenant by providing the tenant ID and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return tenants.ModifyTenant(args[0], args[1:])
	},
}

// deleteTenantCmd represents the delete command
var deleteTenantCmd = &cobra.Command{
	Use:   "delete [tenant-id]",
	Short: "Delete a tenant",
	Long:  `Delete a tenant by providing the tenant ID.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return tenants.DeleteTenant(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to tenants command
	tenantsCmd.AddCommand(listTenantsCmd)
	tenantsCmd.AddCommand(showTenantCmd)
	tenantsCmd.AddCommand(addTenantCmd)
	tenantsCmd.AddCommand(modifyTenantCmd)
	tenantsCmd.AddCommand(deleteTenantCmd)
}
