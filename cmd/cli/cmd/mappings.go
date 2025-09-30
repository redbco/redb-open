package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/mappings"
	"github.com/spf13/cobra"
)

// mappingsCmd represents the mappings command
var mappingsCmd = &cobra.Command{
	Use:   "mappings",
	Short: "Manage mappings",
	Long:  `Commands for managing mappings including listing, showing details, and adding table mappings.`,
}

// listMappingsCmd represents the list command
var listMappingsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all mappings",
	Long:  `Display a formatted list of all mappings with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mappings.ListMappings()
	},
}

// showMappingCmd represents the show command
var showMappingCmd = &cobra.Command{
	Use:   "show [mapping-name]",
	Short: "Show mapping details",
	Long:  `Display detailed information about a specific mapping.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mappings.ShowMapping(args[0])
	},
}

// addMappingCmd represents the add command
var addMappingCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new mapping",
	Long: `Add a new mapping with specified scope (database or table).

Examples:
  # Add table-to-table mapping with auto-generated name and description
  redb mappings add --scope table --source mydb.users --target targetdb.user_profiles
  
  # Add database-to-database mapping with custom name and description
  redb mappings add --scope database --source sourcedb --target targetdb --name db-migration --description "Migrate entire database"
  
  # Add table mapping with custom name only (description auto-generated)
  redb mappings add --scope table --source mydb.users --target targetdb.profiles --name user-profile-mapping`,
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, _ := cmd.Flags().GetString("scope")
		source, _ := cmd.Flags().GetString("source")
		target, _ := cmd.Flags().GetString("target")
		name, _ := cmd.Flags().GetString("name")
		description, _ := cmd.Flags().GetString("description")
		policyID, _ := cmd.Flags().GetString("policy-id")

		return mappings.AddMapping(scope, source, target, name, description, policyID)
	},
}

func init() {
	// Add flags to addMappingCmd
	addMappingCmd.Flags().String("scope", "", "Mapping scope: 'database' or 'table' (required)")
	addMappingCmd.Flags().String("source", "", "Source in format 'database_name[.table_name]' (required)")
	addMappingCmd.Flags().String("target", "", "Target in format 'database_name[.table_name]' (required)")
	addMappingCmd.Flags().String("name", "", "Mapping name (optional, auto-generated if not provided)")
	addMappingCmd.Flags().String("description", "", "Mapping description (optional, auto-generated if not provided)")
	addMappingCmd.Flags().String("policy-id", "", "Policy ID (optional)")

	// Mark required flags
	addMappingCmd.MarkFlagRequired("scope")
	addMappingCmd.MarkFlagRequired("source")
	addMappingCmd.MarkFlagRequired("target")

	// Add subcommands to mappings command
	mappingsCmd.AddCommand(listMappingsCmd)
	mappingsCmd.AddCommand(showMappingCmd)
	mappingsCmd.AddCommand(addMappingCmd)
}
