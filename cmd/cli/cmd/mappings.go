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

// addTableMappingCmd represents the add table-mapping command
var addTableMappingCmd = &cobra.Command{
	Use:   "add table-mapping",
	Short: "Add a new table mapping",
	Long:  `Add a new table mapping by providing mapping details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mappings.AddTableMapping(args)
	},
}

func init() {
	// Add subcommands to mappings command
	mappingsCmd.AddCommand(listMappingsCmd)
	mappingsCmd.AddCommand(showMappingCmd)
	mappingsCmd.AddCommand(addTableMappingCmd)
}
