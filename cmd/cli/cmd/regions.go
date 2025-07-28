package main

import (
	"github.com/redbco/redb-open/cmd/cli/internal/regions"
	"github.com/spf13/cobra"
)

// regionsCmd represents the regions command
var regionsCmd = &cobra.Command{
	Use:   "regions",
	Short: "Manage regions",
	Long:  `Commands for managing regions including listing, showing details, adding, modifying, and deleting regions.`,
}

// listRegionsCmd represents the list command
var listRegionsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all regions",
	Long:  `Display a formatted list of all regions with their basic information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return regions.ListRegions()
	},
}

// showRegionCmd represents the show command
var showRegionCmd = &cobra.Command{
	Use:   "show [region-name]",
	Short: "Show region details",
	Long:  `Display detailed information about a specific region.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return regions.ShowRegion(args[0])
	},
}

// addRegionCmd represents the add command
var addRegionCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a new region",
	Long:  `Add a new region by providing region details interactively.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return regions.AddRegion(args)
	},
}

// modifyRegionCmd represents the modify command
var modifyRegionCmd = &cobra.Command{
	Use:   "modify [region-name]",
	Short: "Modify an existing region",
	Long:  `Modify an existing region by providing the region name and new details interactively.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return regions.ModifyRegion(args[0], args[1:])
	},
}

// deleteRegionCmd represents the delete command
var deleteRegionCmd = &cobra.Command{
	Use:   "delete [region-name]",
	Short: "Delete a region",
	Long:  `Delete a region by providing the region name.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return regions.DeleteRegion(args[0], args[1:])
	},
}

func init() {
	// Add subcommands to regions command
	regionsCmd.AddCommand(listRegionsCmd)
	regionsCmd.AddCommand(showRegionCmd)
	regionsCmd.AddCommand(addRegionCmd)
	regionsCmd.AddCommand(modifyRegionCmd)
	regionsCmd.AddCommand(deleteRegionCmd)
}
