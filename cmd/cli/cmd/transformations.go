package main

import (
	"fmt"
	"os"

	"github.com/redbco/redb-open/cmd/cli/internal/transformations"
	"github.com/spf13/cobra"
)

// transformationsCmd represents the transformations command
var transformationsCmd = &cobra.Command{
	Use:   "transformations",
	Short: "Manage transformations",
	Long:  `List and manage data transformations available in the system.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// listTransformationsCmd represents the transformations list command
var listTransformationsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all available transformations",
	Long: `List all built-in transformations available in the system.

The transformations list includes:
  - Passthrough transformations (direct_mapping, uppercase, lowercase, etc.)
  - Generator transformations (uuid_generator, etc.)
  - Null-returning transformations (null_export, etc.)

Examples:
  # List all available transformations
  redb transformations list

  # List all transformations with type information
  redb transformations list --verbose`,
	Run: func(cmd *cobra.Command, args []string) {
		verbose, _ := cmd.Flags().GetBool("verbose")

		if err := transformations.ListTransformations(verbose); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(transformationsCmd)
	transformationsCmd.AddCommand(listTransformationsCmd)

	// Add flags to list command
	listTransformationsCmd.Flags().BoolP("verbose", "v", false, "Show detailed information for each transformation")
}
