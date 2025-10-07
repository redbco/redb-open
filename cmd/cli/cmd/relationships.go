package main

import (
	"fmt"

	"github.com/redbco/redb-open/cmd/cli/internal/relationships"
	"github.com/spf13/cobra"
)

// relationshipsCmd represents the relationships command
var relationshipsCmd = &cobra.Command{
	Use:   "relationships",
	Short: "Manage relationships between databases",
	Long: `Manage relationships between databases using CDC (Change Data Capture).
Relationships automatically synchronize data between databases using CDC.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// addRelationshipCmd represents the add relationship command
var addRelationshipCmd = &cobra.Command{
	Use:   "add --mapping [mapping-name] --type [type]",
	Short: "Add a new relationship",
	Long: `Add a new relationship between databases using an existing mapping.
	
The relationship will use the mapping to determine source and target databases
and will automatically set up CDC (Change Data Capture) for real-time synchronization.

Relationship types:
  - default: One-way continuous synchronization
  - migration: One-time sync for migration purposes  
  - multi-master: Bi-directional synchronization (advanced)

Examples:
  # Add a relationship with default type
  redb relationships add --mapping user-mapping --type default
  
  # Add a relationship for migration
  redb relationships add --mapping data-migration --type migration`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mappingName, _ := cmd.Flags().GetString("mapping")
		relType, _ := cmd.Flags().GetString("type")

		if mappingName == "" {
			return fmt.Errorf("--mapping flag is required")
		}

		if relType == "" {
			relType = "default"
		}

		return relationships.AddRelationship(mappingName, relType)
	},
}

// startRelationshipCmd represents the start relationship command
var startRelationshipCmd = &cobra.Command{
	Use:   "start [relationship-name]",
	Short: "Start an existing relationship",
	Long: `Start an existing relationship to begin CDC synchronization.
	
This will:
1. Copy all existing data from source to target (initial sync)
2. Set up CDC to capture ongoing changes
3. Continuously replicate changes to the target

Examples:
  # Start a relationship
  redb relationships start user-sync
  
  # Start with custom batch size and workers
  redb relationships start user-sync --batch-size 2000 --parallel-workers 8`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		relationshipName := args[0]
		batchSize, _ := cmd.Flags().GetInt32("batch-size")
		parallelWorkers, _ := cmd.Flags().GetInt32("parallel-workers")

		return relationships.StartRelationship(relationshipName, batchSize, parallelWorkers)
	},
}

// stopRelationshipCmd represents the stop relationship command
var stopRelationshipCmd = &cobra.Command{
	Use:   "stop [relationship-name]",
	Short: "Stop a running relationship",
	Long: `Stop a running relationship temporarily without removing it.
	
The CDC connection will be paused, but the relationship configuration
will be preserved. You can resume it later with the 'start' or 'resume' command.

Examples:
  # Stop a relationship
  redb relationships stop user-sync`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		relationshipName := args[0]
		return relationships.StopRelationship(relationshipName)
	},
}

// resumeRelationshipCmd represents the resume relationship command
var resumeRelationshipCmd = &cobra.Command{
	Use:   "resume [relationship-name]",
	Short: "Resume a stopped relationship",
	Long: `Resume a stopped relationship to continue CDC synchronization.
	
The relationship will resume from where it left off, using the
preserved CDC position if available.

Examples:
  # Resume a relationship
  redb relationships resume user-sync
  
  # Resume and skip initial data sync
  redb relationships resume user-sync --skip-data-sync`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		relationshipName := args[0]
		skipDataSync, _ := cmd.Flags().GetBool("skip-data-sync")

		return relationships.ResumeRelationship(relationshipName, skipDataSync)
	},
}

// removeRelationshipCmd represents the remove relationship command
var removeRelationshipCmd = &cobra.Command{
	Use:   "remove [relationship-name]",
	Short: "Remove a relationship completely",
	Long: `Remove a relationship completely, stopping CDC and cleaning up all resources.
	
WARNING: This will stop the CDC connection and remove the relationship configuration.
The data in the target database will NOT be deleted.

Examples:
  # Remove a relationship
  redb relationships remove user-sync
  
  # Force remove even if cleanup fails
  redb relationships remove user-sync --force`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		relationshipName := args[0]
		force, _ := cmd.Flags().GetBool("force")

		return relationships.RemoveRelationship(relationshipName, force)
	},
}

// listRelationshipsCmd represents the list relationships command
var listRelationshipsCmd = &cobra.Command{
	Use:   "list",
	Short: "List all relationships",
	Long: `List all relationships in the current workspace.
	
Examples:
  # List all relationships
  redb relationships list`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return relationships.ListRelationships()
	},
}

// showRelationshipCmd represents the show relationship command
var showRelationshipCmd = &cobra.Command{
	Use:   "show [relationship-name]",
	Short: "Show details of a relationship",
	Long: `Show detailed information about a specific relationship.
	
Examples:
  # Show relationship details
  redb relationships show user-sync`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		relationshipName := args[0]
		return relationships.ShowRelationship(relationshipName)
	},
}

func init() {
	rootCmd.AddCommand(relationshipsCmd)

	// Add subcommands
	relationshipsCmd.AddCommand(addRelationshipCmd)
	relationshipsCmd.AddCommand(startRelationshipCmd)
	relationshipsCmd.AddCommand(stopRelationshipCmd)
	relationshipsCmd.AddCommand(resumeRelationshipCmd)
	relationshipsCmd.AddCommand(removeRelationshipCmd)
	relationshipsCmd.AddCommand(listRelationshipsCmd)
	relationshipsCmd.AddCommand(showRelationshipCmd)

	// Add flags to addRelationshipCmd
	addRelationshipCmd.Flags().String("mapping", "", "Mapping name to use for the relationship (required)")
	addRelationshipCmd.Flags().String("type", "replication", "Relationship type (currently only 'replication' is supported)")
	addRelationshipCmd.MarkFlagRequired("mapping")

	// Add flags to startRelationshipCmd
	startRelationshipCmd.Flags().Int32("batch-size", 1000, "Number of rows to process in each batch during initial sync")
	startRelationshipCmd.Flags().Int32("parallel-workers", 4, "Number of parallel workers for initial sync")

	// Add flags to resumeRelationshipCmd
	resumeRelationshipCmd.Flags().Bool("skip-data-sync", false, "Skip initial data sync on resume")

	// Add flags to removeRelationshipCmd
	removeRelationshipCmd.Flags().Bool("force", false, "Force removal even if cleanup fails")
}
