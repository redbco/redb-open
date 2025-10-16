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
  redb mappings add --scope table --source mydb.users --target targetdb.profiles --name user-profile-mapping
  
  # Add table-to-MCP resource mapping
  redb mappings add --scope table --source mydb.users --target mcp://users_resource
  
  # Add database-to-MCP resource mapping
  redb mappings add --scope database --source mydb --target mcp://db_resource`,
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

// copyDataCmd represents the copy-data command
var copyDataCmd = &cobra.Command{
	Use:   "copy-data [mapping-name]",
	Short: "Copy data using a mapping",
	Long: `Copy data from source to target databases/tables as defined in the mapping.
This command will stream data from source to target, applying any transformations
defined in the mapping rules.

Examples:
  # Copy data with default settings
  redb mappings copy-data user-mapping
  
  # Copy data with custom batch size and parallel workers
  redb mappings copy-data user-mapping --batch-size 2000 --parallel-workers 8
  
  # Perform a dry run to validate the mapping without copying data
  redb mappings copy-data user-mapping --dry-run
  
  # Copy data with progress updates
  redb mappings copy-data user-mapping --progress`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		mappingName := args[0]
		batchSize, _ := cmd.Flags().GetInt32("batch-size")
		parallelWorkers, _ := cmd.Flags().GetInt32("parallel-workers")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		progress, _ := cmd.Flags().GetBool("progress")

		return mappings.CopyMappingData(mappingName, batchSize, parallelWorkers, dryRun, progress)
	},
}

// modifyRuleCmd represents the modify-rule command
var modifyRuleCmd = &cobra.Command{
	Use:   "modify-rule",
	Short: "Modify an existing mapping rule",
	Long: `Modify source column, target column, transformation, or order of a mapping rule.

Examples:
  # Modify source column
  redb mappings modify-rule --mapping user-mapping --rule user_id_rule --source sourcedb.users.user_id
  
  # Modify target column
  redb mappings modify-rule --mapping user-mapping --rule user_id_rule --target targetdb.profiles.profile_id
  
  # Modify transformation
  redb mappings modify-rule --mapping user-mapping --rule name_rule --transformation uppercase
  
  # Modify order
  redb mappings modify-rule --mapping user-mapping --rule name_rule --order 5
  
  # Modify multiple properties
  redb mappings modify-rule --mapping user-mapping --rule email_rule --source sourcedb.users.email --target targetdb.profiles.email_address --transformation lowercase`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mappingName, _ := cmd.Flags().GetString("mapping")
		ruleName, _ := cmd.Flags().GetString("rule")
		source, _ := cmd.Flags().GetString("source")
		target, _ := cmd.Flags().GetString("target")
		transformation, _ := cmd.Flags().GetString("transformation")
		order, _ := cmd.Flags().GetInt32("order")

		return mappings.ModifyMappingRule(mappingName, ruleName, source, target, transformation, order)
	},
}

// addRuleCmd represents the add-rule command
var addRuleCmd = &cobra.Command{
	Use:   "add-rule",
	Short: "Add a new mapping rule to a mapping",
	Long: `Create a new mapping rule and attach it to a mapping.

Examples:
  # Add a rule with direct mapping
  redb mappings add-rule --mapping user-mapping --rule user_id_rule --source sourcedb.users.user_id --target targetdb.profiles.profile_id --transformation direct_mapping
  
  # Add a rule with transformation
  redb mappings add-rule --mapping user-mapping --rule name_rule --source sourcedb.users.name --target targetdb.profiles.full_name --transformation uppercase
  
  # Add a rule with specific order
  redb mappings add-rule --mapping user-mapping --rule email_rule --source sourcedb.users.email --target targetdb.profiles.email --transformation lowercase --order 2`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mappingName, _ := cmd.Flags().GetString("mapping")
		ruleName, _ := cmd.Flags().GetString("rule")
		source, _ := cmd.Flags().GetString("source")
		target, _ := cmd.Flags().GetString("target")
		transformation, _ := cmd.Flags().GetString("transformation")
		order, _ := cmd.Flags().GetInt32("order")

		return mappings.AddMappingRule(mappingName, ruleName, source, target, transformation, order)
	},
}

// removeRuleCmd represents the remove-rule command
var removeRuleCmd = &cobra.Command{
	Use:   "remove-rule",
	Short: "Remove a mapping rule from a mapping",
	Long: `Detach and optionally delete a mapping rule from a mapping.

Examples:
  # Remove a rule from a mapping (detach only)
  redb mappings remove-rule --mapping user-mapping --rule email_rule
  
  # Remove and delete a rule
  redb mappings remove-rule --mapping user-mapping --rule email_rule --delete`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mappingName, _ := cmd.Flags().GetString("mapping")
		ruleName, _ := cmd.Flags().GetString("rule")
		deleteRule, _ := cmd.Flags().GetBool("delete")

		return mappings.RemoveMappingRule(mappingName, ruleName, deleteRule)
	},
}

// listRulesCmd represents the list-rules command
var listRulesCmd = &cobra.Command{
	Use:   "list-rules",
	Short: "List all mapping rules in a mapping",
	Long: `Display a formatted list of all mapping rules attached to a specific mapping.

Examples:
  # List all rules in a mapping
  redb mappings list-rules --mapping user-mapping`,
	RunE: func(cmd *cobra.Command, args []string) error {
		mappingName, _ := cmd.Flags().GetString("mapping")

		return mappings.ListMappingRules(mappingName)
	},
}

// validateMappingCmd represents the validate command
var validateMappingCmd = &cobra.Command{
	Use:   "validate [mapping-name]",
	Short: "Validate a mapping",
	Long: `Validate that a mapping is correctly configured with all required target columns mapped,
valid transformations, and compatible data types.

Examples:
  # Validate a mapping
  redb mappings validate user-profile-mapping
  
  # The validation checks:
  # - All non-nullable target columns have mapping rules
  # - Transformations are valid with correct inputs/outputs
  # - Data types are compatible between source and target`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return mappings.ValidateMapping(args[0])
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

	// Add flags to copyDataCmd
	copyDataCmd.Flags().Int32("batch-size", 1000, "Number of rows to process in each batch")
	copyDataCmd.Flags().Int32("parallel-workers", 4, "Number of parallel workers for data copying")
	copyDataCmd.Flags().Bool("dry-run", false, "Validate mapping and show what would be copied without actually copying data")
	copyDataCmd.Flags().Bool("progress", false, "Show detailed progress information during copying")

	// Add flags to modifyRuleCmd
	modifyRuleCmd.Flags().String("mapping", "", "Mapping name (required)")
	modifyRuleCmd.Flags().String("rule", "", "Rule name (required)")
	modifyRuleCmd.Flags().String("source", "", "Source column in format 'database.table.column'")
	modifyRuleCmd.Flags().String("target", "", "Target column in format 'database.table.column'")
	modifyRuleCmd.Flags().String("transformation", "", "Transformation name")
	modifyRuleCmd.Flags().Int32("order", -1, "Rule order (position in mapping)")
	modifyRuleCmd.MarkFlagRequired("mapping")
	modifyRuleCmd.MarkFlagRequired("rule")

	// Add flags to addRuleCmd
	addRuleCmd.Flags().String("mapping", "", "Mapping name (required)")
	addRuleCmd.Flags().String("rule", "", "Rule name (required)")
	addRuleCmd.Flags().String("source", "", "Source column in format 'database.table.column' (required)")
	addRuleCmd.Flags().String("target", "", "Target column in format 'database.table.column' (required)")
	addRuleCmd.Flags().String("transformation", "direct_mapping", "Transformation name (default: direct_mapping)")
	addRuleCmd.Flags().Int32("order", -1, "Rule order (position in mapping, auto-assigned if not specified)")
	addRuleCmd.MarkFlagRequired("mapping")
	addRuleCmd.MarkFlagRequired("rule")
	addRuleCmd.MarkFlagRequired("source")
	addRuleCmd.MarkFlagRequired("target")

	// Add flags to removeRuleCmd
	removeRuleCmd.Flags().String("mapping", "", "Mapping name (required)")
	removeRuleCmd.Flags().String("rule", "", "Rule name (required)")
	removeRuleCmd.Flags().Bool("delete", false, "Delete the rule after detaching (default: false)")
	removeRuleCmd.MarkFlagRequired("mapping")
	removeRuleCmd.MarkFlagRequired("rule")

	// Add flags to listRulesCmd
	listRulesCmd.Flags().String("mapping", "", "Mapping name (required)")
	listRulesCmd.MarkFlagRequired("mapping")

	// Add subcommands to mappings command
	mappingsCmd.AddCommand(listMappingsCmd)
	mappingsCmd.AddCommand(showMappingCmd)
	mappingsCmd.AddCommand(addMappingCmd)
	mappingsCmd.AddCommand(copyDataCmd)
	mappingsCmd.AddCommand(validateMappingCmd)
	mappingsCmd.AddCommand(modifyRuleCmd)
	mappingsCmd.AddCommand(addRuleCmd)
	mappingsCmd.AddCommand(removeRuleCmd)
	mappingsCmd.AddCommand(listRulesCmd)
}
