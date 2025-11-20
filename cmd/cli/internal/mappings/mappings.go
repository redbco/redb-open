package mappings

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

type MappingRuleMetadata struct {
	GeneratedAt    string  `json:"generated_at"`
	MatchScore     float64 `json:"match_score"`
	MatchType      string  `json:"match_type"`
	SourceColumn   string  `json:"source_column"`
	SourceTable    string  `json:"source_table"`
	TargetColumn   string  `json:"target_column"`
	TargetTable    string  `json:"target_table"`
	TypeCompatible bool    `json:"type_compatible"`
}

type MappingRule struct {
	MappingRuleID                    string              `json:"mapping_rule_id"`
	MappingRuleName                  string              `json:"mapping_rule_name"`
	MappingRuleDescription           string              `json:"mapping_rule_description"`
	MappingRuleMetadata              MappingRuleMetadata `json:"mapping_rule_metadata"`
	MappingRuleSource                string              `json:"mapping_rule_source"`
	MappingRuleTarget                string              `json:"mapping_rule_target"`
	MappingRuleTransformationID      string              `json:"mapping_rule_transformation_id"`
	MappingRuleTransformationName    string              `json:"mapping_rule_transformation_name"`
	MappingRuleTransformationOptions string              `json:"mapping_rule_transformation_options"`
}

type Mapping struct {
	TenantID           string        `json:"tenant_id"`
	WorkspaceID        string        `json:"workspace_id"`
	MappingID          string        `json:"mapping_id"`
	MappingName        string        `json:"mapping_name"`
	MappingDescription string        `json:"mapping_description"`
	MappingType        string        `json:"mapping_type"`
	PolicyID           string        `json:"policy_id"`
	OwnerID            string        `json:"owner_id"`
	MappingRuleCount   int32         `json:"mapping_rule_count"`
	MappingRules       []MappingRule `json:"mapping_rules"`
	Validated          bool          `json:"validated"`
	ValidatedAt        string        `json:"validated_at"`
	ValidationErrors   []string      `json:"validation_errors"`
	ValidationWarnings []string      `json:"validation_warnings"`
}

// AddMapping creates a new mapping with specified scope
func AddMapping(scope, source, target, name, description, policyID string, clean bool) error {
	// Validate scope
	if scope != "database" && scope != "table" {
		return fmt.Errorf("invalid scope '%s': must be 'database' or 'table'", scope)
	}

	// Check if target is MCP resource
	isMCPTarget := strings.HasPrefix(target, "mcp://")
	var targetDB, targetTable string
	var err error

	// Parse source
	sourceDB, sourceTable, err := ParseSourceTarget(source)
	if err != nil {
		return fmt.Errorf("invalid source format: %v", err)
	}

	if isMCPTarget {
		// Extract MCP resource name from mcp://resource_name
		targetDB = strings.TrimPrefix(target, "mcp://")
		if targetDB == "" {
			return fmt.Errorf("invalid MCP target format: expected 'mcp://resource_name'")
		}
		targetTable = "" // MCP targets don't have table names
	} else {
		// Parse database target
		targetDB, targetTable, err = ParseSourceTarget(target)
		if err != nil {
			return fmt.Errorf("invalid target format: %v", err)
		}

		// Validate scope-specific requirements for database-to-database mappings
		if scope == "table" {
			if sourceTable == "" || targetTable == "" {
				return fmt.Errorf("table scope requires both source and target to include table names (format: database.table)")
			}
		}
	}

	// Generate name and description if not provided
	if name == "" {
		if isMCPTarget {
			name = generateMCPMappingName(scope, sourceDB, sourceTable, targetDB)
		} else {
			name = generateMappingName(scope, sourceDB, sourceTable, targetDB, targetTable)
		}
	}

	if description == "" {
		if isMCPTarget {
			description = generateMCPMappingDescription(scope, sourceDB, sourceTable, targetDB)
		} else {
			description = generateMappingDescription(scope, sourceDB, sourceTable, targetDB, targetTable)
		}
	}

	// Create the mapping request
	mappingReq := struct {
		MappingName        string `json:"mapping_name"`
		MappingDescription string `json:"mapping_description"`
		Scope              string `json:"scope"`
		Source             string `json:"source"`
		Target             string `json:"target"`
		PolicyID           string `json:"policy_id,omitempty"`
		GenerateRules      bool   `json:"generate_rules"`
	}{
		MappingName:        name,
		MappingDescription: description,
		Scope:              scope,
		Source:             source,
		Target:             target,
		PolicyID:           policyID,
		GenerateRules:      !clean, // If clean is true, don't generate rules
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mappings")
	if err != nil {
		return err
	}

	var response struct {
		Message string  `json:"message"`
		Success bool    `json:"success"`
		Mapping Mapping `json:"mapping"`
		Status  string  `json:"status"`
	}
	if err := client.Post(url, mappingReq, &response); err != nil {
		return fmt.Errorf("failed to create mapping: %v", err)
	}

	fmt.Printf("Successfully created %s mapping '%s' (ID: %s)\n", scope, response.Mapping.MappingName, response.Mapping.MappingID)
	return nil
}

// ParseSourceTarget parses database[.table] format (exported for use in cmd package)
func ParseSourceTarget(input string) (database, table string, err error) {
	if input == "" {
		return "", "", fmt.Errorf("source/target cannot be empty")
	}

	parts := strings.Split(input, ".")
	if len(parts) == 1 {
		// Only database name
		return parts[0], "", nil
	} else if len(parts) == 2 {
		// Database and table name
		return parts[0], parts[1], nil
	} else {
		return "", "", fmt.Errorf("invalid format '%s': expected 'database' or 'database.table'", input)
	}
}

// AddTableMapping creates a new table mapping (legacy function for backward compatibility)
func AddTableMapping(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get mapping name
	var mappingName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		mappingName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Mapping Name: ")
		mappingName, _ = reader.ReadString('\n')
		mappingName = strings.TrimSpace(mappingName)
	}

	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	// Get mapping description
	var mappingDescription string
	if len(args) > 1 && strings.HasPrefix(args[1], "--description=") {
		mappingDescription = strings.TrimPrefix(args[1], "--description=")
	} else {
		fmt.Print("Mapping Description: ")
		mappingDescription, _ = reader.ReadString('\n')
		mappingDescription = strings.TrimSpace(mappingDescription)
	}

	if mappingDescription == "" {
		return fmt.Errorf("mapping description is required")
	}

	// Get source database name
	var sourceDatabaseName string
	if len(args) > 2 && strings.HasPrefix(args[2], "--source-database=") {
		sourceDatabaseName = strings.TrimPrefix(args[2], "--source-database=")
	} else {
		fmt.Print("Source Database Name: ")
		sourceDatabaseName, _ = reader.ReadString('\n')
		sourceDatabaseName = strings.TrimSpace(sourceDatabaseName)
	}

	if sourceDatabaseName == "" {
		return fmt.Errorf("source database name is required")
	}

	// Get source table name
	var sourceTableName string
	if len(args) > 3 && strings.HasPrefix(args[3], "--source-table=") {
		sourceTableName = strings.TrimPrefix(args[3], "--source-table=")
	} else {
		fmt.Print("Source Table Name: ")
		sourceTableName, _ = reader.ReadString('\n')
		sourceTableName = strings.TrimSpace(sourceTableName)
	}

	if sourceTableName == "" {
		return fmt.Errorf("source table name is required")
	}

	// Get target database name
	var targetDatabaseName string
	if len(args) > 4 && strings.HasPrefix(args[4], "--target-database=") {
		targetDatabaseName = strings.TrimPrefix(args[4], "--target-database=")
	} else {
		fmt.Print("Target Database Name: ")
		targetDatabaseName, _ = reader.ReadString('\n')
		targetDatabaseName = strings.TrimSpace(targetDatabaseName)
	}

	if targetDatabaseName == "" {
		return fmt.Errorf("target database name is required")
	}

	// Get target table name
	var targetTableName string
	if len(args) > 5 && strings.HasPrefix(args[5], "--target-table=") {
		targetTableName = strings.TrimPrefix(args[5], "--target-table=")
	} else {
		fmt.Print("Target Table Name: ")
		targetTableName, _ = reader.ReadString('\n')
		targetTableName = strings.TrimSpace(targetTableName)
	}

	if targetTableName == "" {
		return fmt.Errorf("target table name is required")
	}

	// Get policy ID (optional)
	var policyID string
	if len(args) > 6 && strings.HasPrefix(args[6], "--policy-id=") {
		policyID = strings.TrimPrefix(args[6], "--policy-id=")
	}

	// Create the table mapping request
	tableMappingReq := struct {
		MappingName               string `json:"mapping_name"`
		MappingDescription        string `json:"mapping_description"`
		MappingSourceDatabaseName string `json:"mapping_source_database_name"`
		MappingSourceTableName    string `json:"mapping_source_table_name"`
		MappingTargetDatabaseName string `json:"mapping_target_database_name"`
		MappingTargetTableName    string `json:"mapping_target_table_name"`
		PolicyID                  string `json:"policy_id,omitempty"`
	}{
		MappingName:               mappingName,
		MappingDescription:        mappingDescription,
		MappingSourceDatabaseName: sourceDatabaseName,
		MappingSourceTableName:    sourceTableName,
		MappingTargetDatabaseName: targetDatabaseName,
		MappingTargetTableName:    targetTableName,
		PolicyID:                  policyID,
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mappings/table")
	if err != nil {
		return err
	}

	var response struct {
		Message string  `json:"message"`
		Success bool    `json:"success"`
		Mapping Mapping `json:"mapping"`
		Status  string  `json:"status"`
	}
	if err := client.Post(url, tableMappingReq, &response); err != nil {
		return fmt.Errorf("failed to create table mapping: %v", err)
	}

	fmt.Printf("Successfully created table mapping '%s' (ID: %s)\n", response.Mapping.MappingName, response.Mapping.MappingID)
	return nil
}

// ListMappings lists all mappings using profile-based authentication
func ListMappings() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mappings")
	if err != nil {
		return err
	}

	var mappingsResponse struct {
		Mappings []Mapping `json:"mappings"`
	}
	if err := client.Get(url, &mappingsResponse); err != nil {
		return fmt.Errorf("failed to list mappings: %v", err)
	}

	if len(mappingsResponse.Mappings) == 0 {
		fmt.Println("No mappings found.")
		return nil
	}

	fmt.Println()
	fmt.Printf("%-40s %-40s %-15s %-10s %-12s\n", "Name", "Description", "Type", "Rules", "Validated")
	fmt.Println(strings.Repeat("-", 122))
	for _, mapping := range mappingsResponse.Mappings {
		description := mapping.MappingDescription
		if len(description) > 38 {
			description = description[:35] + "..."
		}

		// Determine validation status symbol
		validationStatus := "-"
		if mapping.ValidatedAt != "" {
			if !mapping.Validated || len(mapping.ValidationErrors) > 0 {
				validationStatus = "✗"
			} else if len(mapping.ValidationWarnings) > 0 {
				validationStatus = "⚠"
			} else {
				validationStatus = "✓"
			}
		}

		fmt.Printf("%-40s %-40s %-15s %-10d %-12s\n",
			mapping.MappingName,
			description,
			mapping.MappingType,
			mapping.MappingRuleCount,
			validationStatus)
	}
	fmt.Println()
	return nil
}

// ShowMapping displays details of a specific mapping
func ShowMapping(mappingName string) error {
	mappingName = strings.TrimSpace(mappingName)
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s", mappingName))
	if err != nil {
		return err
	}

	var mappingResponse struct {
		Mapping Mapping `json:"mapping"`
	}
	if err := client.Get(url, &mappingResponse); err != nil {
		return fmt.Errorf("failed to get mapping details: %v", err)
	}

	mapping := mappingResponse.Mapping
	fmt.Println()
	fmt.Printf("Mapping Details for '%s'\n", mapping.MappingName)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("ID:          %s\n", mapping.MappingID)
	fmt.Printf("Name:        %s\n", mapping.MappingName)
	fmt.Printf("Description: %s\n", mapping.MappingDescription)
	fmt.Printf("Type:        %s\n", mapping.MappingType)
	fmt.Printf("Owner ID:    %s\n", mapping.OwnerID)
	fmt.Printf("Tenant ID:   %s\n", mapping.TenantID)
	fmt.Printf("Workspace ID: %s\n", mapping.WorkspaceID)
	if mapping.PolicyID != "" {
		fmt.Printf("Policy ID:   %s\n", mapping.PolicyID)
	}

	// Display validation information
	if mapping.ValidatedAt != "" {
		fmt.Println()
		fmt.Println("Validation Status:")
		fmt.Println(strings.Repeat("-", 50))
		if mapping.Validated && len(mapping.ValidationErrors) == 0 {
			if len(mapping.ValidationWarnings) > 0 {
				fmt.Printf("Status:      ⚠ Valid (with %d warning(s))\n", len(mapping.ValidationWarnings))
			} else {
				fmt.Println("Status:      ✓ Valid")
			}
		} else {
			fmt.Printf("Status:      ✗ Invalid (with %d error(s))\n", len(mapping.ValidationErrors))
		}
		fmt.Printf("Validated:   %s\n", mapping.ValidatedAt)

		if len(mapping.ValidationErrors) > 0 {
			fmt.Println("\nValidation Errors:")
			for i, err := range mapping.ValidationErrors {
				fmt.Printf("  %d. %s\n", i+1, err)
			}
		}

		if len(mapping.ValidationWarnings) > 0 {
			fmt.Println("\nValidation Warnings:")
			for i, warn := range mapping.ValidationWarnings {
				fmt.Printf("  %d. %s\n", i+1, warn)
			}
		}
	} else {
		fmt.Println()
		fmt.Println("Validation Status:")
		fmt.Println(strings.Repeat("-", 50))
		fmt.Println("Status:      Not validated yet")
		fmt.Println("Run 'redb mappings validate <mapping-name>' to validate this mapping")
	}
	fmt.Println()

	// Display mapping rules table
	if len(mapping.MappingRules) > 0 {
		fmt.Println("Mapping Rules:")
		fmt.Println(strings.Repeat("-", 150))
		fmt.Printf("%-55s %-30s %-30s %-20s %-10s\n", "Rule Name", "Source Column", "Target Column", "Transformation", "Match")
		fmt.Println(strings.Repeat("-", 150))

		for _, rule := range mapping.MappingRules {
			// Truncate rule name if too long
			ruleName := rule.MappingRuleName
			if len(ruleName) > 54 {
				ruleName = ruleName[:51] + "..."
			}

			// Truncate source column if too long
			sourceCol := rule.MappingRuleMetadata.SourceColumn
			if len(sourceCol) > 29 {
				sourceCol = sourceCol[:26] + "..."
			}

			// Truncate target column if too long
			targetCol := rule.MappingRuleMetadata.TargetColumn
			if len(targetCol) > 29 {
				targetCol = targetCol[:26] + "..."
			}

			// Truncate transformation name if too long
			transformName := rule.MappingRuleTransformationName
			if len(transformName) > 24 {
				transformName = transformName[:21] + "..."
			}

			// Format match indicator based on rule type
			var matchIndicator string
			if rule.MappingRuleMetadata.MatchType == "auto_generated" {
				// Show percentage for auto-generated rules
				matchIndicator = fmt.Sprintf("%.1f%%", rule.MappingRuleMetadata.MatchScore*100)
			} else if rule.MappingRuleMetadata.MatchType == "user_defined" {
				// Show indicator for user-defined rules
				matchIndicator = "User"
			} else {
				// Default to showing score if match_type is not set (backward compatibility)
				matchIndicator = fmt.Sprintf("%.1f%%", rule.MappingRuleMetadata.MatchScore*100)
			}

			fmt.Printf("%-55s %-30s %-30s %-20s %-10s\n",
				ruleName,
				sourceCol,
				targetCol,
				transformName,
				matchIndicator)
		}
		fmt.Println()
	} else {
		fmt.Println("No mapping rules found.")
		fmt.Println()
	}

	return nil
}

// generateMappingName creates a mapping name from source and target information
func generateMappingName(scope, sourceDB, sourceTable, targetDB, targetTable string) string {
	switch scope {
	case "database":
		return fmt.Sprintf("%s_to_%s", sourceDB, targetDB)
	case "table":
		return fmt.Sprintf("%s_%s_to_%s_%s", sourceDB, sourceTable, targetDB, targetTable)
	default:
		return fmt.Sprintf("%s_to_%s", sourceDB, targetDB)
	}
}

// generateMappingDescription creates a verbose description with timestamp
func generateMappingDescription(scope, sourceDB, sourceTable, targetDB, targetTable string) string {
	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05 UTC")

	switch scope {
	case "database":
		return fmt.Sprintf("Auto-generated database mapping from '%s' to '%s' created on %s",
			sourceDB, targetDB, timestamp)
	case "table":
		return fmt.Sprintf("Auto-generated table mapping from '%s.%s' to '%s.%s' created on %s",
			sourceDB, sourceTable, targetDB, targetTable, timestamp)
	default:
		return fmt.Sprintf("Auto-generated mapping from '%s' to '%s' created on %s",
			sourceDB, targetDB, timestamp)
	}
}

// generateMCPMappingName creates a concise name for MCP resource mappings
func generateMCPMappingName(scope, sourceDB, sourceTable, mcpResourceName string) string {
	switch scope {
	case "database":
		return fmt.Sprintf("%s_to_mcp_%s", sourceDB, mcpResourceName)
	case "table":
		return fmt.Sprintf("%s_%s_to_mcp_%s", sourceDB, sourceTable, mcpResourceName)
	default:
		return fmt.Sprintf("%s_to_mcp_%s", sourceDB, mcpResourceName)
	}
}

// generateMCPMappingDescription creates a verbose description for MCP resource mappings
func generateMCPMappingDescription(scope, sourceDB, sourceTable, mcpResourceName string) string {
	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05 UTC")

	switch scope {
	case "database":
		return fmt.Sprintf("Auto-generated MCP mapping from database '%s' to MCP resource '%s' created on %s",
			sourceDB, mcpResourceName, timestamp)
	case "table":
		return fmt.Sprintf("Auto-generated MCP mapping from table '%s.%s' to MCP resource '%s' created on %s",
			sourceDB, sourceTable, mcpResourceName, timestamp)
	default:
		return fmt.Sprintf("Auto-generated MCP mapping from '%s' to MCP resource '%s' created on %s",
			sourceDB, mcpResourceName, timestamp)
	}
}

// CopyMappingData copies data from source to target using the specified mapping
func CopyMappingData(mappingName string, batchSize, parallelWorkers int32, dryRun, progress bool) error {
	mappingName = strings.TrimSpace(mappingName)
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	// Set default values if not provided
	if batchSize <= 0 {
		batchSize = 1000
	}
	if parallelWorkers <= 0 {
		parallelWorkers = 4
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s/copy-data", mappingName))
	if err != nil {
		return err
	}

	// Create the copy data request
	copyDataReq := struct {
		BatchSize       int32 `json:"batch_size"`
		ParallelWorkers int32 `json:"parallel_workers"`
		DryRun          bool  `json:"dry_run"`
		Progress        bool  `json:"progress"`
	}{
		BatchSize:       batchSize,
		ParallelWorkers: parallelWorkers,
		DryRun:          dryRun,
		Progress:        progress,
	}

	fmt.Printf("Starting data copy for mapping '%s'...\n", mappingName)
	if dryRun {
		fmt.Println("DRY RUN MODE: No data will be actually copied")
	}
	fmt.Printf("Configuration: batch_size=%d, parallel_workers=%d\n", batchSize, parallelWorkers)
	fmt.Println()

	// For now, make a simple POST request. In the future, this should be a streaming request
	// to handle real-time progress updates
	var response struct {
		Message       string   `json:"message"`
		Success       bool     `json:"success"`
		Status        string   `json:"status"`
		RowsProcessed int64    `json:"rows_processed"`
		TotalRows     int64    `json:"total_rows"`
		CurrentTable  string   `json:"current_table"`
		Errors        []string `json:"errors"`
		OperationID   string   `json:"operation_id"`
	}

	if err := client.Post(url, copyDataReq, &response); err != nil {
		return fmt.Errorf("failed to start data copy: %v", err)
	}

	if !response.Success {
		fmt.Printf("Data copy failed: %s\n", response.Message)
		if len(response.Errors) > 0 {
			fmt.Println("Errors:")
			for _, errMsg := range response.Errors {
				fmt.Printf("  - %s\n", errMsg)
			}
		}
		return fmt.Errorf("data copy operation failed")
	}

	fmt.Printf("Data copy completed successfully!\n")
	fmt.Printf("Operation ID: %s\n", response.OperationID)
	if response.TotalRows > 0 {
		fmt.Printf("Rows processed: %d/%d\n", response.RowsProcessed, response.TotalRows)
	}
	if response.CurrentTable != "" {
		fmt.Printf("Last table processed: %s\n", response.CurrentTable)
	}

	if len(response.Errors) > 0 {
		fmt.Println("\nWarnings/Non-fatal errors:")
		for _, errMsg := range response.Errors {
			fmt.Printf("  - %s\n", errMsg)
		}
	}

	return nil
}

// ModifyMappingRule modifies an existing mapping rule
func ModifyMappingRule(mappingName, ruleName, source, target, transformation string, order int32) error {
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}
	if ruleName == "" {
		return fmt.Errorf("rule name is required")
	}

	// At least one modification parameter must be provided
	if source == "" && target == "" && transformation == "" && order == -1 {
		return fmt.Errorf("at least one modification parameter must be provided (source, target, transformation, or order)")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s/rules/%s", mappingName, ruleName))
	if err != nil {
		return err
	}

	// Build the request
	modifyReq := struct {
		Source         *string `json:"source,omitempty"`
		Target         *string `json:"target,omitempty"`
		Transformation *string `json:"transformation,omitempty"`
		Order          *int32  `json:"order,omitempty"`
	}{}

	if source != "" {
		modifyReq.Source = &source
	}
	if target != "" {
		modifyReq.Target = &target
	}
	if transformation != "" {
		modifyReq.Transformation = &transformation
	}
	if order >= 0 {
		modifyReq.Order = &order
	}

	var response struct {
		Message string      `json:"message"`
		Success bool        `json:"success"`
		Rule    MappingRule `json:"rule"`
		Status  string      `json:"status"`
	}

	if err := client.Put(url, modifyReq, &response); err != nil {
		return fmt.Errorf("failed to modify mapping rule: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to modify mapping rule: %s", response.Message)
	}

	fmt.Printf("Successfully modified mapping rule '%s' in mapping '%s'\n", ruleName, mappingName)
	return nil
}

// AddMappingRule creates a new mapping rule and attaches it to a mapping
func AddMappingRule(mappingName, ruleName, source, target, transformation string, order int32) error {
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}
	if ruleName == "" {
		return fmt.Errorf("rule name is required")
	}
	if source == "" {
		return fmt.Errorf("source column is required")
	}
	if target == "" {
		return fmt.Errorf("target column is required")
	}
	if transformation == "" {
		transformation = "direct_mapping"
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s/rules", mappingName))
	if err != nil {
		return err
	}

	// Build the request
	addReq := struct {
		RuleName       string `json:"rule_name"`
		Source         string `json:"source"`
		Target         string `json:"target"`
		Transformation string `json:"transformation"`
		Order          *int32 `json:"order,omitempty"`
	}{
		RuleName:       ruleName,
		Source:         source,
		Target:         target,
		Transformation: transformation,
	}

	if order >= 0 {
		addReq.Order = &order
	}

	var response struct {
		Message string      `json:"message"`
		Success bool        `json:"success"`
		Rule    MappingRule `json:"rule"`
		Status  string      `json:"status"`
	}

	if err := client.Post(url, addReq, &response); err != nil {
		return fmt.Errorf("failed to add mapping rule: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to add mapping rule: %s", response.Message)
	}

	fmt.Printf("Successfully added mapping rule '%s' to mapping '%s'\n", ruleName, mappingName)
	return nil
}

// RemoveMappingRule removes a mapping rule from a mapping
func RemoveMappingRule(mappingName, ruleName string, deleteRule bool) error {
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}
	if ruleName == "" {
		return fmt.Errorf("rule name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s/rules/%s", mappingName, ruleName))
	if err != nil {
		return err
	}

	// Add delete query parameter if requested
	if deleteRule {
		url += "?delete=true"
	}

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to remove mapping rule: %v", err)
	}

	if deleteRule {
		fmt.Printf("Successfully removed and deleted mapping rule '%s' from mapping '%s'\n", ruleName, mappingName)
	} else {
		fmt.Printf("Successfully removed mapping rule '%s' from mapping '%s'\n", ruleName, mappingName)
	}
	return nil
}

// RemoveMapping removes a mapping and optionally deletes associated rules
func RemoveMapping(mappingName string, keepRules bool) error {
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s", mappingName))
	if err != nil {
		return err
	}

	// Add keep_rules query parameter if requested
	if keepRules {
		url += "?keep_rules=true"
	}

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to remove mapping: %v", err)
	}

	if keepRules {
		fmt.Printf("Successfully removed mapping '%s' (rules preserved)\n", mappingName)
	} else {
		fmt.Printf("Successfully removed mapping '%s' (unused rules deleted)\n", mappingName)
	}
	return nil
}

// ListMappingRules lists all mapping rules in a mapping
func ListMappingRules(mappingName string) error {
	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s/rules", mappingName))
	if err != nil {
		return err
	}

	var response struct {
		Rules []MappingRule `json:"rules"`
	}

	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list mapping rules: %v", err)
	}

	if len(response.Rules) == 0 {
		fmt.Printf("No mapping rules found for mapping '%s'\n", mappingName)
		return nil
	}

	fmt.Println()
	fmt.Printf("Mapping Rules for '%s':\n", mappingName)
	fmt.Println(strings.Repeat("=", 120))
	fmt.Printf("%-30s %-30s %-30s %-20s\n", "Rule Name", "Source", "Target", "Transformation")
	fmt.Println(strings.Repeat("-", 120))

	for _, rule := range response.Rules {
		// Truncate fields if too long
		ruleName := rule.MappingRuleName
		if len(ruleName) > 29 {
			ruleName = ruleName[:26] + "..."
		}

		source := rule.MappingRuleSource
		if len(source) > 29 {
			source = source[:26] + "..."
		}

		target := rule.MappingRuleTarget
		if len(target) > 29 {
			target = target[:26] + "..."
		}

		transformation := rule.MappingRuleTransformationName
		if len(transformation) > 19 {
			transformation = transformation[:16] + "..."
		}

		fmt.Printf("%-30s %-30s %-30s %-20s\n", ruleName, source, target, transformation)
	}
	fmt.Println()

	return nil
}

// ValidateMapping validates a mapping
func ValidateMapping(mappingName string) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s/validate", mappingName))
	if err != nil {
		return err
	}

	// Make request
	fmt.Printf("Validating mapping '%s'...\n", mappingName)
	var result map[string]interface{}
	if err := client.Post(url, nil, &result); err != nil {
		return fmt.Errorf("failed to validate mapping: %w", err)
	}

	// Extract validation results
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid response format")
	}

	isValid, _ := data["is_valid"].(bool)
	errorsRaw, _ := data["errors"].([]interface{})
	warningsRaw, _ := data["warnings"].([]interface{})
	validatedAt, _ := data["validated_at"].(string)

	// Convert errors and warnings
	errors := []string{}
	for _, e := range errorsRaw {
		if str, ok := e.(string); ok {
			errors = append(errors, str)
		}
	}

	warnings := []string{}
	for _, w := range warningsRaw {
		if str, ok := w.(string); ok {
			warnings = append(warnings, str)
		}
	}

	// Display results
	fmt.Println("\n╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                   MAPPING VALIDATION REPORT                   ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Mapping: %-52s ║\n", mappingName)
	fmt.Printf("║ Validated At: %-47s ║\n", validatedAt)
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	if isValid {
		fmt.Println("║ Status: ✓ VALID                                               ║")
	} else {
		fmt.Println("║ Status: ✗ INVALID                                             ║")
	}

	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	if len(errors) > 0 {
		fmt.Printf("║ Errors: %-53d ║\n", len(errors))
		fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
		for i, errMsg := range errors {
			// Word wrap long error messages
			lines := wrapText(errMsg, 61)
			for j, line := range lines {
				if j == 0 {
					fmt.Printf("║ %d. %-58s ║\n", i+1, line)
				} else {
					fmt.Printf("║    %-58s ║\n", line)
				}
			}
		}
		fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	} else {
		fmt.Println("║ No errors found                                               ║")
		fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	}

	if len(warnings) > 0 {
		fmt.Printf("║ Warnings: %-51d ║\n", len(warnings))
		fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
		for i, warnMsg := range warnings {
			// Word wrap long warning messages
			lines := wrapText(warnMsg, 61)
			for j, line := range lines {
				if j == 0 {
					fmt.Printf("║ %d. %-58s ║\n", i+1, line)
				} else {
					fmt.Printf("║    %-58s ║\n", line)
				}
			}
		}
		fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	} else {
		fmt.Println("║ No warnings found                                             ║")
		fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")

	if !isValid {
		// Exit with error code but don't return error to avoid showing usage
		os.Exit(1)
	}

	return nil
}

// wrapText wraps text to specified width
func wrapText(text string, width int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{}
	}

	lines := []string{}
	currentLine := words[0]

	for _, word := range words[1:] {
		if len(currentLine)+1+len(word) <= width {
			currentLine += " " + word
		} else {
			lines = append(lines, currentLine)
			currentLine = word
		}
	}
	lines = append(lines, currentLine)

	return lines
}
