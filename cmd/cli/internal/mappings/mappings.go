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
}

// AddMapping creates a new mapping with specified scope
func AddMapping(scope, source, target, name, description, policyID string) error {
	// Validate scope
	if scope != "database" && scope != "table" {
		return fmt.Errorf("invalid scope '%s': must be 'database' or 'table'", scope)
	}

	// Parse source and target
	sourceDB, sourceTable, err := parseSourceTarget(source)
	if err != nil {
		return fmt.Errorf("invalid source format: %v", err)
	}

	targetDB, targetTable, err := parseSourceTarget(target)
	if err != nil {
		return fmt.Errorf("invalid target format: %v", err)
	}

	// Validate scope-specific requirements
	if scope == "table" {
		if sourceTable == "" || targetTable == "" {
			return fmt.Errorf("table scope requires both source and target to include table names (format: database.table)")
		}
	}

	// Generate name and description if not provided
	if name == "" {
		name = generateMappingName(scope, sourceDB, sourceTable, targetDB, targetTable)
	}

	if description == "" {
		description = generateMappingDescription(scope, sourceDB, sourceTable, targetDB, targetTable)
	}

	// Create the mapping request
	mappingReq := struct {
		MappingName        string `json:"mapping_name"`
		MappingDescription string `json:"mapping_description"`
		Scope              string `json:"scope"`
		Source             string `json:"source"`
		Target             string `json:"target"`
		PolicyID           string `json:"policy_id,omitempty"`
	}{
		MappingName:        name,
		MappingDescription: description,
		Scope:              scope,
		Source:             source,
		Target:             target,
		PolicyID:           policyID,
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

// parseSourceTarget parses database[.table] format
func parseSourceTarget(input string) (database, table string, err error) {
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
	fmt.Printf("%-20s %-30s %-15s %-10s\n", "Name", "Description", "Type", "Rules")
	fmt.Println(strings.Repeat("-", 80))
	for _, mapping := range mappingsResponse.Mappings {
		description := mapping.MappingDescription
		if len(description) > 28 {
			description = description[:25] + "..."
		}
		fmt.Printf("%-20s %-30s %-15s %-10d\n",
			mapping.MappingName,
			description,
			mapping.MappingType,
			mapping.MappingRuleCount)
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

			// Format match score as percentage
			matchScore := fmt.Sprintf("%.1f%%", rule.MappingRuleMetadata.MatchScore*100)

			fmt.Printf("%-55s %-30s %-30s %-20s %-10s\n",
				ruleName,
				sourceCol,
				targetCol,
				transformName,
				matchScore)
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
