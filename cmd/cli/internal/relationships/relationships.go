package relationships

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

// AddRelationship creates a new relationship using an existing mapping
func AddRelationship(mappingName string, relationshipType string) error {
	mappingName = strings.TrimSpace(mappingName)
	relationshipType = strings.TrimSpace(relationshipType)

	if mappingName == "" {
		return fmt.Errorf("mapping name is required")
	}

	// Validate relationship type
	// Note: Only 'replication' is currently supported by the backend
	validTypes := map[string]bool{
		"replication": true,
		// Future types to be supported:
		// "migration":    true,
		// "multi-master": true,
	}

	if !validTypes[relationshipType] {
		return fmt.Errorf("invalid relationship type: %s (currently only 'replication' is supported)", relationshipType)
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// First, get the mapping to extract source and target information
	mappingURL, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mappings/%s", mappingName))
	if err != nil {
		return err
	}

	var mappingResp struct {
		Mapping struct {
			MappingID    string `json:"mapping_id"`
			MappingName  string `json:"mapping_name"`
			MappingType  string `json:"mapping_type"`
			MappingRules []struct {
				MappingRuleSource string `json:"mapping_rule_source"`
				MappingRuleTarget string `json:"mapping_rule_target"`
			} `json:"mapping_rules"`
		} `json:"mapping"`
	}

	if err := client.Get(mappingURL, &mappingResp); err != nil {
		return fmt.Errorf("failed to get mapping: %v", err)
	}

	// Check if mapping has rules
	if len(mappingResp.Mapping.MappingRules) == 0 {
		return fmt.Errorf("mapping has no rules")
	}

	// Extract source and target from the first rule
	// New format is: redb:/data/database/{database_id}/table/{table_name}/column/{column_name}
	firstRule := mappingResp.Mapping.MappingRules[0]

	// Parse source identifier
	sourceIdentifier := firstRule.MappingRuleSource
	sourceDatabaseID, sourceTableName, err := parseResourceURI(sourceIdentifier)
	if err != nil {
		return fmt.Errorf("invalid source identifier: %s (%v)", sourceIdentifier, err)
	}

	// Parse target identifier
	targetIdentifier := firstRule.MappingRuleTarget
	targetDatabaseID, targetTableName, err := parseResourceURI(targetIdentifier)
	if err != nil {
		return fmt.Errorf("invalid target identifier: %s (%v)", targetIdentifier, err)
	}

	// Get database names for generating relationship name
	sourceDatabaseName, err := getDatabaseNameByID(client, profileInfo, sourceDatabaseID)
	if err != nil {
		return fmt.Errorf("failed to get source database name: %v", err)
	}

	targetDatabaseName, err := getDatabaseNameByID(client, profileInfo, targetDatabaseID)
	if err != nil {
		return fmt.Errorf("failed to get target database name: %v", err)
	}

	// Generate a relationship name
	relationshipName := fmt.Sprintf("%s_to_%s", sourceDatabaseName, targetDatabaseName)

	// Generate a relationship description
	relationshipDescription := fmt.Sprintf("Auto-generated %s relationship from %s.%s to %s.%s using mapping %s",
		relationshipType, sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName, mappingName)

	// Create the relationship
	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/relationships")
	if err != nil {
		return err
	}

	addRelReq := struct {
		RelationshipName             string `json:"relationship_name"`
		RelationshipDescription      string `json:"relationship_description"`
		RelationshipType             string `json:"relationship_type"`
		RelationshipSourceDatabaseID string `json:"relationship_source_database_id"`
		RelationshipSourceTableName  string `json:"relationship_source_table_name"`
		RelationshipTargetDatabaseID string `json:"relationship_target_database_id"`
		RelationshipTargetTableName  string `json:"relationship_target_table_name"`
		MappingID                    string `json:"mapping_id"`
		PolicyID                     string `json:"policy_id"`
	}{
		RelationshipName:             relationshipName,
		RelationshipDescription:      relationshipDescription,
		RelationshipType:             relationshipType,
		RelationshipSourceDatabaseID: sourceDatabaseID,
		RelationshipSourceTableName:  sourceTableName,
		RelationshipTargetDatabaseID: targetDatabaseID,
		RelationshipTargetTableName:  targetTableName,
		MappingID:                    mappingResp.Mapping.MappingID,
		PolicyID:                     "", // Empty string for now, can be made configurable later
	}

	var response struct {
		Message      string `json:"message"`
		Success      bool   `json:"success"`
		Relationship struct {
			RelationshipID   string `json:"relationship_id"`
			RelationshipName string `json:"relationship_name"`
		} `json:"relationship"`
	}

	if err := client.Post(url, addRelReq, &response); err != nil {
		return fmt.Errorf("failed to create relationship: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to create relationship: %s", response.Message)
	}

	fmt.Printf("✓ Relationship '%s' created successfully\n", response.Relationship.RelationshipName)
	fmt.Printf("  Relationship ID: %s\n", response.Relationship.RelationshipID)
	fmt.Printf("\nTo start the relationship and begin synchronization, run:\n")
	fmt.Printf("  redb relationships start %s\n", response.Relationship.RelationshipName)

	return nil
}

// StartRelationship starts a relationship to begin CDC synchronization
func StartRelationship(relationshipName string, batchSize, parallelWorkers int32) error {
	relationshipName = strings.TrimSpace(relationshipName)
	if relationshipName == "" {
		return fmt.Errorf("relationship name is required")
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

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/relationships/%s/start", relationshipName))
	if err != nil {
		return err
	}

	startReq := struct {
		BatchSize       int32 `json:"batch_size"`
		ParallelWorkers int32 `json:"parallel_workers"`
	}{
		BatchSize:       batchSize,
		ParallelWorkers: parallelWorkers,
	}

	// Make the POST request and handle streaming response
	resp, err := client.PostStream(url, startReq)
	if err != nil {
		return fmt.Errorf("failed to start relationship: %v", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("failed to start relationship: HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Parse Server-Sent Events stream
	fmt.Printf("\n📊 Starting relationship synchronization: '%s'\n", relationshipName)
	fmt.Printf("Configuration: batch_size=%d, parallel_workers=%d\n", batchSize, parallelWorkers)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	scanner := bufio.NewScanner(resp.Body)
	lastPhase := ""

	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines and non-data lines
		if line == "" || !strings.HasPrefix(line, "data: ") {
			continue
		}

		// Parse the JSON data
		dataStr := strings.TrimPrefix(line, "data: ")
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(dataStr), &event); err != nil {
			continue
		}

		// Check for error
		if errFlag, ok := event["error"].(bool); ok && errFlag {
			if msg, ok := event["message"].(string); ok {
				return fmt.Errorf("relationship start failed: %s", msg)
			}
			return fmt.Errorf("relationship start failed")
		}

		// Display progress
		phase := getStringField(event, "phase")
		message := getStringField(event, "message")

		if phase != lastPhase && phase != "" {
			fmt.Printf("\n🔹 Phase: %s\n", phase)
			lastPhase = phase
		}

		if message != "" {
			fmt.Printf("   %s\n", message)
		}

		// Show progress details
		if rowsCopied, ok := event["rows_copied"].(float64); ok && rowsCopied > 0 {
			if totalRows, ok := event["total_rows"].(float64); ok && totalRows > 0 {
				progress := (rowsCopied / totalRows) * 100
				fmt.Printf("   Progress: %.1f%% (%d/%d rows)\n", progress, int64(rowsCopied), int64(totalRows))
			}
		}

		// Check if we're done
		if phase == "active" {
			fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			fmt.Printf("✓ Relationship '%s' is now active and synchronizing!\n", relationshipName)
			fmt.Println("\nThe initial data copy is complete and CDC replication is running.")
			fmt.Println("Changes to the source database will be automatically synchronized.")
			return nil
		}

		if phase == "error" {
			fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			return fmt.Errorf("relationship failed to start: %s", message)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading stream: %v", err)
	}

	return nil
}

// Helper function to safely get string fields from map
func getStringField(m map[string]interface{}, key string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return ""
}

// StopRelationship stops a running relationship
func StopRelationship(relationshipName string) error {
	relationshipName = strings.TrimSpace(relationshipName)
	if relationshipName == "" {
		return fmt.Errorf("relationship name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/relationships/%s/stop", relationshipName))
	if err != nil {
		return err
	}

	fmt.Printf("Stopping relationship '%s'...\n", relationshipName)

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
	}

	if err := client.Post(url, nil, &response); err != nil {
		return fmt.Errorf("failed to stop relationship: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to stop relationship: %s", response.Message)
	}

	fmt.Printf("✓ Relationship '%s' stopped successfully\n", relationshipName)
	fmt.Printf("\nThe CDC synchronization is paused. To resume, run:\n")
	fmt.Printf("  redb relationships resume %s\n", relationshipName)

	return nil
}

// ResumeRelationship resumes a stopped relationship
func ResumeRelationship(relationshipName string, skipDataSync bool) error {
	relationshipName = strings.TrimSpace(relationshipName)
	if relationshipName == "" {
		return fmt.Errorf("relationship name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/relationships/%s/resume", relationshipName))
	if err != nil {
		return err
	}

	resumeReq := struct {
		SkipDataSync bool `json:"skip_data_sync"`
	}{
		SkipDataSync: skipDataSync,
	}

	fmt.Printf("Resuming relationship '%s'...\n", relationshipName)

	var response struct {
		Message   string   `json:"message"`
		Success   bool     `json:"success"`
		Phase     string   `json:"phase"`
		CDCStatus string   `json:"cdc_status"`
		Errors    []string `json:"errors"`
	}

	if err := client.Post(url, resumeReq, &response); err != nil {
		return fmt.Errorf("failed to resume relationship: %v", err)
	}

	if !response.Success {
		fmt.Printf("✗ Relationship resume failed: %s\n", response.Message)
		if len(response.Errors) > 0 {
			fmt.Println("\nErrors:")
			for _, e := range response.Errors {
				fmt.Printf("  - %s\n", e)
			}
		}
		return fmt.Errorf("relationship resume failed")
	}

	fmt.Printf("✓ Relationship '%s' resumed successfully\n", relationshipName)
	fmt.Printf("  Phase: %s\n", response.Phase)
	fmt.Printf("  CDC status: %s\n", response.CDCStatus)
	fmt.Printf("\nThe relationship is now active and synchronizing changes in real-time.\n")

	return nil
}

// RemoveRelationship removes a relationship completely
func RemoveRelationship(relationshipName string, force bool) error {
	relationshipName = strings.TrimSpace(relationshipName)
	if relationshipName == "" {
		return fmt.Errorf("relationship name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/relationships/%s/remove", relationshipName))
	if err != nil {
		return err
	}

	if force {
		url += "?force=true"
	}

	fmt.Printf("Removing relationship '%s'...\n", relationshipName)
	if force {
		fmt.Println("(Force mode enabled)")
	}

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to remove relationship: %v", err)
	}

	fmt.Printf("✓ Relationship '%s' removed successfully\n", relationshipName)
	fmt.Println("\nThe relationship has been deleted and CDC synchronization stopped.")
	fmt.Println("Note: Data in the target database has not been deleted.")

	return nil
}

// ListRelationships lists all relationships in the workspace
func ListRelationships() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/relationships")
	if err != nil {
		return err
	}

	var response struct {
		Relationships []struct {
			RelationshipName             string `json:"relationship_name"`
			RelationshipType             string `json:"relationship_type"`
			Status                       string `json:"status"`
			StatusMessage                string `json:"status_message"`
			RelationshipSourceDatabaseID string `json:"relationship_source_database_id"`
			RelationshipSourceTableName  string `json:"relationship_source_table_name"`
			RelationshipTargetDatabaseID string `json:"relationship_target_database_id"`
			RelationshipTargetTableName  string `json:"relationship_target_table_name"`
		} `json:"relationships"`
	}

	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list relationships: %v", err)
	}

	if len(response.Relationships) == 0 {
		fmt.Println("No relationships found in this workspace.")
		fmt.Println("\nTo create a relationship, run:")
		fmt.Println("  redb relationships add --mapping <mapping-name> --type <type>")
		return nil
	}

	// Get database names for all unique database IDs
	dbNames := make(map[string]string)
	for _, rel := range response.Relationships {
		if _, exists := dbNames[rel.RelationshipSourceDatabaseID]; !exists {
			name, err := getDatabaseNameByID(client, profileInfo, rel.RelationshipSourceDatabaseID)
			if err == nil {
				dbNames[rel.RelationshipSourceDatabaseID] = name
			} else {
				dbNames[rel.RelationshipSourceDatabaseID] = rel.RelationshipSourceDatabaseID
			}
		}
		if _, exists := dbNames[rel.RelationshipTargetDatabaseID]; !exists {
			name, err := getDatabaseNameByID(client, profileInfo, rel.RelationshipTargetDatabaseID)
			if err == nil {
				dbNames[rel.RelationshipTargetDatabaseID] = name
			} else {
				dbNames[rel.RelationshipTargetDatabaseID] = rel.RelationshipTargetDatabaseID
			}
		}
	}

	// Use tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Println()
	fmt.Fprintln(w, "Name\tType\tSource\tTarget\tStatus")
	fmt.Fprintln(w, "----\t----\t------\t------\t------")
	for _, rel := range response.Relationships {
		sourceDB := dbNames[rel.RelationshipSourceDatabaseID]
		targetDB := dbNames[rel.RelationshipTargetDatabaseID]
		source := fmt.Sprintf("%s.%s", sourceDB, rel.RelationshipSourceTableName)
		target := fmt.Sprintf("%s.%s", targetDB, rel.RelationshipTargetTableName)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			rel.RelationshipName,
			rel.RelationshipType,
			source,
			target,
			rel.Status)
	}
	_ = w.Flush()
	fmt.Println()

	return nil
}

// ShowRelationship shows details of a specific relationship
func ShowRelationship(relationshipName string) error {
	relationshipName = strings.TrimSpace(relationshipName)
	if relationshipName == "" {
		return fmt.Errorf("relationship name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	if err := common.ValidateWorkspace(profileInfo); err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/relationships/%s", relationshipName))
	if err != nil {
		return err
	}

	var response struct {
		Relationship struct {
			RelationshipID          string `json:"relationship_id"`
			RelationshipName        string `json:"relationship_name"`
			RelationshipType        string `json:"relationship_type"`
			RelationshipDescription string `json:"relationship_description"`
			SourceDatabaseID        string `json:"relationship_source_database_id"`
			SourceTableName         string `json:"relationship_source_table_name"`
			TargetDatabaseID        string `json:"relationship_target_database_id"`
			TargetTableName         string `json:"relationship_target_table_name"`
			MappingID               string `json:"mapping_id"`
			Status                  string `json:"status"`
			StatusMessage           string `json:"status_message"`
			Created                 string `json:"created"`
			Updated                 string `json:"updated"`
		} `json:"relationship"`
	}

	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get relationship: %v", err)
	}

	rel := response.Relationship

	// Get database names
	sourceDBName, err := getDatabaseNameByID(client, profileInfo, rel.SourceDatabaseID)
	if err != nil {
		sourceDBName = rel.SourceDatabaseID
	}
	targetDBName, err := getDatabaseNameByID(client, profileInfo, rel.TargetDatabaseID)
	if err != nil {
		targetDBName = rel.TargetDatabaseID
	}

	fmt.Printf("\nRelationship Details: %s\n", rel.RelationshipName)
	fmt.Printf("=====================================\n\n")
	fmt.Printf("ID:          %s\n", rel.RelationshipID)
	fmt.Printf("Type:        %s\n", rel.RelationshipType)
	fmt.Printf("Description: %s\n", rel.RelationshipDescription)
	fmt.Printf("\nSource:\n")
	fmt.Printf("  Database: %s\n", sourceDBName)
	fmt.Printf("  Table:    %s\n", rel.SourceTableName)
	fmt.Printf("\nTarget:\n")
	fmt.Printf("  Database: %s\n", targetDBName)
	fmt.Printf("  Table:    %s\n", rel.TargetTableName)
	fmt.Printf("\nMapping ID:  %s\n", rel.MappingID)
	fmt.Printf("Status:      %s\n", rel.Status)
	if rel.StatusMessage != "" {
		fmt.Printf("Message:     %s\n", rel.StatusMessage)
	}
	if rel.Created != "" {
		fmt.Printf("\nCreated:     %s\n", rel.Created)
	}
	if rel.Updated != "" {
		fmt.Printf("Updated:     %s\n", rel.Updated)
	}
	fmt.Println()

	return nil
}

// Helper function to get database name by ID
func getDatabaseNameByID(client *httpclient.ProfileHTTPClient, profileInfo *common.ProfileInfo, databaseID string) (string, error) {
	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/databases")
	if err != nil {
		return "", err
	}

	var response struct {
		Databases []struct {
			DatabaseID   string `json:"database_id"`
			DatabaseName string `json:"database_name"`
		} `json:"databases"`
	}

	if err := client.Get(url, &response); err != nil {
		return "", err
	}

	for _, db := range response.Databases {
		if db.DatabaseID == databaseID {
			return db.DatabaseName, nil
		}
	}

	return "", fmt.Errorf("database with ID '%s' not found", databaseID)
}

// parseResourceURI parses the new resource URI format and extracts database ID and table name
// Format: redb:/data/database/{database_id}/table/{table_name}/column/{column_name}
func parseResourceURI(uri string) (databaseID, tableName string, err error) {
	// Check if it starts with redb:/
	if !strings.HasPrefix(uri, "redb:/") {
		return "", "", fmt.Errorf("URI must start with 'redb:/' (got: %s)", uri)
	}

	// Remove the protocol prefix
	path := strings.TrimPrefix(uri, "redb:/")
	
	// Split by /
	parts := strings.Split(path, "/")
	
	// Expected format: data/database/{id}/table/{name}/column/{col}
	// parts[0] = "data" (scope)
	// parts[1] = "database" (resource type)
	// parts[2] = database ID
	// parts[3] = "table" (object type)
	// parts[4] = table name
	// parts[5] = "column" (segment type)
	// parts[6] = column name
	
	if len(parts) < 7 {
		return "", "", fmt.Errorf("invalid URI format, expected: redb:/data/database/{id}/table/{name}/column/{col}")
	}
	
	if parts[0] != "data" {
		return "", "", fmt.Errorf("expected scope 'data', got: %s", parts[0])
	}
	
	if parts[1] != "database" {
		return "", "", fmt.Errorf("expected resource type 'database', got: %s", parts[1])
	}
	
	if parts[3] != "table" {
		return "", "", fmt.Errorf("expected object type 'table', got: %s", parts[3])
	}
	
	if parts[5] != "column" {
		return "", "", fmt.Errorf("expected segment type 'column', got: %s", parts[5])
	}
	
	databaseID = parts[2]
	tableName = parts[4]
	
	return databaseID, tableName, nil
}
