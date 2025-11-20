package mappings

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

// StreamToTableMappingRequest represents the request for stream-to-table mapping
type StreamToTableMappingRequest struct {
	MappingName           string                   `json:"mapping_name"`
	MappingDescription    string                   `json:"mapping_description"`
	SourceIntegrationName string                   `json:"source_integration_name"`
	SourceTopicName       string                   `json:"source_topic_name"`
	TargetDatabaseName    string                   `json:"target_database_name"`
	TargetTableName       string                   `json:"target_table_name"`
	PolicyID              string                   `json:"policy_id,omitempty"`
	Filters               []StreamMappingFilterReq `json:"filters,omitempty"`
}

// TableToStreamMappingRequest represents the request for table-to-stream mapping
type TableToStreamMappingRequest struct {
	MappingName           string                   `json:"mapping_name"`
	MappingDescription    string                   `json:"mapping_description"`
	SourceDatabaseName    string                   `json:"source_database_name"`
	SourceTableName       string                   `json:"source_table_name"`
	TargetIntegrationName string                   `json:"target_integration_name"`
	TargetTopicName       string                   `json:"target_topic_name"`
	PolicyID              string                   `json:"policy_id,omitempty"`
	Filters               []StreamMappingFilterReq `json:"filters,omitempty"`
}

// StreamToStreamMappingRequest represents the request for stream-to-stream mapping
type StreamToStreamMappingRequest struct {
	MappingName           string                   `json:"mapping_name"`
	MappingDescription    string                   `json:"mapping_description"`
	SourceIntegrationName string                   `json:"source_integration_name"`
	SourceTopicName       string                   `json:"source_topic_name"`
	TargetIntegrationName string                   `json:"target_integration_name"`
	TargetTopicName       string                   `json:"target_topic_name"`
	PolicyID              string                   `json:"policy_id,omitempty"`
	Filters               []StreamMappingFilterReq `json:"filters,omitempty"`
}

// StreamMappingFilterReq represents a stream mapping filter
type StreamMappingFilterReq struct {
	FilterType       string                 `json:"filter_type"`
	FilterExpression map[string]interface{} `json:"filter_expression"`
	FilterOrder      int32                  `json:"filter_order"`
	FilterOperator   string                 `json:"filter_operator"`
}

// StreamMappingResponse represents the response from stream mapping creation
type StreamMappingResponse struct {
	Message string  `json:"message"`
	Success bool    `json:"success"`
	Status  string  `json:"status"`
	Mapping Mapping `json:"mapping"`
}

// AddStreamToTableMapping creates a mapping from a stream topic to a database table
func AddStreamToTableMapping(sourceIntegration, sourceTopic, targetDatabase, targetTable, name, description, policyID string) error {
	// Get profile-aware HTTP client
	client, err := httpclient.GetProfileClient()
	if err != nil {
		return fmt.Errorf("failed to get HTTP client: %w", err)
	}

	// Get active profile to extract tenant and workspace
	profile, err := client.GetActiveProfile()
	if err != nil {
		return fmt.Errorf("failed to get active profile: %w", err)
	}

	// Generate name and description if not provided
	if name == "" {
		name = fmt.Sprintf("%s-%s-to-%s-%s", sourceIntegration, sourceTopic, targetDatabase, targetTable)
	}

	if description == "" {
		description = fmt.Sprintf("Stream-to-table mapping from %s/%s to %s.%s", sourceIntegration, sourceTopic, targetDatabase, targetTable)
	}

	// Build request
	reqBody := StreamToTableMappingRequest{
		MappingName:           name,
		MappingDescription:    description,
		SourceIntegrationName: sourceIntegration,
		SourceTopicName:       sourceTopic,
		TargetDatabaseName:    targetDatabase,
		TargetTableName:       targetTable,
		PolicyID:              policyID,
	}

	// Make API call
	url := fmt.Sprintf("%s/%s/api/v1/workspaces/%s/mappings/stream-to-table", profile.Hostname, profile.TenantURL, profile.Workspace)

	var response StreamMappingResponse
	if err := client.Post(url, reqBody, &response); err != nil {
		return fmt.Errorf("failed to create stream-to-table mapping: %w", err)
	}

	// Display success message
	green := color.New(color.FgGreen, color.Bold).SprintFunc()
	fmt.Printf("%s Stream-to-table mapping created successfully: %s\n", green("✓"), name)

	// Display mapping details
	fmt.Println("\nMapping Details:")
	fmt.Printf("  Mapping ID:   %s\n", response.Mapping.MappingID)
	fmt.Printf("  Name:         %s\n", response.Mapping.MappingName)
	fmt.Printf("  Type:         %s\n", response.Mapping.MappingType)
	fmt.Printf("  Source:       %s/%s\n", sourceIntegration, sourceTopic)
	fmt.Printf("  Target:       %s.%s\n", targetDatabase, targetTable)

	return nil
}

// AddTableToStreamMapping creates a mapping from a database table to a stream topic
func AddTableToStreamMapping(sourceDatabase, sourceTable, targetIntegration, targetTopic, name, description, policyID string) error {
	// Get profile-aware HTTP client
	client, err := httpclient.GetProfileClient()
	if err != nil {
		return fmt.Errorf("failed to get HTTP client: %w", err)
	}

	// Get active profile to extract tenant and workspace
	profile, err := client.GetActiveProfile()
	if err != nil {
		return fmt.Errorf("failed to get active profile: %w", err)
	}

	// Generate name and description if not provided
	if name == "" {
		name = fmt.Sprintf("%s-%s-to-%s-%s", sourceDatabase, sourceTable, targetIntegration, targetTopic)
	}

	if description == "" {
		description = fmt.Sprintf("Table-to-stream mapping from %s.%s to %s/%s", sourceDatabase, sourceTable, targetIntegration, targetTopic)
	}

	// Build request
	reqBody := TableToStreamMappingRequest{
		MappingName:           name,
		MappingDescription:    description,
		SourceDatabaseName:    sourceDatabase,
		SourceTableName:       sourceTable,
		TargetIntegrationName: targetIntegration,
		TargetTopicName:       targetTopic,
		PolicyID:              policyID,
	}

	// Make API call
	url := fmt.Sprintf("%s/%s/api/v1/workspaces/%s/mappings/table-to-stream", profile.Hostname, profile.TenantURL, profile.Workspace)

	var response StreamMappingResponse
	if err := client.Post(url, reqBody, &response); err != nil {
		return fmt.Errorf("failed to create table-to-stream mapping: %w", err)
	}

	// Display success message
	green := color.New(color.FgGreen, color.Bold).SprintFunc()
	fmt.Printf("%s Table-to-stream mapping created successfully: %s\n", green("✓"), name)

	// Display mapping details
	fmt.Println("\nMapping Details:")
	fmt.Printf("  Mapping ID:   %s\n", response.Mapping.MappingID)
	fmt.Printf("  Name:         %s\n", response.Mapping.MappingName)
	fmt.Printf("  Type:         %s\n", response.Mapping.MappingType)
	fmt.Printf("  Source:       %s.%s\n", sourceDatabase, sourceTable)
	fmt.Printf("  Target:       %s/%s\n", targetIntegration, targetTopic)

	return nil
}

// AddStreamToStreamMapping creates a mapping from one stream topic to another
func AddStreamToStreamMapping(sourceIntegration, sourceTopic, targetIntegration, targetTopic, name, description, policyID string) error {
	// Get profile-aware HTTP client
	client, err := httpclient.GetProfileClient()
	if err != nil {
		return fmt.Errorf("failed to get HTTP client: %w", err)
	}

	// Get active profile to extract tenant and workspace
	profile, err := client.GetActiveProfile()
	if err != nil {
		return fmt.Errorf("failed to get active profile: %w", err)
	}

	// Generate name and description if not provided
	if name == "" {
		name = fmt.Sprintf("%s-%s-to-%s-%s", sourceIntegration, sourceTopic, targetIntegration, targetTopic)
	}

	if description == "" {
		description = fmt.Sprintf("Stream-to-stream mapping from %s/%s to %s/%s", sourceIntegration, sourceTopic, targetIntegration, targetTopic)
	}

	// Build request
	reqBody := StreamToStreamMappingRequest{
		MappingName:           name,
		MappingDescription:    description,
		SourceIntegrationName: sourceIntegration,
		SourceTopicName:       sourceTopic,
		TargetIntegrationName: targetIntegration,
		TargetTopicName:       targetTopic,
		PolicyID:              policyID,
	}

	// Make API call
	url := fmt.Sprintf("%s/%s/api/v1/workspaces/%s/mappings/stream-to-stream", profile.Hostname, profile.TenantURL, profile.Workspace)

	var response StreamMappingResponse
	if err := client.Post(url, reqBody, &response); err != nil {
		return fmt.Errorf("failed to create stream-to-stream mapping: %w", err)
	}

	// Display success message
	green := color.New(color.FgGreen, color.Bold).SprintFunc()
	fmt.Printf("%s Stream-to-stream mapping created successfully: %s\n", green("✓"), name)

	// Display mapping details
	fmt.Println("\nMapping Details:")
	fmt.Printf("  Mapping ID:   %s\n", response.Mapping.MappingID)
	fmt.Printf("  Name:         %s\n", response.Mapping.MappingName)
	fmt.Printf("  Type:         %s\n", response.Mapping.MappingType)
	fmt.Printf("  Source:       %s/%s\n", sourceIntegration, sourceTopic)
	fmt.Printf("  Target:       %s/%s\n", targetIntegration, targetTopic)

	return nil
}

// ParseStreamSource parses a stream source in the format integration:topic
func ParseStreamSource(source string) (integration, topic string, err error) {
	parts := strings.Split(source, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid stream source format, expected integration:topic, got: %s", source)
	}
	return parts[0], parts[1], nil
}
