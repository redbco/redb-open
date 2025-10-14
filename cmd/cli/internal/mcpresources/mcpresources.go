package mcpresources

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

type MCPResource struct {
	TenantID               string                 `json:"tenant_id"`
	WorkspaceID            string                 `json:"workspace_id"`
	MCPResourceID          string                 `json:"mcp_resource_id"`
	MCPResourceName        string                 `json:"mcp_resource_name"`
	MCPResourceDescription string                 `json:"mcp_resource_description"`
	MCPResourceConfig      map[string]interface{} `json:"mcp_resource_config"`
	MappingID              string                 `json:"mapping_id"`
	PolicyIDs              []string               `json:"policy_ids"`
	OwnerID                string                 `json:"owner_id"`
}

// ListMCPResources lists all MCP resources in the active workspace
func ListMCPResources() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcpresources")
	if err != nil {
		return err
	}

	var response struct {
		MCPResources []MCPResource `json:"mcp_resources"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list MCP resources: %v", err)
	}

	if len(response.MCPResources) == 0 {
		fmt.Println("No MCP resources found.")
		return nil
	}

	// Print header
	fmt.Printf("%-30s %-40s %-30s\n", "NAME", "DESCRIPTION", "MAPPING_ID")
	fmt.Println(strings.Repeat("-", 100))

	// Print each MCP resource
	for _, resource := range response.MCPResources {
		desc := resource.MCPResourceDescription
		if len(desc) > 40 {
			desc = desc[:37] + "..."
		}
		fmt.Printf("%-30s %-40s %-30s\n",
			resource.MCPResourceName,
			desc,
			resource.MappingID)
	}

	return nil
}

// ShowMCPResource shows details of a specific MCP resource
func ShowMCPResource(resourceName string) error {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return fmt.Errorf("resource name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcpresources/%s", resourceName))
	if err != nil {
		return err
	}

	var response struct {
		MCPResource MCPResource `json:"mcp_resource"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get MCP resource: %v", err)
	}

	resource := response.MCPResource

	// Display MCP resource details
	fmt.Println("MCP Resource Details:")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("ID:                  %s\n", resource.MCPResourceID)
	fmt.Printf("Name:                %s\n", resource.MCPResourceName)
	fmt.Printf("Description:         %s\n", resource.MCPResourceDescription)
	fmt.Printf("Mapping ID:          %s\n", resource.MappingID)
	fmt.Printf("Policy IDs:          %s\n", strings.Join(resource.PolicyIDs, ", "))
	fmt.Printf("Owner ID:            %s\n", resource.OwnerID)
	fmt.Printf("Tenant ID:           %s\n", resource.TenantID)
	fmt.Printf("Workspace ID:        %s\n", resource.WorkspaceID)

	// Print config
	if len(resource.MCPResourceConfig) > 0 {
		configJSON, _ := json.MarshalIndent(resource.MCPResourceConfig, "", "  ")
		fmt.Printf("Configuration:\n%s\n", string(configJSON))
	}

	return nil
}

// AddMCPResource creates a new MCP resource
func AddMCPResource(name, description, mapping, configStr string, policyIDs []string) error {
	name = strings.TrimSpace(name)
	description = strings.TrimSpace(description)
	mapping = strings.TrimSpace(mapping)

	if name == "" {
		return fmt.Errorf("resource name is required")
	}
	if mapping == "" {
		return fmt.Errorf("mapping name is required")
	}

	// Generate default description if not provided
	if description == "" {
		description = fmt.Sprintf("MCP resource '%s'", name)
	}

	// Generate default config if not provided
	var config map[string]interface{}
	if configStr == "" {
		// Default config for direct table access
		config = map[string]interface{}{
			"type": "direct_table",
		}
		fmt.Println("Using default config: direct_table")
	} else {
		// Parse provided config JSON
		if err := json.Unmarshal([]byte(configStr), &config); err != nil {
			return fmt.Errorf("invalid config JSON: %v", err)
		}
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcpresources")
	if err != nil {
		return err
	}

	resourceReq := struct {
		MCPResourceName        string                 `json:"mcp_resource_name"`
		MCPResourceDescription string                 `json:"mcp_resource_description"`
		MCPResourceConfig      map[string]interface{} `json:"mcp_resource_config"`
		MappingName            string                 `json:"mapping_name"`
		PolicyIDs              []string               `json:"policy_ids,omitempty"`
	}{
		MCPResourceName:        name,
		MCPResourceDescription: description,
		MCPResourceConfig:      config,
		MappingName:            mapping,
		PolicyIDs:              policyIDs,
	}

	var response struct {
		Message     string      `json:"message"`
		Success     bool        `json:"success"`
		MCPResource MCPResource `json:"mcp_resource"`
		Status      string      `json:"status"`
	}
	if err := client.Post(url, resourceReq, &response); err != nil {
		return fmt.Errorf("failed to create MCP resource: %v", err)
	}

	fmt.Printf("Successfully created MCP resource '%s' (ID: %s)\n", response.MCPResource.MCPResourceName, response.MCPResource.MCPResourceID)
	return nil
}

// AttachMCPResource attaches a resource to an MCP server
func AttachMCPResource(resourceName, serverName string) error {
	resourceName = strings.TrimSpace(resourceName)
	serverName = strings.TrimSpace(serverName)

	if resourceName == "" {
		return fmt.Errorf("resource name is required")
	}
	if serverName == "" {
		return fmt.Errorf("server name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcpresources/attach")
	if err != nil {
		return err
	}

	attachReq := struct {
		MCPResourceName string `json:"mcp_resource_name"`
		MCPServerName   string `json:"mcp_server_name"`
	}{
		MCPResourceName: resourceName,
		MCPServerName:   serverName,
	}

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, attachReq, &response); err != nil {
		return fmt.Errorf("failed to attach MCP resource: %v", err)
	}

	fmt.Printf("Successfully attached MCP resource '%s' to server '%s'\n", resourceName, serverName)
	return nil
}

// DetachMCPResource detaches a resource from an MCP server
func DetachMCPResource(resourceName, serverName string) error {
	resourceName = strings.TrimSpace(resourceName)
	serverName = strings.TrimSpace(serverName)

	if resourceName == "" {
		return fmt.Errorf("resource name is required")
	}
	if serverName == "" {
		return fmt.Errorf("server name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcpresources/detach")
	if err != nil {
		return err
	}

	detachReq := struct {
		MCPResourceName string `json:"mcp_resource_name"`
		MCPServerName   string `json:"mcp_server_name"`
	}{
		MCPResourceName: resourceName,
		MCPServerName:   serverName,
	}

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, detachReq, &response); err != nil {
		return fmt.Errorf("failed to detach MCP resource: %v", err)
	}

	fmt.Printf("Successfully detached MCP resource '%s' from server '%s'\n", resourceName, serverName)
	return nil
}

// DeleteMCPResource deletes an MCP resource
func DeleteMCPResource(resourceName string) error {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return fmt.Errorf("resource name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcpresources/%s", resourceName))
	if err != nil {
		return err
	}

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to delete MCP resource: %v", err)
	}

	fmt.Printf("Successfully deleted MCP resource '%s'\n", resourceName)
	return nil
}
