package mcptools

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

type MCPTool struct {
	TenantID           string                 `json:"tenant_id"`
	WorkspaceID        string                 `json:"workspace_id"`
	MCPToolID          string                 `json:"mcp_tool_id"`
	MCPToolName        string                 `json:"mcp_tool_name"`
	MCPToolDescription string                 `json:"mcp_tool_description"`
	MCPToolConfig      map[string]interface{} `json:"mcp_tool_config"`
	MappingID          string                 `json:"mapping_id"`
	PolicyIDs          []string               `json:"policy_ids"`
	OwnerID            string                 `json:"owner_id"`
}

// ListMCPTools lists all MCP tools in the active workspace
func ListMCPTools() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcptools")
	if err != nil {
		return err
	}

	var response struct {
		MCPTools []MCPTool `json:"mcp_tools"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list MCP tools: %v", err)
	}

	if len(response.MCPTools) == 0 {
		fmt.Println("No MCP tools found.")
		return nil
	}

	// Print header
	fmt.Printf("%-30s %-40s %-30s\n", "NAME", "DESCRIPTION", "MAPPING_ID")
	fmt.Println(strings.Repeat("-", 100))

	// Print each MCP tool
	for _, tool := range response.MCPTools {
		desc := tool.MCPToolDescription
		if len(desc) > 40 {
			desc = desc[:37] + "..."
		}
		fmt.Printf("%-30s %-40s %-30s\n",
			tool.MCPToolName,
			desc,
			tool.MappingID)
	}

	return nil
}

// ShowMCPTool shows details of a specific MCP tool
func ShowMCPTool(toolName string) error {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return fmt.Errorf("tool name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcptools/%s", toolName))
	if err != nil {
		return err
	}

	var response struct {
		MCPTool MCPTool `json:"mcp_tool"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get MCP tool: %v", err)
	}

	tool := response.MCPTool

	// Display MCP tool details
	fmt.Println("MCP Tool Details:")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("ID:                  %s\n", tool.MCPToolID)
	fmt.Printf("Name:                %s\n", tool.MCPToolName)
	fmt.Printf("Description:         %s\n", tool.MCPToolDescription)
	fmt.Printf("Mapping ID:          %s\n", tool.MappingID)
	fmt.Printf("Policy IDs:          %s\n", strings.Join(tool.PolicyIDs, ", "))
	fmt.Printf("Owner ID:            %s\n", tool.OwnerID)
	fmt.Printf("Tenant ID:           %s\n", tool.TenantID)
	fmt.Printf("Workspace ID:        %s\n", tool.WorkspaceID)

	// Print config
	if len(tool.MCPToolConfig) > 0 {
		configJSON, _ := json.MarshalIndent(tool.MCPToolConfig, "", "  ")
		fmt.Printf("Configuration:\n%s\n", string(configJSON))
	}

	return nil
}

// AddMCPTool creates a new MCP tool
func AddMCPTool(name, description, mapping, configStr string, policyIDs []string) error {
	name = strings.TrimSpace(name)
	description = strings.TrimSpace(description)
	mapping = strings.TrimSpace(mapping)

	if name == "" {
		return fmt.Errorf("tool name is required")
	}
	if mapping == "" {
		return fmt.Errorf("mapping name is required")
	}

	// Generate default description if not provided
	if description == "" {
		description = fmt.Sprintf("MCP tool '%s'", name)
	}

	// Generate default config if not provided
	var config map[string]interface{}
	if configStr == "" {
		// Default config for query operation
		config = map[string]interface{}{
			"operation": "query_database",
			"input_schema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"query": map[string]interface{}{
						"type": "string",
					},
				},
				"required": []string{"query"},
			},
		}
		fmt.Println("Using default config: query_database")
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

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcptools")
	if err != nil {
		return err
	}

	toolReq := struct {
		MCPToolName        string                 `json:"mcp_tool_name"`
		MCPToolDescription string                 `json:"mcp_tool_description"`
		MCPToolConfig      map[string]interface{} `json:"mcp_tool_config"`
		MappingName        string                 `json:"mapping_name"`
		PolicyIDs          []string               `json:"policy_ids,omitempty"`
	}{
		MCPToolName:        name,
		MCPToolDescription: description,
		MCPToolConfig:      config,
		MappingName:        mapping,
		PolicyIDs:          policyIDs,
	}

	var response struct {
		Message string  `json:"message"`
		Success bool    `json:"success"`
		MCPTool MCPTool `json:"mcp_tool"`
		Status  string  `json:"status"`
	}
	if err := client.Post(url, toolReq, &response); err != nil {
		return fmt.Errorf("failed to create MCP tool: %v", err)
	}

	fmt.Printf("Successfully created MCP tool '%s' (ID: %s)\n", response.MCPTool.MCPToolName, response.MCPTool.MCPToolID)
	return nil
}

// AttachMCPTool attaches a tool to an MCP server
func AttachMCPTool(toolName, serverName string) error {
	toolName = strings.TrimSpace(toolName)
	serverName = strings.TrimSpace(serverName)

	if toolName == "" {
		return fmt.Errorf("tool name is required")
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

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcptools/attach")
	if err != nil {
		return err
	}

	attachReq := struct {
		MCPToolName   string `json:"mcp_tool_name"`
		MCPServerName string `json:"mcp_server_name"`
	}{
		MCPToolName:   toolName,
		MCPServerName: serverName,
	}

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, attachReq, &response); err != nil {
		return fmt.Errorf("failed to attach MCP tool: %v", err)
	}

	fmt.Printf("Successfully attached MCP tool '%s' to server '%s'\n", toolName, serverName)
	return nil
}

// DetachMCPTool detaches a tool from an MCP server
func DetachMCPTool(toolName, serverName string) error {
	toolName = strings.TrimSpace(toolName)
	serverName = strings.TrimSpace(serverName)

	if toolName == "" {
		return fmt.Errorf("tool name is required")
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

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcptools/detach")
	if err != nil {
		return err
	}

	detachReq := struct {
		MCPToolName   string `json:"mcp_tool_name"`
		MCPServerName string `json:"mcp_server_name"`
	}{
		MCPToolName:   toolName,
		MCPServerName: serverName,
	}

	var response struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, detachReq, &response); err != nil {
		return fmt.Errorf("failed to detach MCP tool: %v", err)
	}

	fmt.Printf("Successfully detached MCP tool '%s' from server '%s'\n", toolName, serverName)
	return nil
}

// DeleteMCPTool deletes an MCP tool
func DeleteMCPTool(toolName string) error {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return fmt.Errorf("tool name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcptools/%s", toolName))
	if err != nil {
		return err
	}

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to delete MCP tool: %v", err)
	}

	fmt.Printf("Successfully deleted MCP tool '%s'\n", toolName)
	return nil
}
