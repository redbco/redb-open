package mcpservers

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

type MCPServer struct {
	TenantID             string   `json:"tenant_id"`
	WorkspaceID          string   `json:"workspace_id"`
	MCPServerID          string   `json:"mcp_server_id"`
	MCPServerName        string   `json:"mcp_server_name"`
	MCPServerDescription string   `json:"mcp_server_description"`
	MCPServerHostIDs     []string `json:"mcp_server_host_ids"`
	MCPServerPort        int32    `json:"mcp_server_port"`
	MCPServerEnabled     bool     `json:"mcp_server_enabled"`
	PolicyIDs            []string `json:"policy_ids"`
	OwnerID              string   `json:"owner_id"`
	StatusMessage        string   `json:"status_message"`
	Status               string   `json:"status"`
}

// ListMCPServers lists all MCP servers in the active workspace
func ListMCPServers() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcpservers")
	if err != nil {
		return err
	}

	var response struct {
		MCPServers []MCPServer `json:"mcp_servers"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to list MCP servers: %v", err)
	}

	if len(response.MCPServers) == 0 {
		fmt.Println("No MCP servers found.")
		return nil
	}

	// Print header
	fmt.Printf("%-30s %-10s %-10s %-30s %-15s\n", "NAME", "PORT", "ENABLED", "NODES", "STATUS")
	fmt.Println(strings.Repeat("-", 105))

	// Print each MCP server
	for _, server := range response.MCPServers {
		enabled := "false"
		if server.MCPServerEnabled {
			enabled = "true"
		}
		nodes := strings.Join(server.MCPServerHostIDs, ",")
		if len(nodes) > 30 {
			nodes = nodes[:27] + "..."
		}
		fmt.Printf("%-30s %-10d %-10s %-30s %-15s\n",
			server.MCPServerName,
			server.MCPServerPort,
			enabled,
			nodes,
			server.Status)
	}

	return nil
}

// ShowMCPServer shows details of a specific MCP server
func ShowMCPServer(serverName string) error {
	serverName = strings.TrimSpace(serverName)
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

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcpservers/%s", serverName))
	if err != nil {
		return err
	}

	var response struct {
		MCPServer MCPServer `json:"mcp_server"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get MCP server: %v", err)
	}

	server := response.MCPServer

	// Display MCP server details
	fmt.Println("MCP Server Details:")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("ID:                  %s\n", server.MCPServerID)
	fmt.Printf("Name:                %s\n", server.MCPServerName)
	fmt.Printf("Description:         %s\n", server.MCPServerDescription)
	fmt.Printf("Port:                %d\n", server.MCPServerPort)
	fmt.Printf("Enabled:             %t\n", server.MCPServerEnabled)
	fmt.Printf("Host Node IDs:       %s\n", strings.Join(server.MCPServerHostIDs, ", "))
	fmt.Printf("Policy IDs:          %s\n", strings.Join(server.PolicyIDs, ", "))
	fmt.Printf("Owner ID:            %s\n", server.OwnerID)
	fmt.Printf("Status:              %s\n", server.Status)
	fmt.Printf("Status Message:      %s\n", server.StatusMessage)
	fmt.Printf("Tenant ID:           %s\n", server.TenantID)
	fmt.Printf("Workspace ID:        %s\n", server.WorkspaceID)

	return nil
}

// AddMCPServer creates a new MCP server
func AddMCPServer(name, description string, port int32, nodes []string, enabled bool, policyIDs []string) error {
	name = strings.TrimSpace(name)
	description = strings.TrimSpace(description)

	if name == "" {
		return fmt.Errorf("server name is required")
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	// Generate default description if not provided
	if description == "" {
		description = fmt.Sprintf("MCP server '%s' on port %d", name, port)
	}

	// If nodes not specified, send empty array
	// The server will automatically use the local node
	if len(nodes) == 0 {
		fmt.Println("Using local node (will be determined by server)")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/mcpservers")
	if err != nil {
		return err
	}

	serverReq := struct {
		MCPServerName        string   `json:"mcp_server_name"`
		MCPServerDescription string   `json:"mcp_server_description"`
		MCPServerPort        int32    `json:"mcp_server_port"`
		MCPServerHostIDs     []string `json:"mcp_server_host_ids"`
		MCPServerEnabled     bool     `json:"mcp_server_enabled"`
		PolicyIDs            []string `json:"policy_ids,omitempty"`
	}{
		MCPServerName:        name,
		MCPServerDescription: description,
		MCPServerPort:        port,
		MCPServerHostIDs:     nodes,
		MCPServerEnabled:     enabled,
		PolicyIDs:            policyIDs,
	}

	var response struct {
		Message   string    `json:"message"`
		Success   bool      `json:"success"`
		MCPServer MCPServer `json:"mcp_server"`
		Status    string    `json:"status"`
	}
	if err := client.Post(url, serverReq, &response); err != nil {
		return fmt.Errorf("failed to create MCP server: %v", err)
	}

	fmt.Printf("Successfully created MCP server '%s' (ID: %s)\n", response.MCPServer.MCPServerName, response.MCPServer.MCPServerID)
	fmt.Printf("Port: %d, Enabled: %t\n", response.MCPServer.MCPServerPort, response.MCPServer.MCPServerEnabled)
	return nil
}

// ModifyMCPServer modifies an existing MCP server
func ModifyMCPServer(serverName, description string, port int32, nodes []string, enabled bool, policyIDs []string) error {
	serverName = strings.TrimSpace(serverName)
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

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcpservers/%s", serverName))
	if err != nil {
		return err
	}

	serverReq := make(map[string]interface{})
	if description != "" {
		serverReq["mcp_server_description"] = description
	}
	if port > 0 {
		serverReq["mcp_server_port"] = port
	}
	if len(nodes) > 0 {
		serverReq["mcp_server_host_ids"] = nodes
	}
	serverReq["mcp_server_enabled"] = enabled
	if len(policyIDs) > 0 {
		serverReq["policy_ids"] = policyIDs
	}

	var response struct {
		Message   string    `json:"message"`
		Success   bool      `json:"success"`
		MCPServer MCPServer `json:"mcp_server"`
		Status    string    `json:"status"`
	}
	if err := client.Put(url, serverReq, &response); err != nil {
		return fmt.Errorf("failed to modify MCP server: %v", err)
	}

	fmt.Printf("Successfully modified MCP server '%s'\n", response.MCPServer.MCPServerName)
	return nil
}

// DeleteMCPServer deletes an MCP server
func DeleteMCPServer(serverName string) error {
	serverName = strings.TrimSpace(serverName)
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

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/mcpservers/%s", serverName))
	if err != nil {
		return err
	}

	if err := client.Delete(url); err != nil {
		return fmt.Errorf("failed to delete MCP server: %v", err)
	}

	fmt.Printf("Successfully deleted MCP server '%s'\n", serverName)
	return nil
}
