package instances

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"golang.org/x/term"
)

type Instance struct {
	TenantID                 string   `json:"tenant_id"`
	WorkspaceID              string   `json:"workspace_id"`
	EnvironmentID            string   `json:"environment_id"`
	ID                       string   `json:"instance_id"`
	InstanceName             string   `json:"instance_name"`
	InstanceDescription      string   `json:"instance_description"`
	InstanceType             string   `json:"instance_type"`
	InstanceVendor           string   `json:"instance_vendor"`
	InstanceVersion          string   `json:"instance_version"`
	InstanceUniqueIdentifier string   `json:"instance_unique_identifier"`
	ConnectedToNodeID        string   `json:"connected_to_node_id"`
	InstanceHost             string   `json:"instance_host"`
	InstancePort             int32    `json:"instance_port"`
	InstanceUsername         string   `json:"instance_username"`
	InstancePassword         string   `json:"instance_password"`
	InstanceSystemDBName     string   `json:"instance_system_db_name"`
	InstanceEnabled          bool     `json:"instance_enabled"`
	InstanceSSL              bool     `json:"instance_ssl"`
	InstanceSSLMode          string   `json:"instance_ssl_mode"`
	InstanceSSLCert          string   `json:"instance_ssl_cert"`
	InstanceSSLKey           string   `json:"instance_ssl_key"`
	InstanceSSLRootCert      string   `json:"instance_ssl_root_cert"`
	PolicyIDs                []string `json:"policy_ids"`
	OwnerID                  string   `json:"owner_id"`
	InstanceStatusMessage    string   `json:"instance_status_message"`
	Status                   string   `json:"status"`
	Created                  string   `json:"created"`
	Updated                  string   `json:"updated"`
}

// InstancesResponse wraps the API response for listing instances
type InstancesResponse struct {
	Instances []Instance `json:"instances"`
}

// InstanceResponse wraps the API response for a single instance
type InstanceResponse struct {
	Instance Instance `json:"instance"`
}

// ConnectInstanceResponse wraps the API response for connecting an instance
type ConnectInstanceResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Instance Instance `json:"instance"`
	Status   string   `json:"status"`
}

type ConnectInstanceRequest struct {
	InstanceName        string `json:"instance_name"`
	InstanceDescription string `json:"instance_description,omitempty"`
	InstanceType        string `json:"instance_type"`
	InstanceHost        string `json:"host"`
	InstancePort        int    `json:"port"`
	InstanceUsername    string `json:"username"`
	InstancePassword    string `json:"password"`
	ConnectedToNodeID   string `json:"node_id"`
	InstanceEnabled     bool   `json:"enabled"`
	InstanceSSL         bool   `json:"ssl"`
	InstanceSSLMode     string `json:"ssl_mode"`
	InstanceSSLCert     string `json:"ssl_cert,omitempty"`
	InstanceSSLKey      string `json:"ssl_key,omitempty"`
	InstanceSSLRootCert string `json:"ssl_root_cert,omitempty"`
}

// ReconnectInstanceResponse wraps the API response for reconnecting an instance
type ReconnectInstanceResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

// UpdateInstanceResponse wraps the API response for updating an instance
type UpdateInstanceResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Instance Instance `json:"instance"`
	Status   string   `json:"status"`
}

// DisconnectInstanceResponse wraps the API response for disconnecting an instance
type DisconnectInstanceResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

type UpdateInstanceRequest struct {
	InstanceNameNew     string `json:"instance_name_new,omitempty"`
	InstanceDescription string `json:"instance_description,omitempty"`
	InstanceType        string `json:"instance_type,omitempty"`
	InstanceVendor      string `json:"instance_vendor,omitempty"`
	InstanceHost        string `json:"host,omitempty"`
	InstancePort        int    `json:"port,omitempty"`
	InstanceUsername    string `json:"username,omitempty"`
	InstancePassword    string `json:"password,omitempty"`
	ConnectedToNodeID   string `json:"node_id,omitempty"`
	InstanceEnabled     bool   `json:"enabled,omitempty"`
	InstanceSSL         bool   `json:"ssl,omitempty"`
	InstanceSSLMode     string `json:"ssl_mode,omitempty"`
	InstanceSSLCert     string `json:"ssl_cert,omitempty"`
	InstanceSSLKey      string `json:"ssl_key,omitempty"`
	InstanceSSLRootCert string `json:"ssl_root_cert,omitempty"`
	EnvironmentID       string `json:"environment_id,omitempty"`
}

type DisconnectInstanceRequest struct {
	DeleteInstance bool `json:"delete_instance,omitempty"`
}

// ListInstances lists all instances
func ListInstances() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/instances", tenantURL, workspaceName)

	var instancesResponse InstancesResponse
	if err := client.Get(url, &instancesResponse, true); err != nil {
		return fmt.Errorf("failed to list instances: %v", err)
	}

	if len(instancesResponse.Instances) == 0 {
		fmt.Println("No instances found.")
		return nil
	}

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()

	// Print header
	fmt.Fprintln(w, "Name\tType\tHost\tPort\tStatus\tEnabled")
	fmt.Fprintln(w, "----\t----\t----\t----\t------\t-------")

	// Print each instance
	for _, instance := range instancesResponse.Instances {
		enabled := "Yes"
		if !instance.InstanceEnabled {
			enabled = "No"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
			instance.InstanceName,
			instance.InstanceType,
			instance.InstanceHost,
			instance.InstancePort,
			instance.Status,
			enabled)
	}

	w.Flush()
	fmt.Println()
	return nil
}

// ShowInstance displays details of a specific instance
func ShowInstance(instanceName string) error {
	// Trim whitespace from instance name
	instanceName = strings.TrimSpace(instanceName)
	if instanceName == "" {
		return fmt.Errorf("instance name is required")
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/instances/%s", tenantURL, workspaceName, instanceName)

	var instanceResponse InstanceResponse
	if err := client.Get(url, &instanceResponse, true); err != nil {
		return fmt.Errorf("failed to get instance details: %v", err)
	}

	instance := instanceResponse.Instance

	// Print instance details
	fmt.Println()
	fmt.Printf("Instance Details for '%s'\n", instance.InstanceName)
	fmt.Println(strings.Repeat("=", 50))

	fmt.Printf("ID:                    %s\n", instance.ID)
	fmt.Printf("Name:                  %s\n", instance.InstanceName)
	fmt.Printf("Description:           %s\n", instance.InstanceDescription)
	fmt.Printf("Type:                  %s\n", instance.InstanceType)
	fmt.Printf("Vendor:                %s\n", instance.InstanceVendor)
	fmt.Printf("Version:               %s\n", instance.InstanceVersion)
	fmt.Printf("Unique Identifier:     %s\n", instance.InstanceUniqueIdentifier)
	fmt.Printf("Connected to Node ID:  %s\n", instance.ConnectedToNodeID)
	fmt.Printf("Host:                  %s\n", instance.InstanceHost)
	fmt.Printf("Port:                  %d\n", instance.InstancePort)
	fmt.Printf("Username:              %s\n", instance.InstanceUsername)
	fmt.Printf("System DB Name:        %s\n", instance.InstanceSystemDBName)
	fmt.Printf("Enabled:               %t\n", instance.InstanceEnabled)
	fmt.Printf("SSL:                   %t\n", instance.InstanceSSL)
	fmt.Printf("SSL Mode:              %s\n", instance.InstanceSSLMode)
	fmt.Printf("Status:                %s\n", instance.Status)
	fmt.Printf("Status Message:        %s\n", instance.InstanceStatusMessage)
	fmt.Printf("Owner ID:              %s\n", instance.OwnerID)
	fmt.Printf("Tenant ID:             %s\n", instance.TenantID)
	fmt.Printf("Workspace ID:          %s\n", instance.WorkspaceID)
	if instance.EnvironmentID != "" {
		fmt.Printf("Environment ID:        %s\n", instance.EnvironmentID)
	}

	// Print SSL certificates if they exist
	if instance.InstanceSSLCert != "" {
		fmt.Printf("SSL Certificate:       %s\n", instance.InstanceSSLCert)
	}
	if instance.InstanceSSLKey != "" {
		fmt.Printf("SSL Key:               %s\n", instance.InstanceSSLKey)
	}
	if instance.InstanceSSLRootCert != "" {
		fmt.Printf("SSL Root Certificate:  %s\n", instance.InstanceSSLRootCert)
	}

	// Print policy IDs if they exist
	if len(instance.PolicyIDs) > 0 {
		fmt.Printf("Policy IDs:            %s\n", strings.Join(instance.PolicyIDs, ", "))
	}

	fmt.Printf("Created:               %s\n", instance.Created)
	fmt.Printf("Updated:               %s\n", instance.Updated)
	fmt.Println()

	return nil
}

// ConnectInstance connects a new instance
func ConnectInstance(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get instance name
	var instanceName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		instanceName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Instance Name: ")
		instanceName, _ = reader.ReadString('\n')
		instanceName = strings.TrimSpace(instanceName)
	}

	if instanceName == "" {
		return fmt.Errorf("instance name is required")
	}

	// Get instance description
	var instanceDescription string
	if len(args) > 1 && strings.HasPrefix(args[1], "--description=") {
		instanceDescription = strings.TrimPrefix(args[1], "--description=")
	} else {
		fmt.Print("Instance Description (optional): ")
		instanceDescription, _ = reader.ReadString('\n')
		instanceDescription = strings.TrimSpace(instanceDescription)
	}

	// Get instance type
	var instanceType string
	if len(args) > 2 && strings.HasPrefix(args[2], "--type=") {
		instanceType = strings.TrimPrefix(args[2], "--type=")
	} else {
		fmt.Print("Instance Type (e.g., postgres, mysql, mongodb): ")
		instanceType, _ = reader.ReadString('\n')
		instanceType = strings.TrimSpace(instanceType)
	}

	if instanceType == "" {
		return fmt.Errorf("instance type is required")
	}

	// Get host
	var host string
	if len(args) > 3 && strings.HasPrefix(args[3], "--host=") {
		host = strings.TrimPrefix(args[3], "--host=")
	} else {
		fmt.Print("Host: ")
		host, _ = reader.ReadString('\n')
		host = strings.TrimSpace(host)
	}

	if host == "" {
		return fmt.Errorf("host is required")
	}

	// Get port
	var portStr string
	if len(args) > 4 && strings.HasPrefix(args[4], "--port=") {
		portStr = strings.TrimPrefix(args[4], "--port=")
	} else {
		fmt.Print("Port: ")
		portStr, _ = reader.ReadString('\n')
		portStr = strings.TrimSpace(portStr)
	}

	if portStr == "" {
		return fmt.Errorf("port is required")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port. Must be an integer")
	}

	// Get username
	var username string
	if len(args) > 5 && strings.HasPrefix(args[5], "--username=") {
		username = strings.TrimPrefix(args[5], "--username=")
	} else {
		fmt.Print("Username: ")
		username, _ = reader.ReadString('\n')
		username = strings.TrimSpace(username)
	}

	if username == "" {
		return fmt.Errorf("username is required")
	}

	// Get password with masking
	var password string
	if len(args) > 6 && strings.HasPrefix(args[6], "--password=") {
		password = strings.TrimPrefix(args[6], "--password=")
	} else {
		fmt.Print("Password: ")
		password, err = readPassword()
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
	}

	if password == "" {
		return fmt.Errorf("password is required")
	}

	// Get node ID (optional)
	var nodeID string
	if len(args) > 7 && strings.HasPrefix(args[7], "--node-id=") {
		nodeID = strings.TrimPrefix(args[7], "--node-id=")
	}

	// Get enabled status
	var enabledStr string
	if len(args) > 8 && strings.HasPrefix(args[8], "--enabled=") {
		enabledStr = strings.TrimPrefix(args[8], "--enabled=")
	} else {
		fmt.Print("Enabled (true/false): ")
		enabledStr, _ = reader.ReadString('\n')
		enabledStr = strings.TrimSpace(enabledStr)
	}

	if enabledStr == "" {
		return fmt.Errorf("enabled status is required")
	}

	if enabledStr != "true" && enabledStr != "false" {
		return fmt.Errorf("invalid enabled status. Must be one of: true, false")
	}

	enabled := enabledStr == "true"

	// Get SSL status
	var sslStr string
	if len(args) > 9 && strings.HasPrefix(args[9], "--ssl=") {
		sslStr = strings.TrimPrefix(args[9], "--ssl=")
	} else {
		fmt.Print("SSL (true/false): ")
		sslStr, _ = reader.ReadString('\n')
		sslStr = strings.TrimSpace(sslStr)
	}

	if sslStr == "" {
		return fmt.Errorf("SSL status is required")
	}

	if sslStr != "true" && sslStr != "false" {
		return fmt.Errorf("invalid SSL status. Must be one of: true, false")
	}

	ssl := sslStr == "true"

	// Get SSL mode
	var sslMode string
	if len(args) > 10 && strings.HasPrefix(args[10], "--ssl-mode=") {
		sslMode = strings.TrimPrefix(args[10], "--ssl-mode=")
	} else {
		if ssl {
			fmt.Print("SSL Mode (require, prefer, disable): ")
			sslMode, _ = reader.ReadString('\n')
			sslMode = strings.TrimSpace(sslMode)
		} else {
			// Automatically set SSL mode to disable when SSL is false
			sslMode = "disable"
		}
	}

	if sslMode == "" {
		return fmt.Errorf("SSL mode is required")
	}

	if sslMode != "require" && sslMode != "prefer" && sslMode != "disable" {
		return fmt.Errorf("invalid SSL mode. Must be one of: require, prefer, disable")
	}

	// Get SSL certificates if SSL mode is not disable
	var sslCert, sslKey, sslRootCert string
	if sslMode != "disable" {
		if len(args) > 11 && strings.HasPrefix(args[11], "--ssl-cert=") {
			sslCert = strings.TrimPrefix(args[11], "--ssl-cert=")
		} else {
			fmt.Print("SSL Certificate (optional): ")
			sslCert, _ = reader.ReadString('\n')
			sslCert = strings.TrimSpace(sslCert)
		}

		if len(args) > 12 && strings.HasPrefix(args[12], "--ssl-key=") {
			sslKey = strings.TrimPrefix(args[12], "--ssl-key=")
		} else {
			fmt.Print("SSL Private Key (optional): ")
			sslKey, _ = reader.ReadString('\n')
			sslKey = strings.TrimSpace(sslKey)
		}

		if len(args) > 13 && strings.HasPrefix(args[13], "--ssl-root-cert=") {
			sslRootCert = strings.TrimPrefix(args[13], "--ssl-root-cert=")
		} else {
			fmt.Print("SSL Root Certificate (optional): ")
			sslRootCert, _ = reader.ReadString('\n')
			sslRootCert = strings.TrimSpace(sslRootCert)
		}
	}

	// Create the instance connection request
	connectReq := ConnectInstanceRequest{
		InstanceName:        instanceName,
		InstanceDescription: instanceDescription,
		InstanceType:        instanceType,
		InstanceHost:        host,
		InstancePort:        port,
		InstanceUsername:    username,
		InstancePassword:    password,
		ConnectedToNodeID:   nodeID,
		InstanceEnabled:     enabled,
		InstanceSSL:         ssl,
		InstanceSSLMode:     sslMode,
		InstanceSSLCert:     sslCert,
		InstanceSSLKey:      sslKey,
		InstanceSSLRootCert: sslRootCert,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err = config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/instances/connect", tenantURL, workspaceName)

	var connectResponse ConnectInstanceResponse
	if err := client.Post(url, connectReq, &connectResponse, true); err != nil {
		return fmt.Errorf("failed to connect instance: %v", err)
	}

	fmt.Printf("Successfully connected instance '%s' (ID: %s)\n", connectResponse.Instance.InstanceName, connectResponse.Instance.ID)
	return nil
}

// ReconnectInstance reconnects an existing instance
func ReconnectInstance(instanceName string, args []string) error {
	// Trim whitespace from instance name
	instanceName = strings.TrimSpace(instanceName)
	if instanceName == "" {
		return fmt.Errorf("instance name is required")
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/instances/%s/reconnect", tenantURL, workspaceName, instanceName)

	var response ReconnectInstanceResponse
	if err := client.Post(url, nil, &response, true); err != nil {
		return fmt.Errorf("failed to reconnect instance: %v", err)
	}

	fmt.Printf("Successfully reconnected instance '%s'\n", instanceName)
	return nil
}

// ModifyInstance updates an existing instance
func ModifyInstance(instanceName string, args []string) error {
	// Trim whitespace from instance name
	instanceName = strings.TrimSpace(instanceName)
	if instanceName == "" {
		return fmt.Errorf("instance name is required")
	}

	// First find the instance to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/instances/%s", tenantURL, workspaceName, instanceName)

	fmt.Println()

	var response InstanceResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get instance: %v", err)
	}

	targetInstance := response.Instance

	reader := bufio.NewReader(os.Stdin)
	updateReq := UpdateInstanceRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		if strings.HasPrefix(arg, "--name=") {
			updateReq.InstanceNameNew = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--description=") {
			updateReq.InstanceDescription = strings.TrimPrefix(arg, "--description=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--type=") {
			updateReq.InstanceType = strings.TrimPrefix(arg, "--type=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--host=") {
			updateReq.InstanceHost = strings.TrimPrefix(arg, "--host=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--port=") {
			portInt, err := strconv.Atoi(strings.TrimPrefix(arg, "--port="))
			if err != nil {
				return fmt.Errorf("invalid port. Must be an integer")
			}
			updateReq.InstancePort = portInt
			hasChanges = true
		} else if strings.HasPrefix(arg, "--username=") {
			updateReq.InstanceUsername = strings.TrimPrefix(arg, "--username=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--password=") {
			updateReq.InstancePassword = strings.TrimPrefix(arg, "--password=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--node-id=") {
			updateReq.ConnectedToNodeID = strings.TrimPrefix(arg, "--node-id=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--ssl=") {
			updateReq.InstanceSSL = strings.TrimPrefix(arg, "--ssl=") == "true"
			hasChanges = true
		} else if strings.HasPrefix(arg, "--ssl-mode=") {
			updateReq.InstanceSSLMode = strings.TrimPrefix(arg, "--ssl-mode=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--ssl-cert=") {
			updateReq.InstanceSSLCert = strings.TrimPrefix(arg, "--ssl-cert=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--ssl-key=") {
			updateReq.InstanceSSLKey = strings.TrimPrefix(arg, "--ssl-key=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--ssl-root-cert=") {
			updateReq.InstanceSSLRootCert = strings.TrimPrefix(arg, "--ssl-root-cert=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--environment-id=") {
			updateReq.EnvironmentID = strings.TrimPrefix(arg, "--environment-id=")
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying instance '%s' (press Enter to keep current value):\n", instanceName)

		fmt.Printf("Name [%s]: ", targetInstance.InstanceName)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.InstanceNameNew = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetInstance.InstanceDescription)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq.InstanceDescription = newDescription
			hasChanges = true
		}

		fmt.Printf("Type [%s]: ", targetInstance.InstanceType)
		newType, _ := reader.ReadString('\n')
		newType = strings.TrimSpace(newType)
		if newType != "" {
			updateReq.InstanceType = newType
			hasChanges = true
		}

		fmt.Printf("Host [%s]: ", targetInstance.InstanceHost)
		newHost, _ := reader.ReadString('\n')
		newHost = strings.TrimSpace(newHost)
		if newHost != "" {
			updateReq.InstanceHost = newHost
			hasChanges = true
		}

		fmt.Printf("Port [%d]: ", targetInstance.InstancePort)
		newPort, _ := reader.ReadString('\n')
		newPort = strings.TrimSpace(newPort)
		if newPort != "" {
			portInt, err := strconv.Atoi(newPort)
			if err != nil {
				return fmt.Errorf("invalid port. Must be an integer")
			}
			updateReq.InstancePort = portInt
			hasChanges = true
		}

		fmt.Printf("Username [%s]: ", targetInstance.InstanceUsername)
		newUsername, _ := reader.ReadString('\n')
		newUsername = strings.TrimSpace(newUsername)
		if newUsername != "" {
			updateReq.InstanceUsername = newUsername
			hasChanges = true
		}

		fmt.Print("Password (leave blank to keep current): ")
		newPassword, err := readPassword()
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		if newPassword != "" {
			updateReq.InstancePassword = newPassword
			hasChanges = true
		}

		fmt.Printf("Connected to Node ID [%s]: ", targetInstance.ConnectedToNodeID)
		newNodeID, _ := reader.ReadString('\n')
		newNodeID = strings.TrimSpace(newNodeID)
		if newNodeID != "" {
			updateReq.ConnectedToNodeID = newNodeID
			hasChanges = true
		}

		fmt.Printf("SSL [%t]: ", targetInstance.InstanceSSL)
		newSSL, _ := reader.ReadString('\n')
		newSSL = strings.TrimSpace(newSSL)
		currentSSL := targetInstance.InstanceSSL
		if newSSL != "" {
			currentSSL = newSSL == "true"
			updateReq.InstanceSSL = currentSSL
			hasChanges = true
		}

		// Only prompt for SSL details if SSL is enabled
		if currentSSL {
			fmt.Printf("SSL Mode [%s]: ", targetInstance.InstanceSSLMode)
			newSSLMode, _ := reader.ReadString('\n')
			newSSLMode = strings.TrimSpace(newSSLMode)
			if newSSLMode != "" {
				updateReq.InstanceSSLMode = newSSLMode
				hasChanges = true
			}

			fmt.Printf("SSL Certificate [%s]: ", targetInstance.InstanceSSLCert)
			newSSLCert, _ := reader.ReadString('\n')
			newSSLCert = strings.TrimSpace(newSSLCert)
			if newSSLCert != "" {
				updateReq.InstanceSSLCert = newSSLCert
				hasChanges = true
			}

			fmt.Printf("SSL Key [%s]: ", targetInstance.InstanceSSLKey)
			newSSLKey, _ := reader.ReadString('\n')
			newSSLKey = strings.TrimSpace(newSSLKey)
			if newSSLKey != "" {
				updateReq.InstanceSSLKey = newSSLKey
				hasChanges = true
			}

			fmt.Printf("SSL Root Certificate [%s]: ", targetInstance.InstanceSSLRootCert)
			newSSLRootCert, _ := reader.ReadString('\n')
			newSSLRootCert = strings.TrimSpace(newSSLRootCert)
			if newSSLRootCert != "" {
				updateReq.InstanceSSLRootCert = newSSLRootCert
				hasChanges = true
			}
		} else {
			// If SSL is disabled, set SSL mode to disable
			updateReq.InstanceSSLMode = "disable"
			hasChanges = true
		}

		if targetInstance.EnvironmentID != "" {
			fmt.Printf("Environment ID [%s]: ", targetInstance.EnvironmentID)
		} else {
			fmt.Print("Environment ID (optional): ")
		}
		newEnvironmentID, _ := reader.ReadString('\n')
		newEnvironmentID = strings.TrimSpace(newEnvironmentID)
		if newEnvironmentID != "" {
			updateReq.EnvironmentID = newEnvironmentID
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the instance
	updateURL := fmt.Sprintf("%s/api/v1/workspaces/%s/instances/%s", tenantURL, workspaceName, instanceName)

	var updateResponse UpdateInstanceResponse
	if err := client.Put(updateURL, updateReq, &updateResponse, true); err != nil {
		return fmt.Errorf("failed to update instance: %v", err)
	}

	fmt.Printf("Successfully updated instance '%s'\n", updateResponse.Instance.InstanceName)
	fmt.Println()
	return nil
}

// DisconnectInstance disconnects an existing instance
func DisconnectInstance(instanceName string, args []string) error {
	// Trim whitespace from instance name
	instanceName = strings.TrimSpace(instanceName)
	if instanceName == "" {
		return fmt.Errorf("instance name is required")
	}

	// Check for force flag and delete_instance flag
	force := false
	deleteInstance := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
		} else if strings.HasPrefix(arg, "--delete-instance=") {
			deleteInstance = strings.TrimPrefix(arg, "--delete-instance=") == "true"
		}
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm disconnection unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to disconnect instance '%s'? This action cannot be undone. (y/N): ", instanceName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}

		// Ask about deleting the instance object
		fmt.Print("Delete the instance object as well? (y/N): ")
		deleteConfirmation, _ := reader.ReadString('\n')
		deleteConfirmation = strings.TrimSpace(strings.ToLower(deleteConfirmation))

		if deleteConfirmation == "y" || deleteConfirmation == "yes" {
			deleteInstance = true
		}
	}

	// Create disconnect request
	disconnectReq := DisconnectInstanceRequest{
		DeleteInstance: deleteInstance,
	}

	// Disconnect the instance
	disconnectURL := fmt.Sprintf("%s/api/v1/workspaces/%s/instances/%s/disconnect", tenantURL, workspaceName, instanceName)

	var disconnectResponse DisconnectInstanceResponse
	if err := client.Post(disconnectURL, disconnectReq, &disconnectResponse, true); err != nil {
		return fmt.Errorf("failed to disconnect instance: %v", err)
	}

	fmt.Printf("Successfully disconnected instance '%s'\n", instanceName)
	if deleteInstance {
		fmt.Println("Instance object has been deleted")
	}
	fmt.Println()
	return nil
}

// readPassword reads a password from stdin with masking
func readPassword() (string, error) {
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println() // Print newline after password input
	return string(bytePassword), nil
}
