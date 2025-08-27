package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

const (
	weaviateDefaultPort = 8080
	weaviateAPIVersion  = "v1"
)

// Connect establishes a connection to a Weaviate database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	provider := config.ConnectionType
	if provider == "" {
		provider = config.DatabaseVendor
	}
	if provider != "weaviate" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'weaviate'", provider)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Extract Weaviate-specific configuration
	host := config.Host
	port := config.Port
	if port == 0 {
		port = weaviateDefaultPort
	}

	// Build base URL
	protocol := "http"
	if config.SSL {
		protocol = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, port, weaviateAPIVersion)

	// Create Weaviate client
	client := &WeaviateClient{
		BaseURL:     baseURL,
		Host:        host,
		Port:        port,
		Username:    config.Username,
		Password:    decryptedPassword,
		SSL:         config.SSL,
		IsConnected: 1,
	}

	// Test the connection
	_, err = listClasses(client)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Weaviate: %v", err)
	}

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "weaviate",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Weaviate instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	provider := config.ConnectionType
	if provider == "" {
		provider = config.DatabaseVendor
	}
	if provider != "weaviate" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'weaviate'", provider)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Extract Weaviate-specific configuration
	host := config.Host
	port := config.Port
	if port == 0 {
		port = weaviateDefaultPort
	}

	// Build base URL
	protocol := "http"
	if config.SSL {
		protocol = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, port, weaviateAPIVersion)

	// Create Weaviate client
	client := &WeaviateClient{
		BaseURL:     baseURL,
		Host:        host,
		Port:        port,
		Username:    config.Username,
		Password:    decryptedPassword,
		SSL:         config.SSL,
		IsConnected: 1,
	}

	// Test the connection
	_, err = listClasses(client)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Weaviate instance: %v", err)
	}

	return &common.InstanceClient{
		DB:          client,
		InstanceID:  config.InstanceID,
		Config:      config,
		IsConnected: 1,
	}, nil
}

// DiscoverDetails fetches database details
func DiscoverDetails(client *WeaviateClient) (map[string]interface{}, error) {
	// Get classes to determine database size
	classes, err := listClasses(client)
	if err != nil {
		return nil, fmt.Errorf("error listing classes: %v", err)
	}

	// Calculate total size and count
	var totalSize int64
	var totalCount int64
	for _, className := range classes {
		details, err := describeClass(client, className)
		if err != nil {
			continue // Skip classes we can't describe
		}
		totalSize += details.Size
		totalCount += details.ObjectCount
	}

	details := make(map[string]interface{})
	details["uniqueIdentifier"] = fmt.Sprintf("weaviate_%s_%d", client.Host, client.Port)
	details["databaseType"] = "weaviate"
	details["databaseEdition"] = "community"
	details["version"] = "1.0.0" // Weaviate doesn't expose version via API
	details["databaseSize"] = totalSize
	details["host"] = client.Host
	details["port"] = client.Port
	details["classCount"] = int64(len(classes))
	details["totalObjects"] = totalCount

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a Weaviate database
func CollectDatabaseMetadata(ctx context.Context, client *WeaviateClient) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database details
	details, err := DiscoverDetails(client)
	if err != nil {
		return nil, err
	}
	metadata["details"] = details

	// Get classes
	classes, err := listClasses(client)
	if err != nil {
		return nil, err
	}
	metadata["classes"] = classes

	// Get class details
	classDetails := make([]WeaviateClassInfo, 0, len(classes))
	for _, className := range classes {
		details, err := describeClass(client, className)
		if err != nil {
			continue
		}
		classDetails = append(classDetails, *details)
	}
	metadata["classDetails"] = classDetails

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Weaviate instance
func CollectInstanceMetadata(ctx context.Context, client *WeaviateClient) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get instance details
	details, err := DiscoverDetails(client)
	if err != nil {
		return nil, err
	}
	metadata["details"] = details

	// Get classes
	classes, err := listClasses(client)
	if err != nil {
		return nil, err
	}
	metadata["classes"] = classes

	// Get class details
	classDetails := make([]WeaviateClassInfo, 0, len(classes))
	for _, className := range classes {
		details, err := describeClass(client, className)
		if err != nil {
			continue
		}
		classDetails = append(classDetails, *details)
	}
	metadata["classDetails"] = classDetails

	return metadata, nil
}

// listClasses lists all classes in the Weaviate database
func listClasses(client *WeaviateClient) ([]string, error) {
	url := fmt.Sprintf("%s/schema", client.BaseURL)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Classes []struct {
			Class string `json:"class"`
		} `json:"classes"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	classes := make([]string, 0, len(response.Classes))
	for _, class := range response.Classes {
		classes = append(classes, class.Class)
	}

	return classes, nil
}

// describeClass gets detailed information about a class
func describeClass(client *WeaviateClient, className string) (*WeaviateClassInfo, error) {
	url := fmt.Sprintf("%s/schema/%s", client.BaseURL, className)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var class WeaviateClassInfo
	if err := json.NewDecoder(resp.Body).Decode(&class); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	// Get class count
	count, err := getClassCount(client, className)
	if err == nil {
		class.ObjectCount = count
	}

	return &class, nil
}

// getClassCount gets the count of objects in a class
func getClassCount(client *WeaviateClient, className string) (int64, error) {
	url := fmt.Sprintf("%s/objects?class=%s&limit=0", client.BaseURL, className)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("error creating request: %v", err)
	}

	// Add authentication if provided
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		TotalResults int64 `json:"totalResults"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, fmt.Errorf("error decoding response: %v", err)
	}

	return response.TotalResults, nil
}

// ExecuteCommand executes a command on a Weaviate database
func ExecuteCommand(ctx context.Context, client *WeaviateClient, command string) ([]byte, error) {
	// Weaviate doesn't support arbitrary command execution
	// This is a placeholder that returns an error
	return nil, fmt.Errorf("command execution is not supported for Weaviate databases")
}

// CreateDatabase creates a new Weaviate database
func CreateDatabase(ctx context.Context, client *WeaviateClient, databaseName string, options map[string]interface{}) error {
	// Weaviate doesn't support creating databases via API
	// Classes are created instead
	return fmt.Errorf("database creation is not supported for Weaviate. Use class creation instead")
}

// DropDatabase drops a Weaviate database
func DropDatabase(ctx context.Context, client *WeaviateClient, databaseName string, options map[string]interface{}) error {
	// Weaviate doesn't support dropping databases via API
	// Classes are dropped instead
	return fmt.Errorf("database deletion is not supported for Weaviate. Use class deletion instead")
}

// Close closes the Weaviate client connection
func (client *WeaviateClient) Close() {
	atomic.StoreInt32(&client.IsConnected, 0)
}
