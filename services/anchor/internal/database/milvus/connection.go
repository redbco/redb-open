package milvus

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
	milvusDefaultPort = 19530
	milvusAPIVersion  = "v1"
)

// Connect establishes a connection to a Milvus database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	if config.DatabaseVendor != "milvus" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'milvus'", config.DatabaseVendor)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Extract Milvus-specific configuration
	host := config.Host
	port := config.Port
	if port == 0 {
		port = milvusDefaultPort
	}

	// Build base URL
	protocol := "http"
	if config.SSL {
		protocol = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, port, milvusAPIVersion)

	// Create Milvus client
	client := &MilvusClient{
		BaseURL:     baseURL,
		Host:        host,
		Port:        port,
		Username:    config.Username,
		Password:    decryptedPassword,
		SSL:         config.SSL,
		IsConnected: 1,
	}

	// Test the connection
	_, err = listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Milvus: %v", err)
	}

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "milvus",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Milvus instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	if config.DatabaseVendor != "milvus" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'milvus'", config.DatabaseVendor)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Extract Milvus-specific configuration
	host := config.Host
	port := config.Port
	if port == 0 {
		port = milvusDefaultPort
	}

	// Build base URL
	protocol := "http"
	if config.SSL {
		protocol = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, port, milvusAPIVersion)

	// Create Milvus client
	client := &MilvusClient{
		BaseURL:     baseURL,
		Host:        host,
		Port:        port,
		Username:    config.Username,
		Password:    decryptedPassword,
		SSL:         config.SSL,
		IsConnected: 1,
	}

	// Test the connection
	_, err = listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Milvus instance: %v", err)
	}

	return &common.InstanceClient{
		DB:          client,
		InstanceID:  config.InstanceID,
		Config:      config,
		IsConnected: 1,
	}, nil
}

// DiscoverDetails fetches database details
func DiscoverDetails(client *MilvusClient) (*MilvusDetails, error) {
	// Get collections to determine database size
	collections, err := listCollections(client)
	if err != nil {
		return nil, fmt.Errorf("error listing collections: %v", err)
	}

	// Calculate total size and count
	var totalSize int64
	var totalCount int64
	for _, collection := range collections {
		details, err := describeCollection(client, collection)
		if err != nil {
			continue // Skip collections we can't describe
		}
		totalSize += details.Size
		totalCount += details.RowCount
	}

	return &MilvusDetails{
		UniqueIdentifier: fmt.Sprintf("milvus_%s_%d", client.Host, client.Port),
		DatabaseType:     "milvus",
		DatabaseEdition:  "community",
		Version:          "2.0.0", // Milvus doesn't expose version via API
		DatabaseSize:     totalSize,
		Host:             client.Host,
		Port:             client.Port,
		CollectionCount:  int64(len(collections)),
		TotalVectors:     totalCount,
	}, nil
}

// CollectDatabaseMetadata collects metadata from a Milvus database
func CollectDatabaseMetadata(ctx context.Context, client *MilvusClient) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get database details
	details, err := DiscoverDetails(client)
	if err != nil {
		return nil, err
	}
	metadata["details"] = details

	// Get collections
	collections, err := listCollections(client)
	if err != nil {
		return nil, err
	}
	metadata["collections"] = collections

	// Get collection details
	collectionDetails := make([]MilvusCollectionInfo, 0, len(collections))
	for _, collectionName := range collections {
		details, err := describeCollection(client, collectionName)
		if err != nil {
			continue
		}
		collectionDetails = append(collectionDetails, *details)
	}
	metadata["collectionDetails"] = collectionDetails

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Milvus instance
func CollectInstanceMetadata(ctx context.Context, client *MilvusClient) (map[string]interface{}, error) {
	metadata := make(map[string]interface{})

	// Get instance details
	details, err := DiscoverDetails(client)
	if err != nil {
		return nil, err
	}
	metadata["details"] = details

	// Get collections
	collections, err := listCollections(client)
	if err != nil {
		return nil, err
	}
	metadata["collections"] = collections

	// Get collection details
	collectionDetails := make([]MilvusCollectionInfo, 0, len(collections))
	for _, collectionName := range collections {
		details, err := describeCollection(client, collectionName)
		if err != nil {
			continue
		}
		collectionDetails = append(collectionDetails, *details)
	}
	metadata["collectionDetails"] = collectionDetails

	return metadata, nil
}

// listCollections lists all collections in the Milvus database
func listCollections(client *MilvusClient) ([]string, error) {
	url := fmt.Sprintf("%s/collections", client.BaseURL)

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
		Collections []struct {
			Name string `json:"name"`
		} `json:"collections"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	collections := make([]string, 0, len(response.Collections))
	for _, collection := range response.Collections {
		collections = append(collections, collection.Name)
	}

	return collections, nil
}

// describeCollection gets detailed information about a collection
func describeCollection(client *MilvusClient, collectionName string) (*MilvusCollectionInfo, error) {
	url := fmt.Sprintf("%s/collections/%s", client.BaseURL, collectionName)

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

	var collection MilvusCollectionInfo
	if err := json.NewDecoder(resp.Body).Decode(&collection); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return &collection, nil
}

// ExecuteCommand executes a command on a Milvus database
func ExecuteCommand(ctx context.Context, client *MilvusClient, command string) ([]byte, error) {
	// Milvus doesn't support arbitrary command execution
	// This is a placeholder that returns an error
	return nil, fmt.Errorf("command execution is not supported for Milvus databases")
}

// CreateDatabase creates a new Milvus database
func CreateDatabase(ctx context.Context, client *MilvusClient, databaseName string, options map[string]interface{}) error {
	// Milvus doesn't support creating databases via API
	// Collections are created instead
	return fmt.Errorf("database creation is not supported for Milvus. Use collection creation instead")
}

// DropDatabase drops a Milvus database
func DropDatabase(ctx context.Context, client *MilvusClient, databaseName string, options map[string]interface{}) error {
	// Milvus doesn't support dropping databases via API
	// Collections are dropped instead
	return fmt.Errorf("database deletion is not supported for Milvus. Use collection deletion instead")
}

// Close closes the Milvus client connection
func (client *MilvusClient) Close() {
	atomic.StoreInt32(&client.IsConnected, 0)
}
