package milvus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

const (
	milvusDefaultPort = 19530
	milvusAPIVersion  = "v1"
)

// Connect establishes a connection to a Milvus database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
	provider := config.ConnectionType
	if provider == "" {
		provider = config.DatabaseVendor
	}
	if provider != "milvus" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'milvus'", provider)
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

	return &dbclient.DatabaseClient{
		DB:           client,
		DatabaseType: "milvus",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Milvus instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
	provider := config.ConnectionType
	if provider == "" {
		provider = config.DatabaseVendor
	}
	if provider != "milvus" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'milvus'", provider)
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

	return &dbclient.InstanceClient{
		DB:          client,
		InstanceID:  config.InstanceID,
		Config:      config,
		IsConnected: 1,
	}, nil
}

// DiscoverDetails fetches database details
func DiscoverDetails(client *MilvusClient) (map[string]interface{}, error) {
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

	details := make(map[string]interface{})
	details["uniqueIdentifier"] = fmt.Sprintf("milvus_%s_%d", client.Host, client.Port)
	details["databaseType"] = "milvus"
	details["databaseEdition"] = "community"
	details["version"] = "2.0.0" // Milvus doesn't expose version via API
	details["databaseSize"] = totalSize
	details["host"] = client.Host
	details["port"] = client.Port
	details["collectionCount"] = int64(len(collections))
	details["totalVectors"] = totalCount

	return details, nil
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

// ExecuteQuery executes a vector search query on Milvus and returns results as a slice of maps
// For Milvus, the query should be a JSON string representing a vector search
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	client, ok := db.(*MilvusClient)
	if !ok {
		return nil, fmt.Errorf("invalid milvus connection type")
	}

	// Parse the query as a Milvus vector search
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse milvus query: %w", err)
	}

	// Extract collection name
	collectionName, ok := queryReq["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("collection name is required in milvus query")
	}

	// Execute search request
	searchURL := fmt.Sprintf("%s/collections/%s/search", client.BaseURL, collectionName)

	jsonData, err := json.Marshal(queryReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	req, err := http.NewRequest("POST", searchURL, strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute milvus query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("milvus query failed: %s", string(body))
	}

	var searchResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode milvus response: %w", err)
	}

	// Convert results to slice of maps
	var results []interface{}
	if data, ok := searchResp["data"].([]interface{}); ok {
		for _, item := range data {
			if itemMap, ok := item.(map[string]interface{}); ok {
				results = append(results, itemMap)
			}
		}
	}

	return results, nil
}

// ExecuteCountQuery executes a count query on Milvus and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	client, ok := db.(*MilvusClient)
	if !ok {
		return 0, fmt.Errorf("invalid milvus connection type")
	}

	// Parse query to extract collection name
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse milvus count query: %w", err)
	}

	collectionName, ok := queryReq["collection"].(string)
	if !ok {
		return 0, fmt.Errorf("collection name is required in milvus count query")
	}

	// Get collection statistics
	statsURL := fmt.Sprintf("%s/collections/%s/stats", client.BaseURL, collectionName)

	req, err := http.NewRequest("GET", statsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get milvus stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("milvus stats request failed: %s", string(body))
	}

	var statsResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&statsResp); err != nil {
		return 0, fmt.Errorf("failed to decode milvus stats: %w", err)
	}

	// Extract row count
	if data, ok := statsResp["data"].(map[string]interface{}); ok {
		if rowCount, ok := data["row_count"].(float64); ok {
			return int64(rowCount), nil
		}
	}

	return 0, nil
}

// StreamTableData streams vectors from a Milvus collection in batches for efficient data copying
// For Milvus, tableName represents the collection name
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	client, ok := db.(*MilvusClient)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid milvus connection type")
	}

	// Build query request for getting vectors
	queryReq := map[string]interface{}{
		"collection_name": tableName,
		"limit":           batchSize,
		"offset":          offset,
	}

	if len(columns) > 0 {
		queryReq["output_fields"] = columns
	}

	// Execute query request
	queryURL := fmt.Sprintf("%s/collections/%s/entities", client.BaseURL, tableName)

	jsonData, err := json.Marshal(queryReq)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to marshal query request: %w", err)
	}

	req, err := http.NewRequest("POST", queryURL, strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute milvus streaming query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, "", fmt.Errorf("milvus streaming query failed: %s", string(body))
	}

	var queryResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, false, "", fmt.Errorf("failed to decode milvus response: %w", err)
	}

	// Convert results to slice of maps
	var results []map[string]interface{}
	if data, ok := queryResp["data"].([]interface{}); ok {
		for _, item := range data {
			if itemMap, ok := item.(map[string]interface{}); ok {
				results = append(results, itemMap)
			}
		}
	}

	rowCount := len(results)
	isComplete := rowCount < int(batchSize)

	// For simple offset-based pagination, we don't use cursor values
	nextCursorValue := ""

	return results, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of vectors in a Milvus collection
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	client, ok := db.(*MilvusClient)
	if !ok {
		return 0, false, fmt.Errorf("invalid milvus connection type")
	}

	// Get collection statistics
	statsURL := fmt.Sprintf("%s/collections/%s/stats", client.BaseURL, tableName)

	req, err := http.NewRequest("GET", statsURL, nil)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create request: %w", err)
	}

	if client.Username != "" && client.Password != "" {
		req.SetBasicAuth(client.Username, client.Password)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get milvus stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, false, fmt.Errorf("milvus stats request failed: %s", string(body))
	}

	var statsResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&statsResp); err != nil {
		return 0, false, fmt.Errorf("failed to decode milvus stats: %w", err)
	}

	// Extract row count
	if data, ok := statsResp["data"].(map[string]interface{}); ok {
		if rowCount, ok := data["row_count"].(float64); ok {
			return int64(rowCount), false, nil
		}
	}

	// Milvus count is always exact, not an estimate
	return 0, false, nil
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
