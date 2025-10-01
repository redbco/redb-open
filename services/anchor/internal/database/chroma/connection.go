package chroma

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	chromav2 "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

const (
	chromaDefaultPort = 8000
	chromaAPIVersion  = "v2"
)

// Connect establishes a connection to a Chroma database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
	// Accept either vendor or type to identify provider; prefer type when set
	provider := config.ConnectionType
	if provider == "" {
		provider = config.DatabaseVendor
	}
	if provider != "chroma" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'chroma'", provider)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		if config.Password == "" {
			decryptedPassword = ""
		} else {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
	}

	// Extract Chroma-specific configuration
	host := config.Host
	port := config.Port
	if port == 0 {
		port = chromaDefaultPort
	}

	// Build base URL
	protocol := "http"
	if config.SSL {
		protocol = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, port, chromaAPIVersion)

	// Build chroma-go client options
	var opts []chromav2.ClientOption
	opts = append(opts, chromav2.WithBaseURL(baseURL))
	if config.Username != "" && decryptedPassword != "" {
		opts = append(opts, chromav2.WithAuth(chromav2.NewBasicAuthCredentialsProvider(config.Username, decryptedPassword)))
	}
	opts = append(opts, chromav2.WithTimeout(30*time.Second))

	apiClient, err := chromav2.NewHTTPClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create chroma client: %w", err)
	}

	client := &ChromaClient{
		API:         apiClient,
		BaseURL:     baseURL,
		Host:        host,
		Port:        port,
		Username:    config.Username,
		Password:    decryptedPassword,
		SSL:         config.SSL,
		IsConnected: 1,
	}

	// Test the connection (ListCollections). If 404-like error and non-default port, retry default port
	if _, err = listCollections(client); err != nil {
		if strings.Contains(err.Error(), "404") && port != chromaDefaultPort {
			fallbackBase := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, chromaDefaultPort, chromaAPIVersion)
			fallbackOpts := []chromav2.ClientOption{chromav2.WithBaseURL(fallbackBase), chromav2.WithTimeout(30 * time.Second)}
			if config.Username != "" && decryptedPassword != "" {
				fallbackOpts = append(fallbackOpts, chromav2.WithAuth(chromav2.NewBasicAuthCredentialsProvider(config.Username, decryptedPassword)))
			}
			fallbackAPI, err2 := chromav2.NewHTTPClient(fallbackOpts...)
			if err2 == nil {
				fallback := &ChromaClient{
					API:         fallbackAPI,
					BaseURL:     fallbackBase,
					Host:        host,
					Port:        chromaDefaultPort,
					Username:    config.Username,
					Password:    decryptedPassword,
					SSL:         config.SSL,
					IsConnected: 1,
				}
				if _, err3 := listCollections(fallback); err3 == nil {
					client = fallback
				} else {
					return nil, fmt.Errorf("error connecting to Chroma: %v", err)
				}
			} else {
				return nil, fmt.Errorf("error creating fallback Chroma client: %v", err2)
			}
		} else {
			return nil, fmt.Errorf("error connecting to Chroma: %v", err)
		}
	}

	return &dbclient.DatabaseClient{
		DB:           client,
		DatabaseType: "chroma",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Chroma instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
	// Accept either vendor or type to identify provider; prefer type when set
	provider := config.ConnectionType
	if provider == "" {
		provider = config.DatabaseVendor
	}
	if provider != "chroma" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'chroma'", provider)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		if config.Password == "" {
			decryptedPassword = ""
		} else {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
	}

	// Extract Chroma-specific configuration
	host := config.Host
	port := config.Port
	if port == 0 {
		port = chromaDefaultPort
	}

	// Build base URL
	protocol := "http"
	if config.SSL {
		protocol = "https"
	}
	baseURL := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, port, chromaAPIVersion)

	var opts []chromav2.ClientOption
	opts = append(opts, chromav2.WithBaseURL(baseURL), chromav2.WithTimeout(30*time.Second))
	if config.Username != "" && decryptedPassword != "" {
		opts = append(opts, chromav2.WithAuth(chromav2.NewBasicAuthCredentialsProvider(config.Username, decryptedPassword)))
	}
	apiClient, err := chromav2.NewHTTPClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create chroma client: %w", err)
	}

	client := &ChromaClient{
		API:         apiClient,
		BaseURL:     baseURL,
		Host:        host,
		Port:        port,
		Username:    config.Username,
		Password:    decryptedPassword,
		SSL:         config.SSL,
		IsConnected: 1,
	}

	if _, err = listCollections(client); err != nil {
		if strings.Contains(err.Error(), "404") && port != chromaDefaultPort {
			fallbackBase := fmt.Sprintf("%s://%s:%d/api/%s", protocol, host, chromaDefaultPort, chromaAPIVersion)
			fallbackOpts := []chromav2.ClientOption{chromav2.WithBaseURL(fallbackBase), chromav2.WithTimeout(30 * time.Second)}
			if config.Username != "" && decryptedPassword != "" {
				fallbackOpts = append(fallbackOpts, chromav2.WithAuth(chromav2.NewBasicAuthCredentialsProvider(config.Username, decryptedPassword)))
			}
			fallbackAPI, err2 := chromav2.NewHTTPClient(fallbackOpts...)
			if err2 == nil {
				fallback := &ChromaClient{
					API:         fallbackAPI,
					BaseURL:     fallbackBase,
					Host:        host,
					Port:        chromaDefaultPort,
					Username:    config.Username,
					Password:    decryptedPassword,
					SSL:         config.SSL,
					IsConnected: 1,
				}
				if _, err3 := listCollections(fallback); err3 == nil {
					client = fallback
				} else {
					return nil, fmt.Errorf("error connecting to Chroma instance: %v", err)
				}
			} else {
				return nil, fmt.Errorf("error creating fallback Chroma client: %v", err2)
			}
		} else {
			return nil, fmt.Errorf("error connecting to Chroma instance: %v", err)
		}
	}

	return &dbclient.InstanceClient{
		DB:           client,
		InstanceType: "chroma",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches database details
func DiscoverDetails(client *ChromaClient) (map[string]interface{}, error) {
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
		totalCount += details.Count
	}

	details := make(map[string]interface{})
	details["uniqueIdentifier"] = fmt.Sprintf("chroma_%s_%d", client.Host, client.Port)
	details["databaseType"] = "chroma"
	details["databaseEdition"] = "community"
	details["version"] = "1.0.0" // Chroma doesn't expose version via API
	details["databaseSize"] = totalSize
	details["host"] = client.Host
	details["port"] = client.Port
	details["collectionCount"] = int64(len(collections))
	details["totalVectors"] = totalCount

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a Chroma database
func CollectDatabaseMetadata(ctx context.Context, client *ChromaClient) (map[string]interface{}, error) {
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
	collectionDetails := make([]ChromaCollectionInfo, 0, len(collections))
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

// CollectInstanceMetadata collects metadata from a Chroma instance
func CollectInstanceMetadata(ctx context.Context, client *ChromaClient) (map[string]interface{}, error) {
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
	collectionDetails := make([]ChromaCollectionInfo, 0, len(collections))
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

// listCollections lists all collections in the Chroma database
func listCollections(client *ChromaClient) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cols, err := client.API.ListCollections(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		if c.Name() != "" {
			names = append(names, c.Name())
		}
	}
	return names, nil
}

// describeCollection gets detailed information about a collection
func describeCollection(client *ChromaClient, collectionName string) (*ChromaCollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	col, err := client.API.GetCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	var info ChromaCollectionInfo
	info.Name = col.Name()
	info.ID = col.ID()
	if md := col.Metadata(); md != nil {
		// Convert collection metadata to map[string]interface{}
		keys := md.Keys()
		info.Metadata = make(map[string]interface{}, len(keys))
		for _, k := range keys {
			if s, ok := md.GetString(k); ok {
				info.Metadata[k] = s
				continue
			}
			if i, ok := md.GetInt(k); ok {
				info.Metadata[k] = i
				continue
			}
			if f, ok := md.GetFloat(k); ok {
				info.Metadata[k] = f
				continue
			}
			if b, ok := md.GetBool(k); ok {
				info.Metadata[k] = b
				continue
			}
		}
	}

	// Count vectors in collection
	count, err := getCollectionCount(client, collectionName)
	if err == nil {
		info.Count = count
	}
	return &info, nil
}

// getCollectionCount gets the count of vectors in a collection
func getCollectionCount(client *ChromaClient, collectionName string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	col, err := client.API.GetCollection(ctx, collectionName)
	if err != nil {
		return 0, err
	}
	count, err := col.Count(ctx)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

// ExecuteQuery executes a vector search query on Chroma and returns results as a slice of maps
// For Chroma, the query should be a JSON string representing a vector query
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	client, ok := db.(*ChromaClient)
	if !ok {
		return nil, fmt.Errorf("invalid chroma connection type")
	}

	// Parse the query as a Chroma vector query
	// Expected format: {"collection": "name", "query_embeddings": [[...]], "n_results": 10}
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse chroma query: %w", err)
	}

	// Extract collection name
	collectionName, ok := queryReq["collection"].(string)
	if !ok {
		return nil, fmt.Errorf("collection name is required in chroma query")
	}

	// Make HTTP request to Chroma API
	queryURL := fmt.Sprintf("%s/api/v1/collections/%s/query", client.BaseURL, collectionName)

	jsonData, err := json.Marshal(queryReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query request: %w", err)
	}

	req, err := http.NewRequest("POST", queryURL, strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	// Chroma client doesn't use API key in this implementation

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute chroma query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chroma query failed: %s", string(body))
	}

	var queryResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode chroma response: %w", err)
	}

	// Convert results to slice of maps
	var queryResults []interface{}
	if results, ok := queryResp["results"].([]interface{}); ok {
		for _, result := range results {
			if resultMap, ok := result.(map[string]interface{}); ok {
				queryResults = append(queryResults, resultMap)
			}
		}
	}

	return queryResults, nil
}

// ExecuteCountQuery executes a count query on Chroma and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	client, ok := db.(*ChromaClient)
	if !ok {
		return 0, fmt.Errorf("invalid chroma connection type")
	}

	// Parse query to extract collection name
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse chroma count query: %w", err)
	}

	collectionName, ok := queryReq["collection"].(string)
	if !ok {
		return 0, fmt.Errorf("collection name is required in chroma count query")
	}

	return getCollectionCount(client, collectionName)
}

// StreamTableData streams vectors from a Chroma collection in batches for efficient data copying
// For Chroma, tableName represents the collection name
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	client, ok := db.(*ChromaClient)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid chroma connection type")
	}

	// Build URL with limit and offset
	url := fmt.Sprintf("%s/api/v1/collections/%s/get?limit=%d&offset=%d", client.BaseURL, tableName, batchSize, offset)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	// Chroma client doesn't use API key in this implementation

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute chroma streaming query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, "", fmt.Errorf("chroma streaming query failed: %s", string(body))
	}

	var streamResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&streamResp); err != nil {
		return nil, false, "", fmt.Errorf("failed to decode chroma response: %w", err)
	}

	// Convert results to slice of maps
	var streamResults []map[string]interface{}

	// Process results from HTTP response
	if ids, ok := streamResp["ids"].([]interface{}); ok {
		for i, id := range ids {
			result := make(map[string]interface{})
			result["id"] = id

			if documents, ok := streamResp["documents"].([]interface{}); ok && len(documents) > i {
				result["document"] = documents[i]
			}
			if metadatas, ok := streamResp["metadatas"].([]interface{}); ok && len(metadatas) > i {
				result["metadata"] = metadatas[i]
			}
			if embeddings, ok := streamResp["embeddings"].([]interface{}); ok && len(embeddings) > i {
				result["embedding"] = embeddings[i]
			}

			streamResults = append(streamResults, result)
		}
	}

	rowCount := len(streamResults)
	isComplete := rowCount < int(batchSize)

	// For simple offset-based pagination, we don't use cursor values
	nextCursorValue := ""

	return streamResults, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of vectors in a Chroma collection
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	client, ok := db.(*ChromaClient)
	if !ok {
		return 0, false, fmt.Errorf("invalid chroma connection type")
	}

	count, err := getCollectionCount(client, tableName)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get chroma collection count: %w", err)
	}

	// Chroma count is always exact, not an estimate
	return count, false, nil
}

// ExecuteCommand executes a command on a Chroma database
func ExecuteCommand(ctx context.Context, client *ChromaClient, command string) ([]byte, error) {
	// Chroma doesn't support arbitrary command execution
	// This is a placeholder that returns an error
	return nil, fmt.Errorf("command execution is not supported for Chroma databases")
}

// CreateDatabase creates a new Chroma database
func CreateDatabase(ctx context.Context, client *ChromaClient, databaseName string, options map[string]interface{}) error {
	// Chroma doesn't support creating databases via API
	// Collections are created instead
	return fmt.Errorf("database creation is not supported for Chroma. Use collection creation instead")
}

// DropDatabase drops a Chroma database
func DropDatabase(ctx context.Context, client *ChromaClient, databaseName string, options map[string]interface{}) error {
	// Chroma doesn't support dropping databases via API
	// Collections are dropped instead
	return fmt.Errorf("database deletion is not supported for Chroma. Use collection deletion instead")
}

// Close closes the Chroma client connection
func (client *ChromaClient) Close() {
	if client.API != nil {
		_ = client.API.Close()
	}
	atomic.StoreInt32(&client.IsConnected, 0)
}
