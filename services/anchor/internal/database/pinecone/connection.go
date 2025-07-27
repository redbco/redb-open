package pinecone

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
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

const (
	pineconeControllerURL = "https://controller.%s.pinecone.io"
	pineconeAPIURL        = "https://%s-%s.svc.%s.pinecone.io"
)

// Connect establishes a connection to a Pinecone database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	if config.DatabaseVendor != "pinecone" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'pinecone'", config.DatabaseVendor)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Extract Pinecone-specific configuration
	apiKey := decryptedPassword  // Using password field for API key
	environment := config.Host   // Using host field for environment
	projectID := config.Username // Using username field for project ID

	if apiKey == "" {
		return nil, fmt.Errorf("pinecone api key is required")
	}

	if environment == "" {
		return nil, fmt.Errorf("pinecone environment is required")
	}

	// Create Pinecone client
	client := &PineconeClient{
		APIKey:      apiKey,
		Environment: environment,
		ProjectID:   projectID,
		BaseURL:     fmt.Sprintf(pineconeControllerURL, environment),
		IsConnected: 1,
	}

	// Test the connection
	_, err = listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Pinecone: %v", err)
	}

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "pinecone",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Pinecone instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	if config.DatabaseVendor != "pinecone" {
		return nil, fmt.Errorf("invalid database provider: %s, expected 'pinecone'", config.DatabaseVendor)
	}

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Extract Pinecone-specific configuration
	apiKey := decryptedPassword  // Using password field for API key
	environment := config.Host   // Using host field for environment
	projectID := config.Username // Using username field for project ID

	if apiKey == "" {
		return nil, fmt.Errorf("pinecone api key is required")
	}

	if environment == "" {
		return nil, fmt.Errorf("pinecone environment is required")
	}

	// Create Pinecone client
	client := &PineconeClient{
		APIKey:      apiKey,
		Environment: environment,
		ProjectID:   projectID,
		BaseURL:     fmt.Sprintf(pineconeControllerURL, environment),
		IsConnected: 1,
	}

	// Test the connection
	_, err = listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Pinecone: %v", err)
	}

	return &common.InstanceClient{
		DB:           client,
		InstanceType: "pinecone",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a Pinecone database
func DiscoverDetails(db interface{}) (*PineconeDetails, error) {
	client, ok := db.(*PineconeClient)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := &PineconeDetails{
		DatabaseType: "pinecone",
		Environment:  client.Environment,
	}

	// Get version information
	details.Version = "1.0" // Pinecone doesn't expose version info directly

	// Get database size (sum of all indexes)
	indexes, err := listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("error fetching indexes: %v", err)
	}

	var totalSize int64
	for _, index := range indexes {
		indexStats, err := getIndexStats(client, index)
		if err != nil {
			continue // Skip this index if we can't get stats
		}
		totalSize += indexStats.IndexSize
	}
	details.DatabaseSize = totalSize

	// Generate a unique identifier
	details.UniqueIdentifier = client.ProjectID

	// Determine edition (free or standard)
	if strings.Contains(client.Environment, "free") {
		details.DatabaseEdition = "free"
	} else {
		details.DatabaseEdition = "standard"
	}

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a Pinecone database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*PineconeClient)
	if !ok {
		return nil, fmt.Errorf("invalid pinecone connection type")
	}

	metadata := make(map[string]interface{})

	// Get indexes
	indexes, err := listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	metadata["indexes_count"] = len(indexes)

	// Get total vector count and size
	var totalVectors int64
	var totalSize int64
	for _, index := range indexes {
		indexStats, err := getIndexStats(client, index)
		if err != nil {
			continue // Skip this index if we can't get stats
		}
		totalVectors += indexStats.VectorCount
		totalSize += indexStats.IndexSize
	}
	metadata["total_vectors"] = totalVectors
	metadata["size_bytes"] = totalSize

	// Add environment info
	metadata["environment"] = client.Environment
	metadata["project_id"] = client.ProjectID

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Pinecone instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*PineconeClient)
	if !ok {
		return nil, fmt.Errorf("invalid pinecone connection type")
	}

	metadata := make(map[string]interface{})

	// Get indexes
	indexes, err := listIndexes(client)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}
	metadata["total_indexes"] = len(indexes)

	// Get collections if available
	collections, err := listCollections(client)
	if err == nil {
		metadata["total_collections"] = len(collections)
	} else {
		metadata["total_collections"] = 0
	}

	// Add environment info
	metadata["environment"] = client.Environment
	metadata["project_id"] = client.ProjectID

	// Pinecone doesn't provide uptime information
	metadata["uptime_seconds"] = int64(0)

	return metadata, nil
}

// Helper functions for Pinecone API calls

func listIndexes(client *PineconeClient) ([]string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/databases", client.BaseURL), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to list indexes: %s", string(body))
	}

	var result []string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func describeIndex(client *PineconeClient, indexName string) (PineconeIndexInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/databases/%s", client.BaseURL, indexName), nil)
	if err != nil {
		return PineconeIndexInfo{}, err
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return PineconeIndexInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return PineconeIndexInfo{}, fmt.Errorf("failed to describe index: %s", string(body))
	}

	var result PineconeIndexInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return PineconeIndexInfo{}, err
	}

	return result, nil
}

func getIndexStats(client *PineconeClient, indexName string) (PineconeIndexInfo, error) {
	// Get the index host
	indexDetails, err := describeIndex(client, indexName)
	if err != nil {
		return PineconeIndexInfo{}, err
	}

	// Construct the API URL for the specific index
	indexHost := fmt.Sprintf(pineconeAPIURL, indexName, client.ProjectID, client.Environment)

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/describe_index_stats", indexHost), nil)
	if err != nil {
		return PineconeIndexInfo{}, err
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return PineconeIndexInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return PineconeIndexInfo{}, fmt.Errorf("failed to get index stats: %s", string(body))
	}

	var result struct {
		Namespaces map[string]struct {
			VectorCount int64 `json:"vectorCount"`
		} `json:"namespaces"`
		Dimension        int     `json:"dimension"`
		IndexFullness    float64 `json:"indexFullness"`
		TotalVectorCount int64   `json:"totalVectorCount"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return PineconeIndexInfo{}, err
	}

	// Extract namespaces
	namespaces := make([]string, 0, len(result.Namespaces))
	for ns := range result.Namespaces {
		namespaces = append(namespaces, ns)
	}

	// Update the index details with stats
	indexDetails.VectorCount = result.TotalVectorCount
	indexDetails.Namespaces = namespaces

	// Estimate index size (this is approximate)
	// Each vector takes approximately: dimension * 4 bytes (float32) + metadata size
	// Using a rough estimate of 100 bytes per vector for metadata
	estimatedBytesPerVector := int64(result.Dimension*4 + 100)
	indexDetails.IndexSize = result.TotalVectorCount * estimatedBytesPerVector

	return indexDetails, nil
}

func listCollections(client *PineconeClient) ([]string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/collections", client.BaseURL), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// If collections are not supported, return an empty list
	if resp.StatusCode == http.StatusNotFound {
		return []string{}, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to list collections: %s", string(body))
	}

	var result []string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func describeCollection(client *PineconeClient, collectionName string) (PineconeCollectionInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/collections/%s", client.BaseURL, collectionName), nil)
	if err != nil {
		return PineconeCollectionInfo{}, err
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return PineconeCollectionInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return PineconeCollectionInfo{}, fmt.Errorf("failed to describe collection: %s", string(body))
	}

	var result PineconeCollectionInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return PineconeCollectionInfo{}, err
	}

	return result, nil
}

// Close closes the Pinecone client connection
// This is a no-op for Pinecone since it uses HTTP requests and doesn't maintain a persistent connection
func (client *PineconeClient) Close() {
	// Set IsConnected to 0 to indicate the client is closed
	atomic.StoreInt32(&client.IsConnected, 0)
}

// ExecuteCommand executes a command on a Pinecone database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	client, ok := db.(*PineconeClient)
	if !ok {
		return nil, fmt.Errorf("invalid pinecone connection type")
	}

	// Pinecone doesn't support arbitrary commands like SQL databases
	// Instead, we interpret common commands and return appropriate data
	var results []map[string]interface{}
	var columnNames []string

	command = strings.TrimSpace(strings.ToLower(command))

	switch {
	case strings.Contains(command, "list") && strings.Contains(command, "index"):
		// List indexes
		indexes, err := listIndexes(client)
		if err != nil {
			return nil, fmt.Errorf("failed to list indexes: %w", err)
		}
		columnNames = []string{"index_name"}
		for _, index := range indexes {
			results = append(results, map[string]interface{}{"index_name": index})
		}

	case strings.Contains(command, "describe") && strings.Contains(command, "index"):
		// Extract index name from command (basic parsing)
		parts := strings.Fields(command)
		if len(parts) < 3 {
			return nil, fmt.Errorf("invalid describe index command format")
		}
		indexName := parts[len(parts)-1]

		indexInfo, err := describeIndex(client, indexName)
		if err != nil {
			return nil, fmt.Errorf("failed to describe index: %w", err)
		}

		columnNames = []string{"name", "dimension", "metric", "status", "vector_count"}
		results = append(results, map[string]interface{}{
			"name":         indexInfo.Name,
			"dimension":    indexInfo.Dimension,
			"metric":       indexInfo.Metric,
			"status":       indexInfo.Status,
			"vector_count": indexInfo.VectorCount,
		})

	case strings.Contains(command, "list") && strings.Contains(command, "collection"):
		// List collections
		collections, err := listCollections(client)
		if err != nil {
			return nil, fmt.Errorf("failed to list collections: %w", err)
		}
		columnNames = []string{"collection_name"}
		for _, collection := range collections {
			results = append(results, map[string]interface{}{"collection_name": collection})
		}

	default:
		return nil, fmt.Errorf("unsupported command for Pinecone: %s", command)
	}

	// Structure the response for gRPC
	response := map[string]interface{}{
		"columns": columnNames,
		"rows":    results,
		"count":   len(results),
	}

	// Convert to JSON bytes for gRPC transmission
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results to JSON: %w", err)
	}

	return jsonBytes, nil
}

// CreateDatabase creates a new Pinecone index (equivalent to a database) with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*PineconeClient)
	if !ok {
		return fmt.Errorf("invalid pinecone connection type")
	}

	// In Pinecone, we create an index instead of a database
	// Build the create index request
	createRequest := map[string]interface{}{
		"name": databaseName,
	}

	// Parse and apply options
	if dimension, ok := options["dimension"]; ok {
		createRequest["dimension"] = dimension
	} else {
		// Default dimension if not specified
		createRequest["dimension"] = 1536
	}

	if metric, ok := options["metric"].(string); ok && metric != "" {
		createRequest["metric"] = metric
	} else {
		createRequest["metric"] = "cosine"
	}

	if podType, ok := options["pod_type"].(string); ok && podType != "" {
		createRequest["pod_type"] = podType
	}

	if pods, ok := options["pods"]; ok {
		createRequest["pods"] = pods
	}

	if replicas, ok := options["replicas"]; ok {
		createRequest["replicas"] = replicas
	}

	if shards, ok := options["shards"]; ok {
		createRequest["shards"] = shards
	}

	if metadataConfig, ok := options["metadata_config"]; ok {
		createRequest["metadata_config"] = metadataConfig
	}

	// Convert to JSON
	jsonData, err := json.Marshal(createRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal create request: %w", err)
	}

	// Make HTTP request to create index
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/databases", client.BaseURL), strings.NewReader(string(jsonData)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create index: %s", string(body))
	}

	return nil
}

// DropDatabase drops a Pinecone index (equivalent to dropping a database) with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*PineconeClient)
	if !ok {
		return fmt.Errorf("invalid pinecone connection type")
	}

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		// Check if index exists first
		indexes, err := listIndexes(client)
		if err != nil {
			return fmt.Errorf("failed to check if index exists: %w", err)
		}

		found := false
		for _, index := range indexes {
			if index == databaseName {
				found = true
				break
			}
		}

		if !found {
			// Index doesn't exist, but that's OK with if_exists
			return nil
		}
	}

	// Make HTTP request to delete index
	req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/databases/%s", client.BaseURL, databaseName), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Api-Key", client.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete index: %s", string(body))
	}

	return nil
}
