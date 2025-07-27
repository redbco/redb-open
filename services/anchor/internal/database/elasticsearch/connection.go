package elasticsearch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to an Elasticsearch cluster
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Create Elasticsearch configuration
	cfg := elasticsearch.Config{
		Addresses: []string{
			fmt.Sprintf("http%s://%s:%d",
				getProtocolSuffix(config.SSL),
				config.Host,
				config.Port),
		},
	}

	// Add authentication if provided
	if config.Username != "" && config.Password != "" {
		cfg.Username = config.Username
		cfg.Password = decryptedPassword
	}

	// Configure SSL/TLS if enabled
	if config.SSL {
		tlsConfig, err := createTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("error configuring TLS: %v", err)
		}
		cfg.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	// Create Elasticsearch client
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating Elasticsearch client: %v", err)
	}

	// Test the connection
	res, err := esClient.Info()
	if err != nil {
		return nil, fmt.Errorf("error connecting to Elasticsearch: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Create our wrapper client
	client := &ElasticsearchClient{
		Client:      esClient,
		IsConnected: 1,
	}

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "elasticsearch",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to an Elasticsearch instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Create Elasticsearch configuration
	cfg := elasticsearch.Config{
		Addresses: []string{
			fmt.Sprintf("http%s://%s:%d",
				getProtocolSuffix(config.SSL),
				config.Host,
				config.Port),
		},
	}

	// Add authentication if provided
	if config.Username != "" && config.Password != "" {
		cfg.Username = config.Username
		cfg.Password = decryptedPassword
	}

	// Configure SSL/TLS if enabled
	if config.SSL {
		tlsConfig, err := createInstanceTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("error configuring TLS: %v", err)
		}
		cfg.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	// Create Elasticsearch client
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating Elasticsearch client: %v", err)
	}

	// Test the connection
	res, err := esClient.Info()
	if err != nil {
		return nil, fmt.Errorf("error connecting to Elasticsearch: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Create our wrapper client
	client := &ElasticsearchClient{
		Client:      esClient,
		IsConnected: 1,
	}

	return &common.InstanceClient{
		DB:           client,
		InstanceType: "elasticsearch",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of an Elasticsearch cluster
func DiscoverDetails(db interface{}) (*ElasticsearchDetails, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	client := esClient.Client
	var details ElasticsearchDetails
	details.DatabaseType = "elasticsearch"

	// Get cluster info
	infoRes, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("error fetching cluster info: %v", err)
	}
	defer infoRes.Body.Close()

	if infoRes.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", infoRes.String())
	}

	// Parse info response
	var infoResp map[string]interface{}
	if err := json.NewDecoder(infoRes.Body).Decode(&infoResp); err != nil {
		return nil, fmt.Errorf("error parsing info response: %v", err)
	}

	// Extract version
	if version, ok := infoResp["version"].(map[string]interface{}); ok {
		if number, ok := version["number"].(string); ok {
			details.Version = number
		}
	}

	// Get cluster health
	healthRes, err := client.Cluster.Health()
	if err != nil {
		return nil, fmt.Errorf("error fetching cluster health: %v", err)
	}
	defer healthRes.Body.Close()

	if healthRes.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch health: %s", healthRes.String())
	}

	// Parse health response
	var healthResp map[string]interface{}
	if err := json.NewDecoder(healthRes.Body).Decode(&healthResp); err != nil {
		return nil, fmt.Errorf("error parsing health response: %v", err)
	}

	// Extract cluster details
	if clusterName, ok := healthResp["cluster_name"].(string); ok {
		details.ClusterName = clusterName
	}
	if status, ok := healthResp["status"].(string); ok {
		details.ClusterHealth = status
	}
	if numberOfNodes, ok := healthResp["number_of_nodes"].(float64); ok {
		details.NumberOfNodes = int(numberOfNodes)
	}

	// Get cluster stats for size
	statsRes, err := client.Cluster.Stats()
	if err != nil {
		return nil, fmt.Errorf("error fetching cluster stats: %v", err)
	}
	defer statsRes.Body.Close()

	if statsRes.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch stats: %s", statsRes.String())
	}

	// Parse stats response
	var statsResp map[string]interface{}
	if err := json.NewDecoder(statsRes.Body).Decode(&statsResp); err != nil {
		return nil, fmt.Errorf("error parsing stats response: %v", err)
	}

	// Extract database size
	if indices, ok := statsResp["indices"].(map[string]interface{}); ok {
		if store, ok := indices["store"].(map[string]interface{}); ok {
			if sizeInBytes, ok := store["size_in_bytes"].(float64); ok {
				details.DatabaseSize = int64(sizeInBytes)
			}
		}
	}

	// Generate unique identifier
	details.UniqueIdentifier = details.ClusterName

	// Determine edition
	if strings.Contains(strings.ToLower(details.Version), "x-pack") {
		details.DatabaseEdition = "enterprise"
	} else {
		details.DatabaseEdition = "community"
	}

	return &details, nil
}

// CollectDatabaseMetadata collects metadata from an Elasticsearch cluster
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return nil, fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	metadata := make(map[string]interface{})

	// Get cluster info
	infoRes, err := client.Info(client.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}
	defer infoRes.Body.Close()

	var infoResp map[string]interface{}
	if err := json.NewDecoder(infoRes.Body).Decode(&infoResp); err != nil {
		return nil, fmt.Errorf("error parsing info response: %v", err)
	}

	// Extract version
	if version, ok := infoResp["version"].(map[string]interface{}); ok {
		if number, ok := version["number"].(string); ok {
			metadata["version"] = number
		}
	}

	// Get indices stats
	indicesRes, err := client.Indices.Stats(client.Indices.Stats.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to get indices stats: %w", err)
	}
	defer indicesRes.Body.Close()

	var indicesResp map[string]interface{}
	if err := json.NewDecoder(indicesRes.Body).Decode(&indicesResp); err != nil {
		return nil, fmt.Errorf("error parsing indices stats response: %v", err)
	}

	// Extract indices count and size
	if all, ok := indicesResp["_all"].(map[string]interface{}); ok {
		if primaries, ok := all["primaries"].(map[string]interface{}); ok {
			if store, ok := primaries["store"].(map[string]interface{}); ok {
				if sizeInBytes, ok := store["size_in_bytes"].(float64); ok {
					metadata["size_bytes"] = int64(sizeInBytes)
				}
			}
		}
		if indices, ok := indicesResp["indices"].(map[string]interface{}); ok {
			metadata["indices_count"] = len(indices)
		}
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from an Elasticsearch instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return nil, fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	metadata := make(map[string]interface{})

	// Get cluster info
	infoRes, err := client.Info(client.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %w", err)
	}
	defer infoRes.Body.Close()

	var infoResp map[string]interface{}
	if err := json.NewDecoder(infoRes.Body).Decode(&infoResp); err != nil {
		return nil, fmt.Errorf("error parsing info response: %v", err)
	}

	// Extract version
	if version, ok := infoResp["version"].(map[string]interface{}); ok {
		if number, ok := version["number"].(string); ok {
			metadata["version"] = number
		}
	}

	// Get cluster health
	healthRes, err := client.Cluster.Health(client.Cluster.Health.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster health: %w", err)
	}
	defer healthRes.Body.Close()

	var healthResp map[string]interface{}
	if err := json.NewDecoder(healthRes.Body).Decode(&healthResp); err != nil {
		return nil, fmt.Errorf("error parsing health response: %v", err)
	}

	// Extract cluster details
	if status, ok := healthResp["status"].(string); ok {
		metadata["cluster_status"] = status
	}
	if numberOfNodes, ok := healthResp["number_of_nodes"].(float64); ok {
		metadata["total_nodes"] = int(numberOfNodes)
	}
	if numberOfDataNodes, ok := healthResp["number_of_data_nodes"].(float64); ok {
		metadata["data_nodes"] = int(numberOfDataNodes)
	}
	if activeShards, ok := healthResp["active_shards"].(float64); ok {
		metadata["active_shards"] = int(activeShards)
	}

	// Get nodes stats
	nodesRes, err := client.Nodes.Stats(client.Nodes.Stats.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes stats: %w", err)
	}
	defer nodesRes.Body.Close()

	var nodesResp map[string]interface{}
	if err := json.NewDecoder(nodesRes.Body).Decode(&nodesResp); err != nil {
		return nil, fmt.Errorf("error parsing nodes stats response: %v", err)
	}

	// Extract uptime from the first node
	if nodes, ok := nodesResp["nodes"].(map[string]interface{}); ok {
		for _, nodeData := range nodes {
			if node, ok := nodeData.(map[string]interface{}); ok {
				if jvm, ok := node["jvm"].(map[string]interface{}); ok {
					if uptime, ok := jvm["uptime_in_millis"].(float64); ok {
						metadata["uptime_seconds"] = int64(uptime / 1000)
						break
					}
				}
			}
		}
	}

	return metadata, nil
}

// Helper functions

func getProtocolSuffix(ssl bool) string {
	if ssl {
		return "s"
	}
	return ""
}

func createTLSConfig(config common.DatabaseConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized,
	}

	// Load client certificate and key if provided
	if config.SSLCert != "" && config.SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if config.SSLRootCert != "" {
		caCert, err := os.ReadFile(config.SSLRootCert)
		if err != nil {
			return nil, fmt.Errorf("error reading CA certificate: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

func createInstanceTLSConfig(config common.InstanceConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized,
	}

	// Load client certificate and key if provided
	if config.SSLCert != "" && config.SSLKey != "" {
		cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
		if err != nil {
			return nil, fmt.Errorf("error loading client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if config.SSLRootCert != "" {
		caCert, err := os.ReadFile(config.SSLRootCert)
		if err != nil {
			return nil, fmt.Errorf("error reading CA certificate: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

// ExecuteCommand executes a command on an Elasticsearch cluster and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return nil, fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client
	var results []map[string]interface{}
	var columnNames []string

	command = strings.TrimSpace(strings.ToLower(command))

	switch {
	case strings.Contains(command, "show") && strings.Contains(command, "indices"):
		// List indices
		res, err := client.Cat.Indices(client.Cat.Indices.WithContext(ctx), client.Cat.Indices.WithFormat("json"))
		if err != nil {
			return nil, fmt.Errorf("failed to list indices: %w", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
		}

		var indices []map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
			return nil, fmt.Errorf("failed to decode indices response: %w", err)
		}

		if len(indices) > 0 {
			// Extract column names from first index
			for key := range indices[0] {
				columnNames = append(columnNames, key)
			}
			results = indices
		}

	case strings.Contains(command, "show") && strings.Contains(command, "nodes"):
		// List nodes
		res, err := client.Cat.Nodes(client.Cat.Nodes.WithContext(ctx), client.Cat.Nodes.WithFormat("json"))
		if err != nil {
			return nil, fmt.Errorf("failed to list nodes: %w", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
		}

		var nodes []map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&nodes); err != nil {
			return nil, fmt.Errorf("failed to decode nodes response: %w", err)
		}

		if len(nodes) > 0 {
			// Extract column names from first node
			for key := range nodes[0] {
				columnNames = append(columnNames, key)
			}
			results = nodes
		}

	case strings.Contains(command, "cluster") && strings.Contains(command, "health"):
		// Get cluster health
		res, err := client.Cluster.Health(client.Cluster.Health.WithContext(ctx))
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster health: %w", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
		}

		var health map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&health); err != nil {
			return nil, fmt.Errorf("failed to decode health response: %w", err)
		}

		// Extract column names
		for key := range health {
			columnNames = append(columnNames, key)
		}
		results = append(results, health)

	default:
		// Try to execute as a search query on _all indices
		if strings.HasPrefix(command, "{") {
			// Looks like a JSON query
			res, err := client.Search(
				client.Search.WithContext(ctx),
				client.Search.WithBody(strings.NewReader(command)),
				client.Search.WithIndex("_all"),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to execute search: %w", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
			}

			var searchResult map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
				return nil, fmt.Errorf("failed to decode search response: %w", err)
			}

			// Extract hits
			if hits, ok := searchResult["hits"].(map[string]interface{}); ok {
				if hitsList, ok := hits["hits"].([]interface{}); ok {
					for _, hit := range hitsList {
						if hitMap, ok := hit.(map[string]interface{}); ok {
							if len(columnNames) == 0 {
								// Extract column names from first hit
								for key := range hitMap {
									columnNames = append(columnNames, key)
								}
							}
							results = append(results, hitMap)
						}
					}
				}
			}
		} else {
			return nil, fmt.Errorf("unsupported command for Elasticsearch: %s", command)
		}
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

// CreateDatabase creates a new Elasticsearch index (equivalent to a database) with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client

	// Build the index configuration
	indexConfig := make(map[string]interface{})

	// Parse and apply options
	if settings, ok := options["settings"]; ok {
		indexConfig["settings"] = settings
	}

	if mappings, ok := options["mappings"]; ok {
		indexConfig["mappings"] = mappings
	}

	if aliases, ok := options["aliases"]; ok {
		indexConfig["aliases"] = aliases
	}

	// Set default settings if none provided
	if len(indexConfig) == 0 {
		indexConfig["settings"] = map[string]interface{}{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		}
	}

	// Convert to JSON
	var body strings.Builder
	if len(indexConfig) > 0 {
		jsonData, err := json.Marshal(indexConfig)
		if err != nil {
			return fmt.Errorf("failed to marshal index configuration: %w", err)
		}
		body.WriteString(string(jsonData))
	}

	// Create the index
	res, err := client.Indices.Create(
		databaseName,
		client.Indices.Create.WithContext(ctx),
		client.Indices.Create.WithBody(strings.NewReader(body.String())),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	return nil
}

// DropDatabase drops an Elasticsearch index (equivalent to dropping a database) with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	esClient, ok := db.(*ElasticsearchClient)
	if !ok {
		return fmt.Errorf("invalid elasticsearch connection type")
	}

	client := esClient.Client

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		// Check if index exists first
		res, err := client.Indices.Exists([]string{databaseName}, client.Indices.Exists.WithContext(ctx))
		if err != nil {
			return fmt.Errorf("failed to check if index exists: %w", err)
		}
		res.Body.Close()

		if res.StatusCode == 404 {
			// Index doesn't exist, but that's OK with if_exists
			return nil
		}
	}

	// Delete the index
	res, err := client.Indices.Delete([]string{databaseName}, client.Indices.Delete.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	return nil
}
