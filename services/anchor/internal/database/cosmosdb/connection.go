package cosmosdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to a CosmosDB database
func Connect(cfg dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
	var decryptedPassword string
	if cfg.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(cfg.TenantID, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
		decryptedPassword = dp
	}

	// Build endpoint URL
	endpoint := buildEndpointURL(cfg)

	// Create credential using the primary key
	credential, err := azcosmos.NewKeyCredential(decryptedPassword)
	if err != nil {
		return nil, fmt.Errorf("error creating CosmosDB credential: %v", err)
	}

	// Create client options
	options := &azcosmos.ClientOptions{}
	if cfg.SSL {
		// SSL is handled automatically by the Azure SDK
		// Additional SSL configuration would go here if needed
	}

	// Create CosmosDB client
	client, err := azcosmos.NewClientWithKey(endpoint, credential, options)
	if err != nil {
		return nil, fmt.Errorf("error creating CosmosDB client: %v", err)
	}

	// Test the connection by getting database properties
	if cfg.DatabaseName != "" {
		databaseClient, err := client.NewDatabase(cfg.DatabaseName)
		if err != nil {
			return nil, fmt.Errorf("error creating database client: %v", err)
		}
		_, err = databaseClient.Read(context.Background(), nil)
		if err != nil {
			return nil, fmt.Errorf("error testing CosmosDB connection: %v", err)
		}
	}

	return &dbclient.DatabaseClient{
		DB:           client,
		DatabaseType: "cosmosdb",
		DatabaseID:   cfg.DatabaseID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a CosmosDB instance
func ConnectInstance(cfg dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
	var decryptedPassword string
	if cfg.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(cfg.TenantID, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
		decryptedPassword = dp
	}

	// Convert instance config to database config for reuse
	dbConfig := dbclient.DatabaseConfig{
		DatabaseID:     cfg.InstanceID,
		WorkspaceID:    cfg.WorkspaceID,
		TenantID:       cfg.TenantID,
		EnvironmentID:  cfg.EnvironmentID,
		Name:           cfg.Name,
		Description:    cfg.Description,
		DatabaseVendor: cfg.DatabaseVendor,
		ConnectionType: cfg.ConnectionType,
		Host:           cfg.Host,
		Port:           cfg.Port,
		Username:       cfg.Username,
		Password:       cfg.Password,
		DatabaseName:   cfg.DatabaseName,
		SSL:            cfg.SSL,
		SSLMode:        cfg.SSLMode,
		SSLCert:        cfg.SSLCert,
		SSLKey:         cfg.SSLKey,
		SSLRootCert:    cfg.SSLRootCert,
		Role:           cfg.Role,
	}

	// Build endpoint URL
	endpoint := buildEndpointURL(dbConfig)

	// Create credential using the primary key
	credential, err := azcosmos.NewKeyCredential(decryptedPassword)
	if err != nil {
		return nil, fmt.Errorf("error creating CosmosDB credential: %v", err)
	}

	// Create client options
	options := &azcosmos.ClientOptions{}

	// Create CosmosDB client
	client, err := azcosmos.NewClientWithKey(endpoint, credential, options)
	if err != nil {
		return nil, fmt.Errorf("error creating CosmosDB client: %v", err)
	}

	// Test the connection
	databaseClient, err := client.NewDatabase("master")
	if err != nil {
		return nil, fmt.Errorf("error creating database client: %v", err)
	}
	_, err = databaseClient.Read(context.Background(), &azcosmos.ReadDatabaseOptions{})
	if err != nil {
		// If master database doesn't exist or we can't read it, try listing databases instead
		_, err = listDatabases(client)
		if err != nil {
			return nil, fmt.Errorf("error testing CosmosDB connection: %v", err)
		}
	}

	return &dbclient.InstanceClient{
		DB:           client,
		InstanceType: "cosmosdb",
		InstanceID:   cfg.InstanceID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// buildEndpointURL constructs the CosmosDB endpoint URL
func buildEndpointURL(cfg dbclient.DatabaseConfig) string {
	// CosmosDB endpoint format: https://{account}.documents.azure.com:443/
	// The account name can be extracted from the Host field
	host := cfg.Host

	// If host doesn't contain the full URL, construct it
	if !strings.HasPrefix(host, "https://") && !strings.HasPrefix(host, "http://") {
		// Remove any .documents.azure.com suffix to get just the account name
		accountName := strings.TrimSuffix(host, ".documents.azure.com")

		// Build standard CosmosDB endpoint
		if cfg.SSL || cfg.Port == 443 {
			host = fmt.Sprintf("https://%s.documents.azure.com:443/", accountName)
		} else {
			host = fmt.Sprintf("http://%s.documents.azure.com:%d/", accountName, cfg.Port)
		}
	}

	return host
}

// listDatabases lists all databases in the CosmosDB account (helper function for testing connection)
func listDatabases(client *azcosmos.Client) ([]string, error) {
	ctx := context.Background()

	// Create a query to list databases
	queryPager := client.NewQueryDatabasesPager("SELECT * FROM c", nil)

	var databases []string
	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, item := range response.Databases {
			databases = append(databases, item.ID)
		}
	}

	return databases, nil
}

// ExecuteQuery executes a SQL query on CosmosDB and returns results as a slice of maps
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return nil, fmt.Errorf("invalid cosmosdb connection type")
	}

	ctx := context.Background()

	// Parse query to extract database and container information
	// Expected format: {"database": "db_name", "container": "container_name", "query": "SELECT * FROM c"}
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse cosmosdb query: %w", err)
	}

	databaseName, ok := queryReq["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database name is required in cosmosdb query")
	}

	containerName, ok := queryReq["container"].(string)
	if !ok {
		return nil, fmt.Errorf("container name is required in cosmosdb query")
	}

	sqlQuery, ok := queryReq["query"].(string)
	if !ok {
		return nil, fmt.Errorf("SQL query is required in cosmosdb query")
	}

	// Get container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get cosmosdb container: %w", err)
	}

	// Execute query
	queryPager := containerClient.NewQueryItemsPager(sqlQuery, azcosmos.PartitionKey{}, nil)

	var results []interface{}
	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to execute cosmosdb query: %w", err)
		}

		for _, item := range response.Items {
			var document map[string]interface{}
			if err := json.Unmarshal(item, &document); err != nil {
				continue
			}
			results = append(results, document)
		}
	}

	return results, nil
}

// ExecuteCountQuery executes a count query on CosmosDB and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return 0, fmt.Errorf("invalid cosmosdb connection type")
	}

	ctx := context.Background()

	// Parse query to extract database and container information
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse cosmosdb count query: %w", err)
	}

	databaseName, ok := queryReq["database"].(string)
	if !ok {
		return 0, fmt.Errorf("database name is required in cosmosdb count query")
	}

	containerName, ok := queryReq["container"].(string)
	if !ok {
		return 0, fmt.Errorf("container name is required in cosmosdb count query")
	}

	// Get container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return 0, fmt.Errorf("failed to get cosmosdb container: %w", err)
	}

	// Execute count query
	countQuery := "SELECT VALUE COUNT(1) FROM c"
	if whereClause, ok := queryReq["where"]; ok && whereClause != "" {
		countQuery = fmt.Sprintf("SELECT VALUE COUNT(1) FROM c WHERE %s", whereClause)
	}

	queryPager := containerClient.NewQueryItemsPager(countQuery, azcosmos.PartitionKey{}, nil)

	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to execute cosmosdb count query: %w", err)
		}

		if len(response.Items) > 0 {
			var count int64
			if err := json.Unmarshal(response.Items[0], &count); err != nil {
				return 0, fmt.Errorf("failed to parse count result: %w", err)
			}
			return count, nil
		}
	}

	return 0, nil
}

// StreamTableData streams documents from a CosmosDB container in batches for efficient data copying
// For CosmosDB, tableName represents the container name
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid cosmosdb connection type")
	}

	ctx := context.Background()

	// Parse tableName to extract database and container
	// Expected format: "database.container"
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, false, "", fmt.Errorf("tableName must be in format 'database.container'")
	}

	databaseName := parts[0]
	containerName := parts[1]

	// Get container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to get cosmosdb container: %w", err)
	}

	// Build query with OFFSET and LIMIT
	query := "SELECT * FROM c"
	if len(columns) > 0 {
		// Build SELECT clause with specific columns
		columnList := strings.Join(columns, ", c.")
		query = fmt.Sprintf("SELECT c.%s FROM c", columnList)
	}
	query = fmt.Sprintf("%s OFFSET %d LIMIT %d", query, offset, batchSize)

	// Execute streaming query
	queryPager := containerClient.NewQueryItemsPager(query, azcosmos.PartitionKey{}, nil)

	var results []map[string]interface{}
	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return nil, false, "", fmt.Errorf("failed to execute cosmosdb streaming query: %w", err)
		}

		for _, item := range response.Items {
			var document map[string]interface{}
			if err := json.Unmarshal(item, &document); err != nil {
				continue
			}
			results = append(results, document)
		}
	}

	rowCount := len(results)
	isComplete := rowCount < int(batchSize)

	// For simple offset-based pagination, we don't use cursor values
	nextCursorValue := ""

	return results, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of documents in a CosmosDB container
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	client, ok := db.(*azcosmos.Client)
	if !ok {
		return 0, false, fmt.Errorf("invalid cosmosdb connection type")
	}

	ctx := context.Background()

	// Parse tableName to extract database and container
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return 0, false, fmt.Errorf("tableName must be in format 'database.container'")
	}

	databaseName := parts[0]
	containerName := parts[1]

	// Get container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get cosmosdb container: %w", err)
	}

	// Build count query
	countQuery := "SELECT VALUE COUNT(1) FROM c"
	if whereClause != "" {
		countQuery = fmt.Sprintf("SELECT VALUE COUNT(1) FROM c WHERE %s", whereClause)
	}

	// Execute count query
	queryPager := containerClient.NewQueryItemsPager(countQuery, azcosmos.PartitionKey{}, nil)

	for queryPager.More() {
		response, err := queryPager.NextPage(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("failed to execute cosmosdb count query: %w", err)
		}

		if len(response.Items) > 0 {
			var count int64
			if err := json.Unmarshal(response.Items[0], &count); err != nil {
				return 0, false, fmt.Errorf("failed to parse count result: %w", err)
			}
			// CosmosDB count is always exact, not an estimate
			return count, false, nil
		}
	}

	return 0, false, nil
}
