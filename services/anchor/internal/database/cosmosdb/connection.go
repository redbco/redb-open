package cosmosdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to a CosmosDB database
func Connect(cfg common.DatabaseConfig) (*common.DatabaseClient, error) {
	decryptedPassword, err := encryption.DecryptPassword(cfg.TenantID, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
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

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "cosmosdb",
		DatabaseID:   cfg.DatabaseID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a CosmosDB instance
func ConnectInstance(cfg common.InstanceConfig) (*common.InstanceClient, error) {
	decryptedPassword, err := encryption.DecryptPassword(cfg.TenantID, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Convert instance config to database config for reuse
	dbConfig := common.DatabaseConfig{
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

	return &common.InstanceClient{
		DB:           client,
		InstanceType: "cosmosdb",
		InstanceID:   cfg.InstanceID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// buildEndpointURL constructs the CosmosDB endpoint URL
func buildEndpointURL(cfg common.DatabaseConfig) string {
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
