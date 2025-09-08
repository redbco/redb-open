package dynamodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to a DynamoDB database
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

	// Build AWS config
	awsConfig, err := buildAWSConfig(cfg, decryptedPassword)
	if err != nil {
		return nil, fmt.Errorf("error building AWS config: %v", err)
	}

	// Create DynamoDB client
	client := dynamodb.NewFromConfig(awsConfig)

	// Test the connection by listing tables (limited to 1 for testing)
	_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{
		Limit: aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("error testing DynamoDB connection: %v", err)
	}

	return &dbclient.DatabaseClient{
		DB:           client,
		DatabaseType: "dynamodb",
		DatabaseID:   cfg.DatabaseID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a DynamoDB instance
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

	// Build AWS config
	awsConfig, err := buildAWSConfig(dbConfig, decryptedPassword)
	if err != nil {
		return nil, fmt.Errorf("error building AWS config: %v", err)
	}

	// Create DynamoDB client
	client := dynamodb.NewFromConfig(awsConfig)

	// Test the connection
	_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{
		Limit: aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("error testing DynamoDB connection: %v", err)
	}

	return &dbclient.InstanceClient{
		DB:           client,
		InstanceType: "dynamodb",
		InstanceID:   cfg.InstanceID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// buildAWSConfig creates AWS configuration for DynamoDB connection
func buildAWSConfig(cfg dbclient.DatabaseConfig, secretKey string) (aws.Config, error) {
	// For DynamoDB, we use the Username as Access Key ID and Password as Secret Access Key
	// Region can be specified in the DatabaseName field or Host field
	region := cfg.DatabaseName
	if region == "" {
		region = cfg.Host
	}
	if region == "" {
		region = "us-east-1" // Default region
	}

	// Parse endpoint if custom endpoint is specified (for DynamoDB Local)
	var endpointResolver aws.EndpointResolverWithOptionsFunc
	if cfg.Host != "" && !strings.Contains(cfg.Host, "amazonaws.com") {
		// Custom endpoint (e.g., DynamoDB Local)
		endpoint := fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
		if cfg.SSL {
			endpoint = fmt.Sprintf("https://%s:%d", cfg.Host, cfg.Port)
		}

		endpointResolver = func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           endpoint,
				SigningRegion: region,
			}, nil
		}
	}

	// Build AWS config
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Username, // Access Key ID
			secretKey,    // Secret Access Key
			"",           // Session Token (empty for long-term credentials)
		)),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("error loading AWS config: %v", err)
	}

	// Apply custom endpoint resolver if specified
	if endpointResolver != nil {
		awsCfg.EndpointResolverWithOptions = endpointResolver
	}

	return awsCfg, nil
}

// DiscoverDetails fetches database details
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, fmt.Errorf("invalid DynamoDB client type")
	}

	// Get basic information about DynamoDB
	// Note: DynamoDB doesn't have a direct way to get version info,
	// so we'll use service limits as a way to test connectivity
	_, err := client.DescribeLimits(context.Background(), &dynamodb.DescribeLimitsInput{})
	if err != nil {
		return nil, fmt.Errorf("error testing DynamoDB connection: %v", err)
	}

	// Calculate total database size by summing all table sizes
	var totalSize int64
	listTablesResult, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %v", err)
	}

	for _, tableName := range listTablesResult.TableNames {
		describeResult, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err == nil && describeResult.Table.TableSizeBytes != nil {
			totalSize += *describeResult.Table.TableSizeBytes
		}
	}

	details := make(map[string]interface{})
	details["uniqueIdentifier"] = "dynamodb-instance" // DynamoDB doesn't have unique instance IDs
	details["databaseType"] = "dynamodb"
	details["databaseEdition"] = "AWS DynamoDB"
	details["version"] = "latest" // DynamoDB is always latest version
	details["databaseSize"] = totalSize
	details["region"] = "unknown"      // Would need to be extracted from config
	details["billingMode"] = "unknown" // Would need to check each table

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, fmt.Errorf("invalid DynamoDB client type")
	}

	metadata := make(map[string]interface{})

	// Get table count
	listTablesResult, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("error listing tables: %v", err)
	}

	metadata["table_count"] = len(listTablesResult.TableNames)
	metadata["table_names"] = listTablesResult.TableNames
	metadata["database_type"] = "dynamodb"

	// Get total size and item counts
	var totalSize, totalItems int64
	tableDetails := make([]map[string]interface{}, 0)

	for _, tableName := range listTablesResult.TableNames {
		describeResult, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			continue // Skip tables we can't describe
		}

		table := describeResult.Table
		tableDetail := map[string]interface{}{
			"name":         *table.TableName,
			"status":       string(table.TableStatus),
			"billing_mode": string(table.BillingModeSummary.BillingMode),
		}

		if table.TableSizeBytes != nil {
			tableDetail["size_bytes"] = *table.TableSizeBytes
			totalSize += *table.TableSizeBytes
		}

		if table.ItemCount != nil {
			tableDetail["item_count"] = *table.ItemCount
			totalItems += *table.ItemCount
		}

		tableDetails = append(tableDetails, tableDetail)
	}

	metadata["total_size_bytes"] = totalSize
	metadata["total_items"] = totalItems
	metadata["tables"] = tableDetails

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a database instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, fmt.Errorf("invalid DynamoDB client type")
	}

	metadata := make(map[string]interface{})

	// Get service limits
	limitsResult, err := client.DescribeLimits(ctx, &dynamodb.DescribeLimitsInput{})
	if err != nil {
		return nil, fmt.Errorf("error getting service limits: %v", err)
	}

	metadata["instance_type"] = "dynamodb"
	metadata["max_table_read_capacity"] = limitsResult.TableMaxReadCapacityUnits
	metadata["max_table_write_capacity"] = limitsResult.TableMaxWriteCapacityUnits
	metadata["max_account_read_capacity"] = limitsResult.AccountMaxReadCapacityUnits
	metadata["max_account_write_capacity"] = limitsResult.AccountMaxWriteCapacityUnits

	// Get table count for this instance
	listTablesResult, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err == nil {
		metadata["table_count"] = len(listTablesResult.TableNames)
	}

	return metadata, nil
}

// ExecuteCommand executes a command on a database
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	// DynamoDB doesn't support SQL-like commands
	// This is a placeholder that could be extended to support specific DynamoDB operations
	return nil, fmt.Errorf("DynamoDB does not support SQL-like command execution. Command: %s", command)
}

// CreateDatabase creates a new database
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	// DynamoDB doesn't have the concept of creating databases like SQL databases
	// In DynamoDB, you work with tables directly within your AWS account/region
	return fmt.Errorf("DynamoDB does not support database creation. Tables are created directly in the service")
}

// DropDatabase drops a database
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return fmt.Errorf("invalid DynamoDB client type")
	}

	// For DynamoDB, "dropping a database" means deleting all tables
	// This is equivalent to the WipeDatabase function
	listTablesResult, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("error listing tables: %v", err)
	}

	for _, tableName := range listTablesResult.TableNames {
		_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return fmt.Errorf("error deleting table %s: %v", tableName, err)
		}
	}

	return nil
}
