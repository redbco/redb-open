package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

// ExecuteQuery executes a query on DynamoDB and returns results as a slice of maps
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, fmt.Errorf("invalid dynamodb connection type")
	}

	ctx := context.Background()

	// Parse query to extract table information and operation
	// Expected format: {"table": "table_name", "operation": "scan|query", "key_condition": "...", "filter": "..."}
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse dynamodb query: %w", err)
	}

	tableName, ok := queryReq["table"].(string)
	if !ok {
		return nil, fmt.Errorf("table name is required in dynamodb query")
	}

	operation, ok := queryReq["operation"].(string)
	if !ok {
		operation = "scan" // Default to scan
	}

	var results []interface{}

	switch operation {
	case "scan":
		// Execute scan operation
		scanInput := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		}

		// Add filter expression if provided
		if filter, ok := queryReq["filter"].(string); ok && filter != "" {
			scanInput.FilterExpression = aws.String(filter)
		}

		scanPaginator := dynamodb.NewScanPaginator(client, scanInput)
		for scanPaginator.HasMorePages() {
			output, err := scanPaginator.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to execute dynamodb scan: %w", err)
			}

			for _, item := range output.Items {
				// Convert DynamoDB item to map[string]interface{}
				result := make(map[string]interface{})
				for key, value := range item {
					result[key] = convertAttributeValue(value)
				}
				results = append(results, result)
			}
		}

	case "query":
		// Execute query operation (requires key condition)
		keyCondition, ok := queryReq["key_condition"].(string)
		if !ok || keyCondition == "" {
			return nil, fmt.Errorf("key_condition is required for dynamodb query operation")
		}

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String(keyCondition),
		}

		// Add filter expression if provided
		if filter, ok := queryReq["filter"].(string); ok && filter != "" {
			queryInput.FilterExpression = aws.String(filter)
		}

		queryPaginator := dynamodb.NewQueryPaginator(client, queryInput)
		for queryPaginator.HasMorePages() {
			output, err := queryPaginator.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to execute dynamodb query: %w", err)
			}

			for _, item := range output.Items {
				// Convert DynamoDB item to map[string]interface{}
				result := make(map[string]interface{})
				for key, value := range item {
					result[key] = convertAttributeValue(value)
				}
				results = append(results, result)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported operation: %s", operation)
	}

	return results, nil
}

// ExecuteCountQuery executes a count query on DynamoDB and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return 0, fmt.Errorf("invalid dynamodb connection type")
	}

	ctx := context.Background()

	// Parse query to extract table information
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse dynamodb count query: %w", err)
	}

	tableName, ok := queryReq["table"].(string)
	if !ok {
		return 0, fmt.Errorf("table name is required in dynamodb count query")
	}

	operation, ok := queryReq["operation"].(string)
	if !ok {
		operation = "scan" // Default to scan
	}

	var totalCount int64

	switch operation {
	case "scan":
		scanInput := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
			Select:    "COUNT",
		}

		// Add filter expression if provided
		if filter, ok := queryReq["filter"].(string); ok && filter != "" {
			scanInput.FilterExpression = aws.String(filter)
		}

		scanPaginator := dynamodb.NewScanPaginator(client, scanInput)
		for scanPaginator.HasMorePages() {
			output, err := scanPaginator.NextPage(ctx)
			if err != nil {
				return 0, fmt.Errorf("failed to execute dynamodb count scan: %w", err)
			}
			totalCount += int64(output.Count)
		}

	case "query":
		keyCondition, ok := queryReq["key_condition"].(string)
		if !ok || keyCondition == "" {
			return 0, fmt.Errorf("key_condition is required for dynamodb query operation")
		}

		queryInput := &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String(keyCondition),
			Select:                 "COUNT",
		}

		// Add filter expression if provided
		if filter, ok := queryReq["filter"].(string); ok && filter != "" {
			queryInput.FilterExpression = aws.String(filter)
		}

		queryPaginator := dynamodb.NewQueryPaginator(client, queryInput)
		for queryPaginator.HasMorePages() {
			output, err := queryPaginator.NextPage(ctx)
			if err != nil {
				return 0, fmt.Errorf("failed to execute dynamodb count query: %w", err)
			}
			totalCount += int64(output.Count)
		}

	default:
		return 0, fmt.Errorf("unsupported operation: %s", operation)
	}

	return totalCount, nil
}

// StreamTableData streams items from a DynamoDB table in batches for efficient data copying
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid dynamodb connection type")
	}

	ctx := context.Background()

	// Build scan input
	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(batchSize),
	}

	// Add projection expression for specific columns if requested
	if len(columns) > 0 {
		projectionExpression := strings.Join(columns, ", ")
		scanInput.ProjectionExpression = aws.String(projectionExpression)
	}

	// Handle cursor-based pagination
	// In DynamoDB streaming, offset should actually be a base64-encoded cursor
	var exclusiveStartKey map[string]types.AttributeValue
	if offset > 0 {
		// Try to decode offset as a cursor first
		cursorStr := fmt.Sprintf("%d", offset)
		if decodedCursor, err := base64.StdEncoding.DecodeString(cursorStr); err == nil {
			if err := json.Unmarshal(decodedCursor, &exclusiveStartKey); err == nil {
				scanInput.ExclusiveStartKey = exclusiveStartKey
			}
		}

		// If cursor decoding fails, fall back to scan-based offset (less efficient)
		if exclusiveStartKey == nil && offset > 0 {
			// Perform initial scan to reach the offset position
			tempScanInput := &dynamodb.ScanInput{
				TableName: aws.String(tableName),
				Select:    types.SelectAllAttributes,
			}

			itemsSkipped := int64(0)
			tempScanPaginator := dynamodb.NewScanPaginator(client, tempScanInput)

			for tempScanPaginator.HasMorePages() && itemsSkipped < offset {
				tempOutput, err := tempScanPaginator.NextPage(ctx)
				if err != nil {
					return nil, false, "", fmt.Errorf("failed to skip to offset in dynamodb: %w", err)
				}

				itemsSkipped += int64(tempOutput.Count)
				if itemsSkipped >= offset {
					exclusiveStartKey = tempOutput.LastEvaluatedKey
					break
				}
			}

			if exclusiveStartKey != nil {
				scanInput.ExclusiveStartKey = exclusiveStartKey
			}
		}
	}

	// Execute scan
	output, err := client.Scan(ctx, scanInput)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute dynamodb scan: %w", err)
	}

	// Convert results
	var results []map[string]interface{}
	for _, item := range output.Items {
		result := make(map[string]interface{})
		for key, value := range item {
			result[key] = convertAttributeValue(value)
		}
		results = append(results, result)
	}

	rowCount := len(results)
	isComplete := output.LastEvaluatedKey == nil || rowCount < int(batchSize)

	// Encode LastEvaluatedKey as cursor for proper pagination
	nextCursorValue := ""
	if output.LastEvaluatedKey != nil {
		cursorData, err := json.Marshal(output.LastEvaluatedKey)
		if err == nil {
			nextCursorValue = base64.StdEncoding.EncodeToString(cursorData)
		}
	}

	return results, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of items in a DynamoDB table
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	client, ok := db.(*dynamodb.Client)
	if !ok {
		return 0, false, fmt.Errorf("invalid dynamodb connection type")
	}

	ctx := context.Background()

	// Get table description to get item count
	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	output, err := client.DescribeTable(ctx, describeInput)
	if err != nil {
		return 0, false, fmt.Errorf("failed to describe dynamodb table: %w", err)
	}

	// DynamoDB provides an approximate item count
	itemCount := int64(0)
	if output.Table != nil && output.Table.ItemCount != nil {
		itemCount = *output.Table.ItemCount
	}

	// The count from DescribeTable is approximate, not exact
	return itemCount, true, nil
}

// convertAttributeValue converts a DynamoDB AttributeValue to a Go interface{}
func convertAttributeValue(av types.AttributeValue) interface{} {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		return v.Value
	case *types.AttributeValueMemberM:
		result := make(map[string]interface{})
		for key, value := range v.Value {
			result[key] = convertAttributeValue(value)
		}
		return result
	case *types.AttributeValueMemberL:
		result := make([]interface{}, len(v.Value))
		for i, value := range v.Value {
			result[i] = convertAttributeValue(value)
		}
		return result
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberBOOL:
		return v.Value
	default:
		return nil
	}
}
