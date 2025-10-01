package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to an Apache Iceberg catalog
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
	var decryptedPassword string
	if config.Password != "" {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
		decryptedPassword = dp
	}

	// Parse connection string to determine catalog type and configuration
	catalogConfig, err := parseConnectionString(config, decryptedPassword)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string: %v", err)
	}

	// Create Iceberg client based on catalog type
	client, err := createIcebergClient(catalogConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating Iceberg client: %v", err)
	}

	// Test the connection
	if err := testConnection(client); err != nil {
		return nil, fmt.Errorf("error testing Iceberg connection: %v", err)
	}

	return &dbclient.DatabaseClient{
		DB:           client,
		DatabaseType: "iceberg",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to an Apache Iceberg catalog instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
	var decryptedPassword string
	if config.Password != "" {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
		decryptedPassword = dp
	}

	// Convert InstanceConfig to DatabaseConfig for connection
	dbConfig := dbclient.DatabaseConfig{
		DatabaseID:     config.InstanceID,
		TenantID:       config.TenantID,
		WorkspaceID:    config.WorkspaceID,
		EnvironmentID:  config.EnvironmentID,
		ConnectionType: config.ConnectionType,
		Host:           config.Host,
		Port:           config.Port,
		Username:       config.Username,
		Password:       config.Password,
		DatabaseName:   config.DatabaseName,
		SSL:            config.SSL,
		SSLMode:        config.SSLMode,
		SSLCert:        config.SSLCert,
		SSLKey:         config.SSLKey,
		SSLRootCert:    config.SSLRootCert,
	}

	// Parse connection string to determine catalog type and configuration
	catalogConfig, err := parseConnectionString(dbConfig, decryptedPassword)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string: %v", err)
	}

	// Create Iceberg client based on catalog type
	client, err := createIcebergClient(catalogConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating Iceberg client: %v", err)
	}

	// Test the connection
	if err := testConnection(client); err != nil {
		return nil, fmt.Errorf("error testing Iceberg connection: %v", err)
	}

	return &dbclient.InstanceClient{
		DB:           client,
		InstanceType: "iceberg",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// parseConnectionString parses the Iceberg connection string and returns catalog configuration
func parseConnectionString(config dbclient.DatabaseConfig, decryptedPassword string) (map[string]interface{}, error) {
	// Parse URL parameters from connection string template
	// iceberg://{username}:{password}@{host}:{port}/{database}?catalog={catalog}&warehouse={warehouse}

	catalogConfig := make(map[string]interface{})

	// Determine catalog type based on host or URL scheme
	if strings.Contains(config.Host, "rest") || strings.Contains(config.Host, "api") {
		catalogConfig["type"] = "rest"
		catalogConfig["uri"] = fmt.Sprintf("http%s://%s:%d",
			map[bool]string{true: "s", false: ""}[config.SSL],
			config.Host, config.Port)
	} else if strings.Contains(config.Host, "hive") || strings.Contains(config.Host, "metastore") {
		catalogConfig["type"] = "hive"
		catalogConfig["uri"] = fmt.Sprintf("thrift://%s:%d", config.Host, config.Port)
	} else {
		// Default to Hadoop catalog for file-based catalogs
		catalogConfig["type"] = "hadoop"
	}

	// Set common properties
	catalogConfig["catalog"] = config.DatabaseName
	if config.Username != "" {
		catalogConfig["username"] = config.Username
	}
	if decryptedPassword != "" {
		catalogConfig["password"] = decryptedPassword
	}

	// Parse additional properties from connection string or config
	// This would typically come from URL parameters or additional config
	properties := make(map[string]string)

	// Add warehouse location (this would typically come from URL parameters)
	if warehouse := getWarehouseFromConfig(config); warehouse != "" {
		properties["warehouse"] = warehouse
	}

	// Add SSL configuration
	if config.SSL {
		properties["ssl"] = "true"
		if config.SSLCert != "" {
			properties["ssl.cert"] = config.SSLCert
		}
		if config.SSLKey != "" {
			properties["ssl.key"] = config.SSLKey
		}
		if config.SSLRootCert != "" {
			properties["ssl.ca"] = config.SSLRootCert
		}
	}

	catalogConfig["properties"] = properties

	return catalogConfig, nil
}

// getWarehouseFromConfig extracts warehouse path from config
// In a real implementation, this would parse URL parameters
func getWarehouseFromConfig(config dbclient.DatabaseConfig) string {
	// This is a simplified implementation
	// In practice, you'd parse the connection string URL parameters
	// For now, we'll use a default based on the database name
	if config.DatabaseName != "" {
		return fmt.Sprintf("s3://iceberg-warehouse/%s", config.DatabaseName)
	}
	return "s3://iceberg-warehouse/default"
}

// createIcebergClient creates an Iceberg client based on catalog configuration
func createIcebergClient(catalogConfig map[string]interface{}) (*IcebergClient, error) {
	catalogType, ok := catalogConfig["type"].(string)
	if !ok {
		return nil, fmt.Errorf("catalog type not specified")
	}

	client := &IcebergClient{
		CatalogType: catalogType,
		Properties:  make(map[string]string),
	}

	// Set properties
	if props, ok := catalogConfig["properties"].(map[string]string); ok {
		client.Properties = props
	}

	switch catalogType {
	case "rest":
		return createRestCatalogClient(client, catalogConfig)
	case "hive":
		return createHiveCatalogClient(client, catalogConfig)
	case "hadoop":
		return createHadoopCatalogClient(client, catalogConfig)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", catalogType)
	}
}

// createRestCatalogClient creates a REST catalog client
func createRestCatalogClient(client *IcebergClient, config map[string]interface{}) (*IcebergClient, error) {
	uri, ok := config["uri"].(string)
	if !ok {
		return nil, fmt.Errorf("REST catalog URI not specified")
	}

	client.BaseURL = uri
	client.CatalogName = getStringFromConfig(config, "catalog", "default")

	// Create HTTP client with timeout
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Add authentication if provided
	if username := getStringFromConfig(config, "username", ""); username != "" {
		// In a real implementation, you'd set up proper authentication
		// This could be basic auth, bearer token, etc.
	}

	client.HTTPClient = httpClient

	return client, nil
}

// createHiveCatalogClient creates a Hive metastore catalog client
func createHiveCatalogClient(client *IcebergClient, config map[string]interface{}) (*IcebergClient, error) {
	uri, ok := config["uri"].(string)
	if !ok {
		return nil, fmt.Errorf("hive metastore URI not specified")
	}

	client.BaseURL = uri
	client.CatalogName = getStringFromConfig(config, "catalog", "default")
	client.WarehousePath = getStringFromConfig(config, "warehouse", "")

	// In a real implementation, you'd create a Hive metastore client here
	// For now, we'll just store the configuration

	return client, nil
}

// createHadoopCatalogClient creates a Hadoop catalog client
func createHadoopCatalogClient(client *IcebergClient, config map[string]interface{}) (*IcebergClient, error) {
	client.CatalogName = getStringFromConfig(config, "catalog", "default")
	client.WarehousePath = getStringFromConfig(config, "warehouse", "")

	if client.WarehousePath == "" {
		return nil, fmt.Errorf("warehouse path required for Hadoop catalog")
	}

	// In a real implementation, you'd set up Hadoop configuration here

	return client, nil
}

// getStringFromConfig safely gets a string value from config map
func getStringFromConfig(config map[string]interface{}, key, defaultValue string) string {
	if value, ok := config[key].(string); ok {
		return value
	}
	return defaultValue
}

// testConnection tests the Iceberg catalog connection
func testConnection(client *IcebergClient) error {
	switch client.CatalogType {
	case "rest":
		return testRestCatalogConnection(client)
	case "hive":
		return testHiveCatalogConnection(client)
	case "hadoop":
		return testHadoopCatalogConnection(client)
	default:
		return fmt.Errorf("unsupported catalog type for connection test: %s", client.CatalogType)
	}
}

// testRestCatalogConnection tests REST catalog connection
func testRestCatalogConnection(client *IcebergClient) error {
	if client.HTTPClient == nil {
		return fmt.Errorf("HTTP client not initialized")
	}

	httpClient, ok := client.HTTPClient.(*http.Client)
	if !ok {
		return fmt.Errorf("invalid HTTP client type")
	}

	// Test connection by calling the config endpoint
	configURL := fmt.Sprintf("%s/v1/config", strings.TrimSuffix(client.BaseURL, "/"))

	req, err := http.NewRequest("GET", configURL, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REST catalog returned status %d", resp.StatusCode)
	}

	return nil
}

// testHiveCatalogConnection tests Hive metastore connection
func testHiveCatalogConnection(client *IcebergClient) error {
	// In a real implementation, you'd test the Hive metastore connection
	// For now, we'll just validate the configuration
	if client.BaseURL == "" {
		return fmt.Errorf("hive metastore URI not configured")
	}
	return nil
}

// testHadoopCatalogConnection tests Hadoop catalog connection
func testHadoopCatalogConnection(client *IcebergClient) error {
	// In a real implementation, you'd test access to the warehouse path
	// For now, we'll just validate the configuration
	if client.WarehousePath == "" {
		return fmt.Errorf("warehouse path not configured")
	}
	return nil
}

// DiscoverDetails fetches the details of an Iceberg catalog
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "iceberg"
	details["databaseEdition"] = "Apache Iceberg"
	details["catalogName"] = client.CatalogName
	details["catalogType"] = client.CatalogType
	details["warehousePath"] = client.WarehousePath
	details["uniqueIdentifier"] = fmt.Sprintf("iceberg-%s-%s", client.CatalogType, client.CatalogName)

	// Determine storage backend from warehouse path
	if strings.HasPrefix(client.WarehousePath, "s3://") {
		details["storageBackend"] = "S3"
	} else if strings.HasPrefix(client.WarehousePath, "gs://") {
		details["storageBackend"] = "GCS"
	} else if strings.HasPrefix(client.WarehousePath, "abfs://") || strings.HasPrefix(client.WarehousePath, "adl://") {
		details["storageBackend"] = "Azure"
	} else if strings.HasPrefix(client.WarehousePath, "hdfs://") {
		details["storageBackend"] = "HDFS"
	} else {
		details["storageBackend"] = "Local/Other"
	}

	// Get version information (this would require actual API calls)
	details["version"] = "1.0.0" // Placeholder

	return details, nil
}

// CollectDatabaseMetadata collects metadata from an Iceberg catalog
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type")
	}

	metadata := make(map[string]interface{})

	metadata["catalog_name"] = client.CatalogName
	metadata["catalog_type"] = client.CatalogType
	metadata["warehouse_path"] = client.WarehousePath
	metadata["base_url"] = client.BaseURL
	metadata["properties"] = client.Properties

	// In a real implementation, you'd collect actual metadata from the catalog
	metadata["namespace_count"] = 0
	metadata["table_count"] = 0
	metadata["total_data_size"] = int64(0)

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from an Iceberg catalog instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	return CollectDatabaseMetadata(ctx, db)
}

// ExecuteCommand executes a command on an Iceberg catalog and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return nil, fmt.Errorf("invalid database connection type")
	}

	// Parse and execute the command
	// This is a simplified implementation - in practice, you'd need to parse
	// SQL commands and translate them to Iceberg API calls

	result := make(map[string]interface{})
	result["catalog"] = client.CatalogName
	result["command"] = command
	result["status"] = "executed"
	result["message"] = "Command execution not fully implemented for Iceberg"

	return json.Marshal(result)
}

// CreateDatabase creates a new namespace in an Iceberg catalog
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*IcebergClient)
	if !ok {
		return fmt.Errorf("invalid database connection type")
	}

	// In Iceberg, databases are called namespaces
	switch client.CatalogType {
	case "rest":
		return createNamespaceREST(client, databaseName, options)
	case "hive":
		return createNamespaceHive(client, databaseName, options)
	case "hadoop":
		return createNamespaceHadoop(client, databaseName, options)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// DropDatabase drops a namespace from an Iceberg catalog
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*IcebergClient)
	if !ok {
		return fmt.Errorf("invalid database connection type")
	}

	// In Iceberg, databases are called namespaces
	switch client.CatalogType {
	case "rest":
		return dropNamespaceREST(client, databaseName, options)
	case "hive":
		return dropNamespaceHive(client, databaseName, options)
	case "hadoop":
		return dropNamespaceHadoop(client, databaseName, options)
	default:
		return fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// createNamespaceREST creates a namespace using REST catalog
func createNamespaceREST(client *IcebergClient, namespace string, options map[string]interface{}) error {
	// Implementation would make REST API call to create namespace
	return fmt.Errorf("REST namespace creation not implemented")
}

// dropNamespaceREST drops a namespace using REST catalog
func dropNamespaceREST(client *IcebergClient, namespace string, options map[string]interface{}) error {
	// Implementation would make REST API call to drop namespace
	return fmt.Errorf("REST namespace deletion not implemented")
}

// createNamespaceHive creates a namespace using Hive catalog
func createNamespaceHive(client *IcebergClient, namespace string, options map[string]interface{}) error {
	// Implementation would use Hive metastore client to create namespace
	return fmt.Errorf("hive namespace creation not implemented")
}

// dropNamespaceHive drops a namespace using Hive catalog
func dropNamespaceHive(client *IcebergClient, namespace string, options map[string]interface{}) error {
	// Implementation would use Hive metastore client to drop namespace
	return fmt.Errorf("hive namespace deletion not implemented")
}

// createNamespaceHadoop creates a namespace using Hadoop catalog
func createNamespaceHadoop(client *IcebergClient, namespace string, options map[string]interface{}) error {
	// Implementation would create directory structure for namespace
	return fmt.Errorf("hadoop namespace creation not implemented")
}

// dropNamespaceHadoop drops a namespace using Hadoop catalog
func dropNamespaceHadoop(client *IcebergClient, namespace string, options map[string]interface{}) error {
	// Implementation would remove directory structure for namespace
	return fmt.Errorf("hadoop namespace deletion not implemented")
}

// ExecuteQuery executes a query on Iceberg tables and returns results as a slice of maps
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return nil, fmt.Errorf("invalid iceberg connection type")
	}

	// Parse query to extract table information
	// Expected format: {"namespace": "namespace", "table": "table_name", "query": "SELECT * FROM table"}
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse iceberg query: %w", err)
	}

	namespace, ok := queryReq["namespace"].(string)
	if !ok {
		return nil, fmt.Errorf("namespace is required in iceberg query")
	}

	tableName, ok := queryReq["table"].(string)
	if !ok {
		return nil, fmt.Errorf("table name is required in iceberg query")
	}

	sqlQuery, ok := queryReq["query"].(string)
	if !ok {
		return nil, fmt.Errorf("SQL query is required in iceberg query")
	}

	// For Iceberg, we would typically use a compute engine like Spark, Trino, or Flink
	// This is a simplified implementation that would need to be extended
	switch client.CatalogType {
	case "rest":
		return executeQueryREST(client, namespace, tableName, sqlQuery)
	case "hive":
		return executeQueryHive(client, namespace, tableName, sqlQuery)
	case "hadoop":
		return executeQueryHadoop(client, namespace, tableName, sqlQuery)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// ExecuteCountQuery executes a count query on Iceberg tables and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return 0, fmt.Errorf("invalid iceberg connection type")
	}

	// Parse query to extract table information
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse iceberg count query: %w", err)
	}

	namespace, ok := queryReq["namespace"].(string)
	if !ok {
		return 0, fmt.Errorf("namespace is required in iceberg count query")
	}

	tableName, ok := queryReq["table"].(string)
	if !ok {
		return 0, fmt.Errorf("table name is required in iceberg count query")
	}

	// Execute count query based on catalog type
	switch client.CatalogType {
	case "rest":
		return executeCountQueryREST(client, namespace, tableName)
	case "hive":
		return executeCountQueryHive(client, namespace, tableName)
	case "hadoop":
		return executeCountQueryHadoop(client, namespace, tableName)
	default:
		return 0, fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// StreamTableData streams data from an Iceberg table in batches for efficient data copying
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid iceberg connection type")
	}

	// Parse tableName to extract namespace and table
	// Expected format: "namespace.table"
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, false, "", fmt.Errorf("tableName must be in format 'namespace.table'")
	}

	namespace := parts[0]
	table := parts[1]

	// Stream data based on catalog type
	switch client.CatalogType {
	case "rest":
		return streamTableDataREST(client, namespace, table, batchSize, offset, columns)
	case "hive":
		return streamTableDataHive(client, namespace, table, batchSize, offset, columns)
	case "hadoop":
		return streamTableDataHadoop(client, namespace, table, batchSize, offset, columns)
	default:
		return nil, false, "", fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// GetTableRowCount returns the number of rows in an Iceberg table
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	client, ok := db.(*IcebergClient)
	if !ok {
		return 0, false, fmt.Errorf("invalid iceberg connection type")
	}

	// Parse tableName to extract namespace and table
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return 0, false, fmt.Errorf("tableName must be in format 'namespace.table'")
	}

	namespace := parts[0]
	table := parts[1]

	// Get row count based on catalog type
	switch client.CatalogType {
	case "rest":
		return getTableRowCountREST(client, namespace, table, whereClause)
	case "hive":
		return getTableRowCountHive(client, namespace, table, whereClause)
	case "hadoop":
		return getTableRowCountHadoop(client, namespace, table, whereClause)
	default:
		return 0, false, fmt.Errorf("unsupported catalog type: %s", client.CatalogType)
	}
}

// Helper functions for REST catalog operations
func executeQueryREST(client *IcebergClient, namespace, table, query string) ([]interface{}, error) {
	// Build REST API request for query execution
	queryURL := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s/scan", client.BaseURL, namespace, table)

	// Parse SQL query and convert to Iceberg scan parameters
	scanRequest := map[string]interface{}{
		"columns": []string{}, // All columns by default
		"filter":  nil,        // No filter by default
	}

	// In production, you would parse the SQL query and extract:
	// - SELECT columns
	// - WHERE conditions
	// - ORDER BY clauses
	// - LIMIT/OFFSET

	jsonData, err := json.Marshal(scanRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal scan request: %w", err)
	}

	req, err := http.NewRequest("POST", queryURL, strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, fmt.Errorf("failed to create REST request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if authToken, ok := client.Properties["auth.token"]; ok && authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	httpClient := &http.Client{Timeout: 300 * time.Second} // Longer timeout for data operations
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute REST query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("REST query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var scanResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&scanResult); err != nil {
		return nil, fmt.Errorf("failed to decode REST response: %w", err)
	}

	// Convert scan result to standard format
	var results []interface{}
	if data, ok := scanResult["data"].([]interface{}); ok {
		results = data
	}

	return results, nil
}

func executeCountQueryREST(client *IcebergClient, namespace, table string) (int64, error) {
	// Get table metadata for row count
	metadataURL := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", client.BaseURL, namespace, table)

	req, err := http.NewRequest("GET", metadataURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create REST metadata request: %w", err)
	}

	if authToken, ok := client.Properties["auth.token"]; ok && authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get table metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to get table metadata: status %d", resp.StatusCode)
	}

	var metadata map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return 0, fmt.Errorf("failed to decode metadata response: %w", err)
	}

	// Extract row count from table statistics
	if stats, ok := metadata["statistics"].(map[string]interface{}); ok {
		if rowCount, ok := stats["row_count"].(float64); ok {
			return int64(rowCount), nil
		}
	}

	return 0, fmt.Errorf("row count not available in table metadata")
}

func streamTableDataREST(client *IcebergClient, namespace, table string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	// Build scan request with pagination
	scanURL := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s/scan", client.BaseURL, namespace, table)

	scanRequest := map[string]interface{}{
		"limit":  batchSize,
		"offset": offset,
	}

	if len(columns) > 0 {
		scanRequest["columns"] = columns
	}

	jsonData, err := json.Marshal(scanRequest)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to marshal scan request: %w", err)
	}

	req, err := http.NewRequest("POST", scanURL, strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to create REST scan request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if authToken, ok := client.Properties["auth.token"]; ok && authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	httpClient := &http.Client{Timeout: 300 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute REST scan: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, "", fmt.Errorf("REST scan failed: %s", string(body))
	}

	var scanResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&scanResult); err != nil {
		return nil, false, "", fmt.Errorf("failed to decode scan response: %w", err)
	}

	// Extract results
	var results []map[string]interface{}
	if data, ok := scanResult["data"].([]interface{}); ok {
		for _, item := range data {
			if itemMap, ok := item.(map[string]interface{}); ok {
				results = append(results, itemMap)
			}
		}
	}

	rowCount := len(results)
	isComplete := rowCount < int(batchSize)

	// Use offset-based pagination for REST
	nextCursorValue := ""
	if !isComplete {
		nextCursorValue = fmt.Sprintf("%d", offset+int64(rowCount))
	}

	return results, isComplete, nextCursorValue, nil
}

func getTableRowCountREST(client *IcebergClient, namespace, table, whereClause string) (int64, bool, error) {
	count, err := executeCountQueryREST(client, namespace, table)
	if err != nil {
		return 0, false, err
	}

	// Iceberg table statistics are typically exact
	return count, false, nil
}

// Helper functions for Hive catalog operations
func executeQueryHive(client *IcebergClient, namespace, table, query string) ([]interface{}, error) {
	// In a real implementation, this would use Hive metastore and compute engine
	return []interface{}{
		map[string]interface{}{
			"message":   "Hive query execution not fully implemented",
			"namespace": namespace,
			"table":     table,
			"query":     query,
		},
	}, nil
}

func executeCountQueryHive(client *IcebergClient, namespace, table string) (int64, error) {
	// In a real implementation, this would use Hive metastore to get statistics
	return 0, fmt.Errorf("Hive count query not implemented")
}

func streamTableDataHive(client *IcebergClient, namespace, table string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	// In a real implementation, this would stream data via Hive/compute engine
	results := []map[string]interface{}{
		{
			"message":    "Hive streaming not fully implemented",
			"namespace":  namespace,
			"table":      table,
			"batch_size": batchSize,
			"offset":     offset,
		},
	}
	return results, true, "", nil
}

func getTableRowCountHive(client *IcebergClient, namespace, table, whereClause string) (int64, bool, error) {
	// In a real implementation, this would get table statistics from Hive metastore
	return 0, true, fmt.Errorf("Hive row count not implemented")
}

// Helper functions for Hadoop catalog operations
func executeQueryHadoop(client *IcebergClient, namespace, table, query string) ([]interface{}, error) {
	// In a real implementation, this would read Parquet/ORC files directly from Hadoop
	return []interface{}{
		map[string]interface{}{
			"message":   "Hadoop query execution not fully implemented",
			"namespace": namespace,
			"table":     table,
			"query":     query,
		},
	}, nil
}

func executeCountQueryHadoop(client *IcebergClient, namespace, table string) (int64, error) {
	// In a real implementation, this would read table metadata from Hadoop filesystem
	return 0, fmt.Errorf("Hadoop count query not implemented")
}

func streamTableDataHadoop(client *IcebergClient, namespace, table string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	// In a real implementation, this would read data files directly from Hadoop
	results := []map[string]interface{}{
		{
			"message":    "Hadoop streaming not fully implemented",
			"namespace":  namespace,
			"table":      table,
			"batch_size": batchSize,
			"offset":     offset,
		},
	}
	return results, true, "", nil
}

func getTableRowCountHadoop(client *IcebergClient, namespace, table, whereClause string) (int64, bool, error) {
	// In a real implementation, this would read table metadata from Hadoop filesystem
	return 0, true, fmt.Errorf("Hadoop row count not implemented")
}
