package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to a Cassandra database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Create a cluster configuration
	cluster := gocql.NewCluster(config.Host)
	cluster.Port = config.Port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: config.Username,
		Password: decryptedPassword,
	}

	// Set keyspace if provided
	if config.DatabaseName != "" {
		cluster.Keyspace = config.DatabaseName
	}

	// Configure SSL if enabled
	if config.SSL {
		tlsConfig, err := createTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("error configuring TLS: %v", err)
		}
		cluster.SslOpts = tlsConfig
	}

	// Set reasonable defaults
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second

	// Create session
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error connecting to Cassandra: %v", err)
	}

	// Test the connection
	if err := session.Query("SELECT release_version FROM system.local").Scan(new(string)); err != nil {
		session.Close()
		return nil, fmt.Errorf("error testing Cassandra connection: %v", err)
	}

	return &common.DatabaseClient{
		DB:           session,
		DatabaseType: "cassandra",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Cassandra instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Create a cluster configuration
	cluster := gocql.NewCluster(config.Host)
	cluster.Port = config.Port
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: config.Username,
		Password: decryptedPassword,
	}

	// Set keyspace if provided
	if config.DatabaseName != "" {
		cluster.Keyspace = config.DatabaseName
	}

	// Configure SSL if enabled
	if config.SSL {
		tlsConfig, err := createInstanceTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("error configuring TLS: %v", err)
		}
		cluster.SslOpts = tlsConfig
	}

	// Set reasonable defaults
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second

	// Create session
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error connecting to Cassandra: %v", err)
	}

	// Test the connection
	if err := session.Query("SELECT release_version FROM system.local").Scan(new(string)); err != nil {
		session.Close()
		return nil, fmt.Errorf("error testing Cassandra connection: %v", err)
	}

	return &common.InstanceClient{
		DB:           session,
		InstanceType: "cassandra",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a Cassandra database
func DiscoverDetails(db interface{}) (*CassandraDetails, error) {
	session, ok := db.(*gocql.Session)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	var details CassandraDetails
	details.DatabaseType = "cassandra"

	// Get server version
	var version string
	if err := session.Query("SELECT release_version FROM system.local").Scan(&version); err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details.Version = version

	// Get cluster name
	var clusterName string
	if err := session.Query("SELECT cluster_name FROM system.local").Scan(&clusterName); err != nil {
		return nil, fmt.Errorf("error fetching cluster name: %v", err)
	}
	details.ClusterName = clusterName

	// Get datacenter
	var datacenter string
	if err := session.Query("SELECT data_center FROM system.local").Scan(&datacenter); err != nil {
		return nil, fmt.Errorf("error fetching datacenter: %v", err)
	}
	details.Datacenter = datacenter

	// Count keyspaces
	var keyspaceCount int
	iter := session.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()
	for iter.Scan(new(string)) {
		keyspaceCount++
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error counting keyspaces: %v", err)
	}
	details.Keyspaces = keyspaceCount

	// Generate a unique identifier based on cluster name and datacenter
	details.UniqueIdentifier = fmt.Sprintf("%s-%s", clusterName, datacenter)

	// Determine edition (Community or Enterprise)
	if strings.Contains(strings.ToLower(version), "dse") {
		details.DatabaseEdition = "enterprise"
	} else {
		details.DatabaseEdition = "community"
	}

	// Estimate database size (this is approximate as Cassandra doesn't provide direct size metrics)
	var databaseSize int64
	if GetKeyspace(session) != "" {
		iter := session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", GetKeyspace(session)).Iter()
		var tableName string
		for iter.Scan(&tableName) {
			var size int64
			// This is an approximation, as Cassandra doesn't provide exact table sizes easily
			if err := session.Query("SELECT sum(value) FROM system.size_estimates WHERE keyspace_name = ? AND table_name = ?",
				GetKeyspace(session), tableName).Scan(&size); err == nil {
				databaseSize += size
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("error estimating database size: %v", err)
		}
	}
	details.DatabaseSize = databaseSize

	return &details, nil
}

func createTLSConfig(config common.DatabaseConfig) (*gocql.SslOptions, error) {
	sslOpts := &gocql.SslOptions{
		EnableHostVerification: true,
	}

	if config.SSLCert != "" && config.SSLKey != "" {
		sslOpts.CertPath = config.SSLCert
		sslOpts.KeyPath = config.SSLKey
	}

	if config.SSLRootCert != "" {
		sslOpts.CaPath = config.SSLRootCert
	}

	if config.SSLRejectUnauthorized != nil {
		sslOpts.EnableHostVerification = *config.SSLRejectUnauthorized
	}

	return sslOpts, nil
}

func createInstanceTLSConfig(config common.InstanceConfig) (*gocql.SslOptions, error) {
	sslOpts := &gocql.SslOptions{
		EnableHostVerification: true,
	}

	if config.SSLCert != "" && config.SSLKey != "" {
		sslOpts.CertPath = config.SSLCert
		sslOpts.KeyPath = config.SSLKey
	}

	if config.SSLRootCert != "" {
		sslOpts.CaPath = config.SSLRootCert
	}

	if config.SSLRejectUnauthorized != nil {
		sslOpts.EnableHostVerification = *config.SSLRejectUnauthorized
	}

	return sslOpts, nil
}

// CollectDatabaseMetadata collects metadata from a Cassandra database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	session, ok := db.(*gocql.Session)
	if !ok {
		return nil, fmt.Errorf("invalid cassandra connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	if err := session.Query("SELECT release_version FROM system.local").Scan(&version); err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get keyspace information
	keyspace := GetKeyspace(session)
	if keyspace == "" {
		return metadata, nil
	}

	metadata["keyspace"] = keyspace

	// Count tables in keyspace
	var tablesCount int
	iter := session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace).Iter()
	for iter.Scan(new(string)) {
		tablesCount++
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	// Estimate keyspace size
	var sizeBytes int64
	iter = session.Query("SELECT sum(value) FROM system.size_estimates WHERE keyspace_name = ?", keyspace).Iter()
	for iter.Scan(&sizeBytes) {
		// Just need one row
	}
	if err := iter.Close(); err != nil {
		// Size estimates might not be available, so don't fail if this query doesn't work
		sizeBytes = 0
	}
	metadata["size_bytes"] = sizeBytes

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Cassandra instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	session, ok := db.(*gocql.Session)
	if !ok {
		return nil, fmt.Errorf("invalid cassandra connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	if err := session.Query("SELECT release_version FROM system.local").Scan(&version); err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get cluster name
	var clusterName string
	if err := session.Query("SELECT cluster_name FROM system.local").Scan(&clusterName); err != nil {
		return nil, fmt.Errorf("failed to get cluster name: %w", err)
	}
	metadata["cluster_name"] = clusterName

	// Get datacenter
	var datacenter string
	if err := session.Query("SELECT data_center FROM system.local").Scan(&datacenter); err != nil {
		return nil, fmt.Errorf("failed to get datacenter: %w", err)
	}
	metadata["datacenter"] = datacenter

	// Get uptime
	var uptimeInSeconds int64
	if err := session.Query("SELECT uptime_in_seconds FROM system.local").Scan(&uptimeInSeconds); err != nil {
		// If uptime is not available, don't fail
		uptimeInSeconds = 0
	}
	metadata["uptime_seconds"] = uptimeInSeconds

	// Count keyspaces
	var keyspaceCount int
	iter := session.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()
	for iter.Scan(new(string)) {
		keyspaceCount++
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to count keyspaces: %w", err)
	}
	metadata["total_keyspaces"] = keyspaceCount

	// Get node count
	var nodeCount int
	iter = session.Query("SELECT peer FROM system.peers").Iter()
	for iter.Scan(new(string)) {
		nodeCount++
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to count nodes: %w", err)
	}
	// Add 1 for the local node
	nodeCount++
	metadata["node_count"] = nodeCount

	// Get partitioner
	var partitioner string
	if err := session.Query("SELECT partitioner FROM system.local").Scan(&partitioner); err != nil {
		return nil, fmt.Errorf("failed to get partitioner: %w", err)
	}
	metadata["partitioner"] = partitioner

	// Set default values for connections if not available
	if _, ok := metadata["total_connections"]; !ok {
		metadata["total_connections"] = 0 // Default placeholder
	}

	if _, ok := metadata["max_connections"]; !ok {
		metadata["max_connections"] = 100 // Default placeholder
	}

	return metadata, nil
}

// GetKeyspace returns the current keyspace from the session
func GetKeyspace(session *gocql.Session) string {
	// Since session.Keyspace() doesn't exist, we need to extract it from the session configuration
	// Try to get it from a query
	var keyspace string
	iter := session.Query("SELECT keyspace_name FROM system_schema.keyspaces LIMIT 1").Iter()
	iter.Scan(&keyspace)
	iter.Close()

	// If we couldn't get it from a query, try to extract it from the session's configuration
	if keyspace == "" {
		// We can try to access it through reflection or use the config that was passed in
		// For now, we'll return an empty string and handle this case in the calling code
	}

	return keyspace
}

// ExecuteCommand executes a command on a Cassandra database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	session, ok := db.(*gocql.Session)
	if !ok {
		return nil, fmt.Errorf("invalid cassandra connection type")
	}

	// Execute the command
	iter := session.Query(command).Iter()

	// Get column information
	columnNames := iter.Columns()
	if columnNames == nil {
		columnNames = []gocql.ColumnInfo{}
	}

	// Extract column names
	colNames := make([]string, len(columnNames))
	for i, col := range columnNames {
		colNames[i] = col.Name
	}

	// Collect results in a structured format
	var results []map[string]interface{}

	// Prepare values slice for scanning
	values := make([]interface{}, len(columnNames))
	valuePtrs := make([]interface{}, len(columnNames))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Scan all rows
	for iter.Scan(valuePtrs...) {
		row := make(map[string]interface{})
		for i, colName := range colNames {
			row[colName] = values[i]
		}
		results = append(results, row)
	}

	// Check for iteration errors
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error during iteration: %w", err)
	}

	// Structure the response for gRPC
	response := map[string]interface{}{
		"columns": colNames,
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

// CreateDatabase creates a new Cassandra keyspace with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	session, ok := db.(*gocql.Session)
	if !ok {
		return fmt.Errorf("invalid cassandra connection type")
	}

	// Build the CREATE KEYSPACE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE KEYSPACE %s", databaseName))

	// Parse and apply options
	if len(options) > 0 {
		commandBuilder.WriteString(" WITH")

		var optionParts []string

		// Replication strategy
		if replication, ok := options["replication"].(map[string]interface{}); ok {
			replicationStr := "{"
			var replicationParts []string

			if class, exists := replication["class"].(string); exists {
				replicationParts = append(replicationParts, fmt.Sprintf("'class': '%s'", class))
			}

			if factor, exists := replication["replication_factor"]; exists {
				if factorInt, validInt := factor.(int); validInt {
					replicationParts = append(replicationParts, fmt.Sprintf("'replication_factor': %d", factorInt))
				}
			}

			// Handle datacenter-specific replication for NetworkTopologyStrategy
			for key, value := range replication {
				if key != "class" && key != "replication_factor" {
					if valueInt, validInt := value.(int); validInt {
						replicationParts = append(replicationParts, fmt.Sprintf("'%s': %d", key, valueInt))
					}
				}
			}

			replicationStr += strings.Join(replicationParts, ", ") + "}"
			optionParts = append(optionParts, fmt.Sprintf(" REPLICATION = %s", replicationStr))
		} else {
			// Default replication strategy
			optionParts = append(optionParts, " REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
		}

		// Durable writes
		if durableWrites, ok := options["durable_writes"].(bool); ok {
			optionParts = append(optionParts, fmt.Sprintf(" AND DURABLE_WRITES = %t", durableWrites))
		}

		commandBuilder.WriteString(strings.Join(optionParts, ""))
	} else {
		// Default replication strategy if no options provided
		commandBuilder.WriteString(" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	}

	// Create the keyspace
	if err := session.Query(commandBuilder.String()).Exec(); err != nil {
		return fmt.Errorf("failed to create keyspace: %w", err)
	}

	return nil
}

// DropDatabase drops a Cassandra keyspace with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	session, ok := db.(*gocql.Session)
	if !ok {
		return fmt.Errorf("invalid cassandra connection type")
	}

	// Build the DROP KEYSPACE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP KEYSPACE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// Note: Cassandra doesn't support CASCADE/RESTRICT for keyspaces like PostgreSQL does for databases
	// All tables and data in the keyspace are automatically dropped

	// Drop the keyspace
	if err := session.Query(commandBuilder.String()).Exec(); err != nil {
		return fmt.Errorf("failed to drop keyspace: %w", err)
	}

	return nil
}
