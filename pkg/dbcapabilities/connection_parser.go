package dbcapabilities

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ConnectionDetails holds parsed connection information
type ConnectionDetails struct {
	DatabaseType   string            `json:"database_type"`
	DatabaseVendor string            `json:"database_vendor"`
	Host           string            `json:"host"`
	Port           int32             `json:"port"`
	Username       string            `json:"username"`
	Password       string            `json:"password"`
	DatabaseName   string            `json:"database_name"`
	SSL            bool              `json:"ssl"`
	SSLMode        string            `json:"ssl_mode"`
	Parameters     map[string]string `json:"parameters"`
	IsSystemDB     bool              `json:"is_system_db"`
	SystemDBName   string            `json:"system_db_name,omitempty"`
}

// ParseConnectionString parses a connection string and returns connection details
func ParseConnectionString(connectionString string) (*ConnectionDetails, error) {
	if connectionString == "" {
		return nil, fmt.Errorf("connection string cannot be empty")
	}

	// Parse the URL
	parsedURL, err := url.Parse(connectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string format: %v", err)
	}

	// Extract database type from scheme
	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme == "" {
		return nil, fmt.Errorf("connection string must include a scheme (e.g., postgresql://)")
	}

	// Map scheme to database type using existing ParseID function
	dbType, ok := ParseID(scheme)
	if !ok {
		return nil, fmt.Errorf("unsupported database type: %s", scheme)
	}

	// Get database capabilities
	capability, ok := Get(dbType)
	if !ok {
		return nil, fmt.Errorf("database capabilities not found for type: %s", string(dbType))
	}

	// Extract connection details
	details := &ConnectionDetails{
		DatabaseType:   string(dbType),
		DatabaseVendor: "custom", // Default vendor
		Parameters:     make(map[string]string),
	}

	// Extract host and port
	if parsedURL.Hostname() == "" {
		return nil, fmt.Errorf("host is required in connection string")
	}
	details.Host = parsedURL.Hostname()

	// Extract port or use default
	if parsedURL.Port() != "" {
		port, err := strconv.Atoi(parsedURL.Port())
		if err != nil {
			return nil, fmt.Errorf("invalid port number: %s", parsedURL.Port())
		}
		details.Port = int32(port)
	} else {
		details.Port = int32(capability.DefaultPort)
	}

	// Extract username and password
	if parsedURL.User != nil {
		details.Username = parsedURL.User.Username()
		if password, hasPassword := parsedURL.User.Password(); hasPassword {
			details.Password = password
		}
	}

	// Extract database name from path
	path := strings.Trim(parsedURL.Path, "/")
	if path != "" {
		details.DatabaseName = path
	}

	// Determine if this should use system database for instances
	if capability.HasSystemDatabase && len(capability.SystemDatabases) > 0 {
		systemDB := capability.SystemDatabases[0]

		// If no database specified or if the specified database is a system database
		if details.DatabaseName == "" || isSystemDatabase(details.DatabaseName, capability.SystemDatabases) {
			details.IsSystemDB = true
			details.SystemDBName = systemDB
			if details.DatabaseName == "" {
				details.DatabaseName = systemDB
			}
		}
	}

	// Parse query parameters
	queryParams := parsedURL.Query()
	for key, values := range queryParams {
		if len(values) > 0 {
			details.Parameters[key] = values[0]
		}
	}

	// Handle SSL configuration based on database type
	err = parseSSLConfiguration(details, capability, queryParams)
	if err != nil {
		return nil, fmt.Errorf("error parsing SSL configuration: %v", err)
	}

	// Validate required fields
	if details.Username == "" {
		return nil, fmt.Errorf("username is required in connection string")
	}

	return details, nil
}

// isSystemDatabase checks if the given database name is a system database
func isSystemDatabase(dbName string, systemDatabases []string) bool {
	for _, sysDB := range systemDatabases {
		if strings.EqualFold(dbName, sysDB) {
			return true
		}
	}
	return false
}

// parseSSLConfiguration handles SSL-related parameters based on database type
func parseSSLConfiguration(details *ConnectionDetails, capability Capability, queryParams url.Values) error {
	dbType := DatabaseType(details.DatabaseType)

	switch dbType {
	case PostgreSQL, CockroachDB:
		return parsePostgreSQLSSL(details, queryParams)
	case MySQL, MariaDB:
		return parseMySQLSSL(details, queryParams)
	case SQLServer:
		return parseSQLServerSSL(details, queryParams)
	case MongoDB:
		return parseMongoDBSSL(details, queryParams)
	case Redis:
		return parseRedisSSL(details, queryParams)
	case Snowflake:
		return parseSnowflakeSSL(details, queryParams)
	case ClickHouse:
		return parseClickHouseSSL(details, queryParams)
	case Neo4j:
		return parseNeo4jSSL(details, queryParams)
	case Elasticsearch:
		return parseElasticsearchSSL(details, queryParams)
	default:
		// Default SSL handling for other database types
		return parseDefaultSSL(details, queryParams)
	}
}

// parsePostgreSQLSSL handles PostgreSQL-specific SSL parameters
func parsePostgreSQLSSL(details *ConnectionDetails, queryParams url.Values) error {
	sslMode := queryParams.Get("sslmode")
	if sslMode == "" {
		sslMode = "prefer" // PostgreSQL default
	}

	details.SSLMode = sslMode
	details.SSL = sslMode != "disable"

	// Handle SSL certificate parameters
	if sslCert := queryParams.Get("sslcert"); sslCert != "" {
		details.Parameters["ssl_cert"] = sslCert
	}
	if sslKey := queryParams.Get("sslkey"); sslKey != "" {
		details.Parameters["ssl_key"] = sslKey
	}
	if sslRootCert := queryParams.Get("sslrootcert"); sslRootCert != "" {
		details.Parameters["ssl_root_cert"] = sslRootCert
	}

	return nil
}

// parseMySQLSSL handles MySQL/MariaDB-specific SSL parameters
func parseMySQLSSL(details *ConnectionDetails, queryParams url.Values) error {
	tls := queryParams.Get("tls")
	if tls == "" {
		tls = "false" // MySQL default
	}

	details.SSL = tls == "true" || tls == "skip-verify"
	if details.SSL {
		if tls == "skip-verify" {
			details.SSLMode = "prefer"
		} else {
			details.SSLMode = "require"
		}
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseSQLServerSSL handles SQL Server-specific SSL parameters
func parseSQLServerSSL(details *ConnectionDetails, queryParams url.Values) error {
	encrypt := queryParams.Get("encrypt")
	if encrypt == "" {
		encrypt = "false" // SQL Server default
	}

	details.SSL = encrypt == "true"
	if details.SSL {
		details.SSLMode = "require"
		if trustCert := queryParams.Get("trustservercertificate"); trustCert == "true" {
			details.SSLMode = "prefer"
		}
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseMongoDBSSL handles MongoDB-specific SSL parameters
func parseMongoDBSSL(details *ConnectionDetails, queryParams url.Values) error {
	tls := queryParams.Get("tls")
	ssl := queryParams.Get("ssl") // Legacy parameter

	if tls != "" {
		details.SSL = tls == "true"
	} else if ssl != "" {
		details.SSL = ssl == "true"
	} else {
		details.SSL = false
	}

	if details.SSL {
		details.SSLMode = "require"
		if queryParams.Get("tlsInsecure") == "true" {
			details.SSLMode = "prefer"
		}
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseRedisSSL handles Redis-specific SSL parameters
func parseRedisSSL(details *ConnectionDetails, queryParams url.Values) error {
	ssl := queryParams.Get("ssl")
	if ssl == "" {
		ssl = "false" // Redis default
	}

	details.SSL = ssl == "true"
	if details.SSL {
		details.SSLMode = "require"
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseSnowflakeSSL handles Snowflake-specific SSL parameters
func parseSnowflakeSSL(details *ConnectionDetails, queryParams url.Values) error {
	// Snowflake always uses SSL
	details.SSL = true
	details.SSLMode = "require"
	return nil
}

// parseClickHouseSSL handles ClickHouse-specific SSL parameters
func parseClickHouseSSL(details *ConnectionDetails, queryParams url.Values) error {
	secure := queryParams.Get("secure")
	if secure == "" {
		secure = "false" // ClickHouse default
	}

	details.SSL = secure == "true"
	if details.SSL {
		details.SSLMode = "require"
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseNeo4jSSL handles Neo4j-specific SSL parameters
func parseNeo4jSSL(details *ConnectionDetails, queryParams url.Values) error {
	ssl := queryParams.Get("ssl")
	if ssl == "" {
		ssl = "false" // Neo4j default
	}

	details.SSL = ssl == "true"
	if details.SSL {
		details.SSLMode = "require"
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseElasticsearchSSL handles Elasticsearch-specific SSL parameters
func parseElasticsearchSSL(details *ConnectionDetails, queryParams url.Values) error {
	ssl := queryParams.Get("ssl")
	if ssl == "" {
		ssl = "false" // Elasticsearch default
	}

	details.SSL = ssl == "true"
	if details.SSL {
		details.SSLMode = "require"
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// parseDefaultSSL handles SSL parameters for databases not specifically handled
func parseDefaultSSL(details *ConnectionDetails, queryParams url.Values) error {
	ssl := queryParams.Get("ssl")
	if ssl == "" {
		ssl = "false" // Default to no SSL
	}

	details.SSL = ssl == "true"
	if details.SSL {
		details.SSLMode = "require"
	} else {
		details.SSLMode = "disable"
	}

	return nil
}

// GetSystemDatabaseName returns the system database name for instance connections
func GetSystemDatabaseName(databaseType string) (string, error) {
	dbType, ok := ParseID(databaseType)
	if !ok {
		return "", fmt.Errorf("unsupported database type: %s", databaseType)
	}

	capability, ok := Get(dbType)
	if !ok {
		return "", fmt.Errorf("database capabilities not found for type: %s", databaseType)
	}

	if !capability.HasSystemDatabase || len(capability.SystemDatabases) == 0 {
		return "", fmt.Errorf("database type %s does not have a system database", databaseType)
	}

	return capability.SystemDatabases[0], nil
}

// ValidateConnectionString validates a connection string without fully parsing it
func ValidateConnectionString(connectionString string) error {
	_, err := ParseConnectionString(connectionString)
	return err
}
