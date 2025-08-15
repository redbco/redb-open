package dbcapabilities

import "strings"

// DatabaseType is the canonical identifier for a database technology supported by reDB.
// Use these constants to look up capability information.
type DatabaseType string

const (
	// Relational SQL
	PostgreSQL  DatabaseType = "postgres"
	MySQL       DatabaseType = "mysql"
	MariaDB     DatabaseType = "mariadb"
	SQLServer   DatabaseType = "mssql"
	Oracle      DatabaseType = "oracle"
	TiDB        DatabaseType = "tidb"
	ClickHouse  DatabaseType = "clickhouse"
	DB2         DatabaseType = "db2"
	CockroachDB DatabaseType = "cockroach"
	DuckDB      DatabaseType = "duckdb"

	// NoSQL / Other paradigms
	Cassandra     DatabaseType = "cassandra"
	DynamoDB      DatabaseType = "dynamodb"
	MongoDB       DatabaseType = "mongodb"
	Redis         DatabaseType = "redis"
	Neo4j         DatabaseType = "neo4j"
	Elasticsearch DatabaseType = "elasticsearch"
	CosmosDB      DatabaseType = "cosmosdb"

	// Analytics / Columnar / Cloud warehouses
	Snowflake DatabaseType = "snowflake"

	// Vectors / AI
	Milvus   DatabaseType = "milvus"
	Weaviate DatabaseType = "weaviate"
	Pinecone DatabaseType = "pinecone"
	Chroma   DatabaseType = "chroma"
	LanceDB  DatabaseType = "lancedb"

	// Other
	EdgeDB DatabaseType = "edgedb"

	// Object Storage
	S3        DatabaseType = "s3"
	GCS       DatabaseType = "gcs"
	AzureBlob DatabaseType = "azure_blob"
	MinIO     DatabaseType = "minio"
)

// DataParadigm enumerates the primary data storage paradigms a database supports.
type DataParadigm string

const (
	ParadigmRelational  DataParadigm = "relational"    // Tables, schemas, SQL
	ParadigmDocument    DataParadigm = "document"      // Collections, documents
	ParadigmKeyValue    DataParadigm = "keyvalue"      // Key/Value
	ParadigmGraph       DataParadigm = "graph"         // Nodes/Edges
	ParadigmColumnar    DataParadigm = "columnar"      // Columnar analytics
	ParadigmWideColumn  DataParadigm = "widecolumn"    // Wide-column (e.g., Cassandra)
	ParadigmSearchIndex DataParadigm = "searchindex"   // Inverted indices (e.g., Elasticsearch)
	ParadigmVector      DataParadigm = "vector"        // Vector embeddings
	ParadigmTimeSeries  DataParadigm = "timeseries"    // Time-series specialized
	ParadigmObjectStore DataParadigm = "objectstorage" // Object/blob storage
)

// Capability describes what a database supports in a way that microservices can consume uniformly.
type Capability struct {
	// Human-friendly vendor or product name, e.g., "PostgreSQL".
	Name string `json:"name"`

	// Canonical ID used across the codebase (see DatabaseType constants), e.g., "postgres".
	ID DatabaseType `json:"id"`

	// Whether the database exposes a built-in/system database and its typical names.
	HasSystemDatabase bool     `json:"hasSystemDatabase"`
	SystemDatabases   []string `json:"systemDatabases,omitempty"`

	// Whether Change Data Capture (CDC) style replication is supported.
	SupportsCDC   bool     `json:"supportsCDC"`
	CDCMechanisms []string `json:"cdcMechanisms,omitempty"`

	// Whether the instance has a unique identifier
	HasUniqueIdentifier bool `json:"hasUniqueIdentifier"`

	// Clustering support
	SupportsClustering   bool     `json:"supportsClustering"`
	ClusteringMechanisms []string `json:"clusteringMechanisms,omitempty"`

	// List of hosting providers that support the database
	SupportedVendors []string `json:"supportedVendors,omitempty"`

	// Default ports
	DefaultPort    int `json:"defaultPort"`
	DefaultSSLPort int `json:"defaultSSLPort"`

	// Connection string template
	ConnectionStringTemplate string `json:"connectionStringTemplate"`

	// Primary data storage paradigms supported.
	Paradigms []DataParadigm `json:"paradigms"`

	// Common aliases (directory names, drivers, env labels) that map to this database.
	Aliases []string `json:"aliases,omitempty"`
}

// All is a registry of capabilities keyed by the canonical database ID.
var All = map[DatabaseType]Capability{
	PostgreSQL: {
		Name:                     "PostgreSQL",
		ID:                       PostgreSQL,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"postgres"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"logical_decoding", "wal2json", "pgoutput"},
		HasUniqueIdentifier:      true, // Unique ID: system_identifier from pg_control_system().
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom", "aws-rds", "aws-aurora", "azure-database", "gcp-cloudsql", "supabase", "heroku-postgres"},
		DefaultPort:              5432,
		DefaultSSLPort:           5432,
		ConnectionStringTemplate: "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}",
		Paradigms:                []DataParadigm{ParadigmRelational},
		Aliases:                  []string{"postgresql", "pgsql"},
	},
	MySQL: {
		Name:                     "MySQL",
		ID:                       MySQL,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"mysql"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"binlog"},
		HasUniqueIdentifier:      true, // Unique ID: @@server_uuid.
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom", "aws-rds", "aws-aurora", "azure-database", "gcp-cloudsql"},
		DefaultPort:              3306,
		DefaultSSLPort:           3306,
		ConnectionStringTemplate: "mysql://{username}:{password}@{host}:{port}/{database}?tls={tls}",
		Paradigms:                []DataParadigm{ParadigmRelational},
		Aliases:                  []string{"aurora-mysql"},
	},
	MariaDB: {
		Name:                     "MariaDB",
		ID:                       MariaDB,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"mysql"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"binlog"},
		HasUniqueIdentifier:      true, // Unique ID: @@server_uuid.
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom", "mariadb-corporation", "aws-rds", "azure-database"},
		DefaultPort:              3306,
		DefaultSSLPort:           3306,
		ConnectionStringTemplate: "mysql://{username}:{password}@{host}:{port}/{database}?tls={tls}",
		Paradigms:                []DataParadigm{ParadigmRelational},
	},
	SQLServer: {
		Name:                     "Microsoft SQL Server",
		ID:                       SQLServer,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"master"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"cdc", "change_tracking"},
		HasUniqueIdentifier:      true, // Unique ID: SERVERPROPERTY('ServerGuid').
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active", "active-passive"},
		SupportedVendors:         []string{"custom", "azure-sql", "aws-rds", "gcp-sqlserver"},
		DefaultPort:              1433,
		DefaultSSLPort:           1433,
		ConnectionStringTemplate: "sqlserver://{username}:{password}@{host}:{port}/{database}?encrypt={encrypt}&trustservercertificate={trustservercertificate}",
		Paradigms:                []DataParadigm{ParadigmRelational},
		Aliases:                  []string{"sqlserver", "mssql", "azure-sql"},
	},
	Oracle: {
		Name:                     "Oracle Database",
		ID:                       Oracle,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"CDB$ROOT"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"logminer", "goldengate"},
		HasUniqueIdentifier:      true, // Unique ID: DBID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active", "active-passive"},
		SupportedVendors:         []string{"custom", "oracle-cloud", "aws-rds", "azure-oracle"},
		DefaultPort:              1521,
		DefaultSSLPort:           1521,
		ConnectionStringTemplate: "oracle://{username}:{password}@{host}:{port}/{database}?server={server}&ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmRelational},
	},
	TiDB: {
		Name:                     "TiDB",
		ID:                       TiDB,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"INFORMATION_SCHEMA", "mysql"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"tidb-binlog", "changefeed"},
		HasUniqueIdentifier:      true, // Unique ID: CLUSTER_ID() function or pd/cluster_id from Placement Driver (PD) API.
		SupportsClustering:       true, // TiDB is natively distributed, supporting active-active SQL layer with TiKV storage nodes.
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "pingcap-cloud", "tidb-cloud"},
		DefaultPort:              4000,
		DefaultSSLPort:           4000,
		ConnectionStringTemplate: "tidb://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}&tidb_cluster_id={tidb_cluster_id}",
		Paradigms:                []DataParadigm{ParadigmRelational},
		Aliases:                  []string{"tidb", "pingcap-tidb"},
	},
	ClickHouse: {
		Name:                     "ClickHouse",
		ID:                       ClickHouse,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"system"},
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: server UUID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active", "active-passive"},
		SupportedVendors:         []string{"custom", "clickhouse-cloud", "altinity"},
		DefaultPort:              8123,
		DefaultSSLPort:           9000,
		ConnectionStringTemplate: "clickhouse://{username}:{password}@{host}:{port}/{database}?secure={secure}&compress={compress}",
		Paradigms:                []DataParadigm{ParadigmColumnar},
	},
	DB2: {
		Name:                     "IBM Db2",
		ID:                       DB2,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"SYSIBM"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"ibm-cdc"},
		HasUniqueIdentifier:      true, // Unique ID: DBID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active", "active-passive"},
		SupportedVendors:         []string{"custom", "ibm-cloud"},
		DefaultPort:              50000,
		DefaultSSLPort:           50000,
		ConnectionStringTemplate: "db2://{username}:{password}@{host}:{port}/{database}?security={security}",
		Paradigms:                []DataParadigm{ParadigmRelational},
		Aliases:                  []string{"ibm-db2"},
	},
	CockroachDB: {
		Name:                     "CockroachDB",
		ID:                       CockroachDB,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"system"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"changefeed"},
		HasUniqueIdentifier:      true, // Unique ID: cluster_id.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "cockroach-cloud"},
		DefaultPort:              26257,
		DefaultSSLPort:           26257,
		ConnectionStringTemplate: "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}&options={options}",
		Paradigms:                []DataParadigm{ParadigmRelational},
		Aliases:                  []string{"cockroachdb"},
	},
	Cassandra: {
		Name:                     "Apache Cassandra",
		ID:                       Cassandra,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"system"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"commitlog-cdc"},
		HasUniqueIdentifier:      true, // Unique ID: host_id.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "datastax-astara", "aws-keyspaces"},
		DefaultPort:              9042,
		DefaultSSLPort:           9042,
		ConnectionStringTemplate: "cassandra://{username}:{password}@{host}:{port}/{database}?consistency={consistency}&ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmWideColumn, ParadigmTimeSeries},
	},
	DynamoDB: {
		Name:                     "Amazon DynamoDB",
		ID:                       DynamoDB,
		HasSystemDatabase:        false,
		SupportsCDC:              true,
		CDCMechanisms:            []string{"streams"},
		HasUniqueIdentifier:      true, // Unique ID: Table ARN.
		SupportsClustering:       false,
		SupportedVendors:         []string{"aws-dynamodb"},
		DefaultPort:              443,
		DefaultSSLPort:           443,
		ConnectionStringTemplate: "dynamodb://{username}:{password}@{host}?endpoint={endpoint}&table={table}",
		Paradigms:                []DataParadigm{ParadigmKeyValue, ParadigmWideColumn},
	},
	MongoDB: {
		Name:                     "MongoDB",
		ID:                       MongoDB,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"admin"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"change_streams"},
		HasUniqueIdentifier:      true, // Unique ID: replica set name.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active", "active-passive"},
		SupportedVendors:         []string{"custom", "mongodb-atlas", "azure-cosmosdb-mongo"},
		DefaultPort:              27017,
		DefaultSSLPort:           27017,
		ConnectionStringTemplate: "mongodb://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmDocument},
	},
	Redis: {
		Name:                     "Redis",
		ID:                       Redis,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: node ID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active", "active-passive"},
		SupportedVendors:         []string{"custom", "aws-elasticache", "azure-redis", "gcp-memorystore"},
		DefaultPort:              6379,
		DefaultSSLPort:           6379,
		ConnectionStringTemplate: "redis://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmKeyValue, ParadigmTimeSeries},
	},
	Neo4j: {
		Name:                     "Neo4j",
		ID:                       Neo4j,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"system"},
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: DatabaseType.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "neo4j-aura"},
		DefaultPort:              7474,
		DefaultSSLPort:           7473,
		ConnectionStringTemplate: "neo4j://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmGraph},
	},
	Elasticsearch: {
		Name:                     "Elasticsearch",
		ID:                       Elasticsearch,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: cluster_uuid.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "elastic-cloud", "aws-opensearch", "azure-elasticsearch"},
		DefaultPort:              9200,
		DefaultSSLPort:           9200,
		ConnectionStringTemplate: "elasticsearch://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmSearchIndex},
	},
	CosmosDB: {
		Name:                     "Azure Cosmos DB",
		ID:                       CosmosDB,
		HasSystemDatabase:        false,
		SupportsCDC:              true,
		CDCMechanisms:            []string{"change_feed"},
		HasUniqueIdentifier:      true, // Unique ID: Account Resource ID.
		SupportsClustering:       false,
		SupportedVendors:         []string{"azure-cosmosdb"},
		DefaultPort:              443,
		DefaultSSLPort:           443,
		ConnectionStringTemplate: "cosmosdb://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmDocument, ParadigmKeyValue, ParadigmGraph},
	},
	Snowflake: {
		Name:                     "Snowflake",
		ID:                       Snowflake,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"SNOWFLAKE"},
		SupportsCDC:              true,
		CDCMechanisms:            []string{"streams"},
		HasUniqueIdentifier:      true, // Unique ID: ACCOUNT_ID.
		SupportsClustering:       false,
		SupportedVendors:         []string{"snowflake"},
		DefaultPort:              443,
		DefaultSSLPort:           443,
		ConnectionStringTemplate: "snowflake://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmColumnar},
	},
	Milvus: {
		Name:                     "Milvus",
		ID:                       Milvus,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: cluster ID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "zilliz-cloud"},
		DefaultPort:              19530,
		DefaultSSLPort:           19530,
		ConnectionStringTemplate: "milvus://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmVector},
	},
	Weaviate: {
		Name:                     "Weaviate",
		ID:                       Weaviate,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: cluster UUID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom", "weaviate-cloud"},
		DefaultPort:              8080,
		DefaultSSLPort:           8080,
		ConnectionStringTemplate: "weaviate://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmVector},
	},
	Pinecone: {
		Name:                     "Pinecone",
		ID:                       Pinecone,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: index/project ID.
		SupportsClustering:       false,
		SupportedVendors:         []string{"pinecone"},
		DefaultPort:              8080,
		DefaultSSLPort:           8080,
		ConnectionStringTemplate: "pinecone://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmVector},
	},
	Chroma: {
		Name:                     "Chroma",
		ID:                       Chroma,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      false,
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom"},
		DefaultPort:              8000,
		DefaultSSLPort:           8000,
		ConnectionStringTemplate: "chroma://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmVector},
	},
	LanceDB: {
		Name:                     "LanceDB",
		ID:                       LanceDB,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      false,
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom"},
		DefaultPort:              6666,
		DefaultSSLPort:           6666,
		ConnectionStringTemplate: "lancedb://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmVector},
	},
	DuckDB: {
		Name:                     "DuckDB",
		ID:                       DuckDB,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      false,
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom"},
		DefaultPort:              8080,
		DefaultSSLPort:           8080,
		ConnectionStringTemplate: "duckdb://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmRelational},
	},
	EdgeDB: {
		Name:                     "EdgeDB",
		ID:                       EdgeDB,
		HasSystemDatabase:        true,
		SystemDatabases:          []string{"edgedb"},
		SupportsCDC:              false,
		HasUniqueIdentifier:      false,
		SupportsClustering:       false,
		SupportedVendors:         []string{"custom", "edgedb-cloud"},
		DefaultPort:              5656,
		DefaultSSLPort:           5656,
		ConnectionStringTemplate: "edgedb://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmRelational, ParadigmGraph},
		Aliases:                  []string{"gel", "geldata"},
	},
	S3: {
		Name:                     "Amazon S3",
		ID:                       S3,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: bucket ARN.
		SupportsClustering:       false,
		SupportedVendors:         []string{"aws-s3"},
		DefaultPort:              443,
		DefaultSSLPort:           443,
		ConnectionStringTemplate: "s3://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmObjectStore},
		Aliases:                  []string{"aws-s3"},
	},
	GCS: {
		Name:                     "Google Cloud Storage",
		ID:                       GCS,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: bucket ID.
		SupportsClustering:       false,
		SupportedVendors:         []string{"gcp-storage"},
		DefaultPort:              443,
		DefaultSSLPort:           443,
		ConnectionStringTemplate: "gs://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmObjectStore},
		Aliases:                  []string{"google-cloud-storage"},
	},
	AzureBlob: {
		Name:                     "Azure Blob Storage",
		ID:                       AzureBlob,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: container resource ID.
		SupportsClustering:       false,
		SupportedVendors:         []string{"azure-blob"},
		DefaultPort:              443,
		DefaultSSLPort:           443,
		ConnectionStringTemplate: "az://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmObjectStore},
		Aliases:                  []string{"azure-blob", "azureblob"},
	},
	MinIO: {
		Name:                     "MinIO",
		ID:                       MinIO,
		HasSystemDatabase:        false,
		SupportsCDC:              false,
		HasUniqueIdentifier:      true, // Unique ID: deploymentID.
		SupportsClustering:       true,
		ClusteringMechanisms:     []string{"active-active"},
		SupportedVendors:         []string{"custom"},
		DefaultPort:              9000,
		DefaultSSLPort:           9000,
		ConnectionStringTemplate: "minio://{username}:{password}@{host}:{port}/{database}?ssl={ssl}",
		Paradigms:                []DataParadigm{ParadigmObjectStore},
	},
}

// nameToID is a normalized lookup index from any known name/alias to the canonical DatabaseType.
var nameToID map[string]DatabaseType

func init() {
	nameToID = make(map[string]DatabaseType, len(All)*2)
	for id, cap := range All {
		// Canonical ID
		nameToID[strings.ToLower(string(id))] = id
		// Also record vendor/product name
		if cap.Name != "" {
			nameToID[strings.ToLower(cap.Name)] = id
		}
		// Aliases
		for _, a := range cap.Aliases {
			if a == "" {
				continue
			}
			nameToID[strings.ToLower(a)] = id
		}
	}
}

// ParseID attempts to resolve an arbitrary database name (canonical id, alias, or product name)
// to a canonical DatabaseType. Returns false if unknown.
func ParseID(name string) (DatabaseType, bool) {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "" {
		return "", false
	}
	id, ok := nameToID[n]
	return id, ok
}

// GetByName returns the Capability by looking up using a free-form name (id or alias).
func GetByName(name string) (Capability, bool) {
	if id, ok := ParseID(name); ok {
		return Get(id)
	}
	return Capability{}, false
}

// MustGetByName returns the Capability by name or panics if unknown.
func MustGetByName(name string) Capability {
	cap, ok := GetByName(name)
	if !ok {
		panic("dbcapabilities: unknown database name: " + name)
	}
	return cap
}

// SupportsCDCString reports CDC support using a free-form name (id or alias).
func SupportsCDCString(name string) bool {
	if id, ok := ParseID(name); ok {
		return SupportsCDC(id)
	}
	return false
}

// HasSystemDBString reports system DB presence using a free-form name (id or alias).
func HasSystemDBString(name string) bool {
	if id, ok := ParseID(name); ok {
		return HasSystemDB(id)
	}
	return false
}

// SupportsParadigmString reports whether the database supports a given paradigm using a free-form name.
func SupportsParadigmString(name string, p DataParadigm) bool {
	if id, ok := ParseID(name); ok {
		return SupportsParadigm(id, p)
	}
	return false
}

// IDs returns the list of all known database IDs.
func IDs() []DatabaseType {
	out := make([]DatabaseType, 0, len(All))
	for id := range All {
		out = append(out, id)
	}
	return out
}

// Get returns capabilities for the given ID and a boolean indicating existence.
func Get(id DatabaseType) (Capability, bool) {
	c, ok := All[id]
	return c, ok
}

// MustGet returns capabilities for the given ID and panics if not found.
func MustGet(id DatabaseType) Capability {
	c, ok := Get(id)
	if !ok {
		panic("dbcapabilities: unknown database id: " + string(id))
	}
	return c
}

// SupportsParadigm reports whether the database supports a given data paradigm.
func SupportsParadigm(id DatabaseType, p DataParadigm) bool {
	c, ok := Get(id)
	if !ok {
		return false
	}
	for _, dp := range c.Paradigms {
		if dp == p {
			return true
		}
	}
	return false
}

// HasSystemDB is a convenience accessor for HasSystemDatabase.
func HasSystemDB(id DatabaseType) bool {
	c, ok := Get(id)
	return ok && c.HasSystemDatabase
}

// SupportsCDC reports whether CDC-style replication is supported.
func SupportsCDC(id DatabaseType) bool {
	c, ok := Get(id)
	return ok && c.SupportsCDC
}

// GetByConnectionType returns the Capability by looking up using a connection type string.
// This is useful for refactoring existing code that uses connection type strings.
func GetByConnectionType(connectionType string) (Capability, bool) {
	return GetByName(connectionType)
}

// MustGetByConnectionType returns the Capability by connection type or panics if unknown.
func MustGetByConnectionType(connectionType string) Capability {
	return MustGetByName(connectionType)
}

// IsValidConnectionType checks if a connection type string is valid.
func IsValidConnectionType(connectionType string) bool {
	_, ok := ParseID(connectionType)
	return ok
}
