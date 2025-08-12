package dbcapabilities

import "strings"

// DatabaseID is the canonical identifier for a database technology supported by reDB.
// Use these constants to look up capability information.
type DatabaseID string

const (
	// Relational SQL
	PostgreSQL  DatabaseID = "postgres"
	MySQL       DatabaseID = "mysql"
	MariaDB     DatabaseID = "mariadb"
	SQLServer   DatabaseID = "mssql"
	Oracle      DatabaseID = "oracle"
	ClickHouse  DatabaseID = "clickhouse"
	DB2         DatabaseID = "db2"
	CockroachDB DatabaseID = "cockroach"
	DuckDB      DatabaseID = "duckdb"

	// NoSQL / Other paradigms
	Cassandra     DatabaseID = "cassandra"
	DynamoDB      DatabaseID = "dynamodb"
	MongoDB       DatabaseID = "mongodb"
	Redis         DatabaseID = "redis"
	Neo4j         DatabaseID = "neo4j"
	Elasticsearch DatabaseID = "elasticsearch"
	CosmosDB      DatabaseID = "cosmosdb"

	// Analytics / Columnar / Cloud warehouses
	Snowflake DatabaseID = "snowflake"

	// Vectors / AI
	Milvus   DatabaseID = "milvus"
	Weaviate DatabaseID = "weaviate"
	Pinecone DatabaseID = "pinecone"
	Chroma   DatabaseID = "chroma"
	LanceDB  DatabaseID = "lancedb"

	// Other
	EdgeDB DatabaseID = "edgedb"

	// Object Storage
	S3        DatabaseID = "s3"
	GCS       DatabaseID = "gcs"
	AzureBlob DatabaseID = "azure_blob"
	MinIO     DatabaseID = "minio"
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

	// Canonical ID used across the codebase (see DatabaseID constants), e.g., "postgres".
	ID DatabaseID `json:"id"`

	// Whether the database exposes a built-in/system database and its typical names.
	HasSystemDatabase bool     `json:"hasSystemDatabase"`
	SystemDatabases   []string `json:"systemDatabases,omitempty"`

	// Whether Change Data Capture (CDC) style replication is supported.
	SupportsCDC   bool     `json:"supportsCDC"`
	CDCMechanisms []string `json:"cdcMechanisms,omitempty"`

	// Primary data storage paradigms supported.
	Paradigms []DataParadigm `json:"paradigms"`

	// Common aliases (directory names, drivers, env labels) that map to this database.
	Aliases []string `json:"aliases,omitempty"`
}

// All is a registry of capabilities keyed by the canonical database ID.
var All = map[DatabaseID]Capability{
	PostgreSQL: {
		Name:              "PostgreSQL",
		ID:                PostgreSQL,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"postgres"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"logical_decoding", "wal2json", "pgoutput"},
		Paradigms:         []DataParadigm{ParadigmRelational},
		Aliases:           []string{"postgresql", "pgsql"},
	},
	MySQL: {
		Name:              "MySQL",
		ID:                MySQL,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"mysql"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"binlog"},
		Paradigms:         []DataParadigm{ParadigmRelational},
		Aliases:           []string{"aurora-mysql"},
	},
	MariaDB: {
		Name:              "MariaDB",
		ID:                MariaDB,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"mysql"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"binlog"},
		Paradigms:         []DataParadigm{ParadigmRelational},
	},
	SQLServer: {
		Name:              "Microsoft SQL Server",
		ID:                SQLServer,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"master"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"cdc", "change_tracking"},
		Paradigms:         []DataParadigm{ParadigmRelational},
		Aliases:           []string{"sqlserver", "mssql", "azure-sql"},
	},
	Oracle: {
		Name:              "Oracle Database",
		ID:                Oracle,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"CDB$ROOT"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"logminer", "goldengate"},
		Paradigms:         []DataParadigm{ParadigmRelational},
	},
	ClickHouse: {
		Name:              "ClickHouse",
		ID:                ClickHouse,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"system"},
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmColumnar},
	},
	DB2: {
		Name:              "IBM Db2",
		ID:                DB2,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"SYSIBM"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"ibm-cdc"},
		Paradigms:         []DataParadigm{ParadigmRelational},
		Aliases:           []string{"ibm-db2"},
	},
	CockroachDB: {
		Name:              "CockroachDB",
		ID:                CockroachDB,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"system"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"changefeed"},
		Paradigms:         []DataParadigm{ParadigmRelational},
		Aliases:           []string{"cockroachdb"},
	},
	Cassandra: {
		Name:              "Apache Cassandra",
		ID:                Cassandra,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"system"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"commitlog-cdc"},
		Paradigms:         []DataParadigm{ParadigmWideColumn, ParadigmTimeSeries},
	},
	DynamoDB: {
		Name:              "Amazon DynamoDB",
		ID:                DynamoDB,
		HasSystemDatabase: false,
		SupportsCDC:       true,
		CDCMechanisms:     []string{"streams"},
		Paradigms:         []DataParadigm{ParadigmKeyValue, ParadigmWideColumn},
	},
	MongoDB: {
		Name:              "MongoDB",
		ID:                MongoDB,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"admin"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"change_streams"},
		Paradigms:         []DataParadigm{ParadigmDocument},
	},
	Redis: {
		Name:              "Redis",
		ID:                Redis,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmKeyValue, ParadigmTimeSeries},
	},
	Neo4j: {
		Name:              "Neo4j",
		ID:                Neo4j,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"system"},
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmGraph},
	},
	Elasticsearch: {
		Name:              "Elasticsearch",
		ID:                Elasticsearch,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmSearchIndex},
	},
	CosmosDB: {
		Name:              "Azure Cosmos DB",
		ID:                CosmosDB,
		HasSystemDatabase: false,
		SupportsCDC:       true,
		CDCMechanisms:     []string{"change_feed"},
		Paradigms:         []DataParadigm{ParadigmDocument, ParadigmKeyValue, ParadigmGraph},
	},
	Snowflake: {
		Name:              "Snowflake",
		ID:                Snowflake,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"SNOWFLAKE"},
		SupportsCDC:       true,
		CDCMechanisms:     []string{"streams"},
		Paradigms:         []DataParadigm{ParadigmColumnar},
	},
	Milvus: {
		Name:              "Milvus",
		ID:                Milvus,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmVector},
	},
	Weaviate: {
		Name:              "Weaviate",
		ID:                Weaviate,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmVector},
	},
	Pinecone: {
		Name:              "Pinecone",
		ID:                Pinecone,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmVector},
	},
	Chroma: {
		Name:              "Chroma",
		ID:                Chroma,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmVector},
	},
	LanceDB: {
		Name:              "LanceDB",
		ID:                LanceDB,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmVector},
	},
	DuckDB: {
		Name:              "DuckDB",
		ID:                DuckDB,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmRelational},
	},
	EdgeDB: {
		Name:              "EdgeDB",
		ID:                EdgeDB,
		HasSystemDatabase: true,
		SystemDatabases:   []string{"edgedb"},
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmRelational, ParadigmGraph},
		Aliases:           []string{"gel", "geldata"},
	},
	S3: {
		Name:              "Amazon S3",
		ID:                S3,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmObjectStore},
		Aliases:           []string{"aws-s3"},
	},
	GCS: {
		Name:              "Google Cloud Storage",
		ID:                GCS,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmObjectStore},
		Aliases:           []string{"google-cloud-storage"},
	},
	AzureBlob: {
		Name:              "Azure Blob Storage",
		ID:                AzureBlob,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmObjectStore},
		Aliases:           []string{"azure-blob", "azureblob"},
	},
	MinIO: {
		Name:              "MinIO",
		ID:                MinIO,
		HasSystemDatabase: false,
		SupportsCDC:       false,
		Paradigms:         []DataParadigm{ParadigmObjectStore},
	},
}

// nameToID is a normalized lookup index from any known name/alias to the canonical DatabaseID.
var nameToID map[string]DatabaseID

func init() {
	nameToID = make(map[string]DatabaseID, len(All)*2)
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
// to a canonical DatabaseID. Returns false if unknown.
func ParseID(name string) (DatabaseID, bool) {
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
func IDs() []DatabaseID {
	out := make([]DatabaseID, 0, len(All))
	for id := range All {
		out = append(out, id)
	}
	return out
}

// Get returns capabilities for the given ID and a boolean indicating existence.
func Get(id DatabaseID) (Capability, bool) {
	c, ok := All[id]
	return c, ok
}

// MustGet returns capabilities for the given ID and panics if not found.
func MustGet(id DatabaseID) Capability {
	c, ok := Get(id)
	if !ok {
		panic("dbcapabilities: unknown database id: " + string(id))
	}
	return c
}

// SupportsParadigm reports whether the database supports a given data paradigm.
func SupportsParadigm(id DatabaseID, p DataParadigm) bool {
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
func HasSystemDB(id DatabaseID) bool {
	c, ok := Get(id)
	return ok && c.HasSystemDatabase
}

// SupportsCDC reports whether CDC-style replication is supported.
func SupportsCDC(id DatabaseID) bool {
	c, ok := Get(id)
	return ok && c.SupportsCDC
}
