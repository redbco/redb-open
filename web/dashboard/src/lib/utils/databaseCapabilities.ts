// Database capabilities extracted from pkg/dbcapabilities
// This should match the supported database types from the backend

export interface DatabaseCapability {
  id: string;
  name: string;
  aliases: string[];
  defaultPort: number;
}

// Supported database types and their aliases
// Based on pkg/dbcapabilities/capabilities.go
export const SUPPORTED_DATABASES: DatabaseCapability[] = [
  // Relational SQL
  { id: 'postgres', name: 'PostgreSQL', aliases: ['postgresql', 'pgsql'], defaultPort: 5432 },
  { id: 'mysql', name: 'MySQL', aliases: ['aurora-mysql'], defaultPort: 3306 },
  { id: 'mariadb', name: 'MariaDB', aliases: [], defaultPort: 3306 },
  { id: 'mssql', name: 'Microsoft SQL Server', aliases: ['sqlserver', 'azure-sql'], defaultPort: 1433 },
  { id: 'oracle', name: 'Oracle Database', aliases: [], defaultPort: 1521 },
  { id: 'tidb', name: 'TiDB', aliases: ['pingcap-tidb'], defaultPort: 4000 },
  { id: 'clickhouse', name: 'ClickHouse', aliases: [], defaultPort: 8123 },
  { id: 'db2', name: 'IBM Db2', aliases: ['ibm-db2'], defaultPort: 50000 },
  { id: 'cockroach', name: 'CockroachDB', aliases: ['cockroachdb'], defaultPort: 26257 },
  { id: 'duckdb', name: 'DuckDB', aliases: [], defaultPort: 8080 },
  { id: 'hana', name: 'SAP HANA', aliases: ['sap-hana', 'hdb', 'saphana'], defaultPort: 30015 },
  
  // NoSQL / Other paradigms
  { id: 'cassandra', name: 'Apache Cassandra', aliases: [], defaultPort: 9042 },
  { id: 'dynamodb', name: 'Amazon DynamoDB', aliases: [], defaultPort: 443 },
  { id: 'mongodb', name: 'MongoDB', aliases: [], defaultPort: 27017 },
  { id: 'redis', name: 'Redis', aliases: [], defaultPort: 6379 },
  { id: 'neo4j', name: 'Neo4j', aliases: [], defaultPort: 7474 },
  { id: 'elasticsearch', name: 'Elasticsearch', aliases: [], defaultPort: 9200 },
  { id: 'opensearch', name: 'OpenSearch', aliases: ['aws-opensearch'], defaultPort: 9200 },
  { id: 'solr', name: 'Apache Solr', aliases: ['apache-solr'], defaultPort: 8983 },
  { id: 'cosmosdb', name: 'Azure Cosmos DB', aliases: [], defaultPort: 443 },
  
  // Analytics / Columnar / Cloud warehouses
  { id: 'snowflake', name: 'Snowflake', aliases: [], defaultPort: 443 },
  { id: 'iceberg', name: 'Apache Iceberg', aliases: ['apache-iceberg'], defaultPort: 8080 },
  
  // Vectors / AI
  { id: 'milvus', name: 'Milvus', aliases: [], defaultPort: 19530 },
  { id: 'weaviate', name: 'Weaviate', aliases: [], defaultPort: 8080 },
  { id: 'pinecone', name: 'Pinecone', aliases: [], defaultPort: 8080 },
  { id: 'chroma', name: 'Chroma', aliases: [], defaultPort: 8000 },
  { id: 'lancedb', name: 'LanceDB', aliases: [], defaultPort: 6666 },
  
  // Other
  { id: 'edgedb', name: 'EdgeDB', aliases: ['gel', 'geldata'], defaultPort: 5656 },
  
  // Object Storage
  { id: 's3', name: 'Amazon S3', aliases: ['aws-s3'], defaultPort: 443 },
  { id: 'gcs', name: 'Google Cloud Storage', aliases: ['google-cloud-storage'], defaultPort: 443 },
  { id: 'azure_blob', name: 'Azure Blob Storage', aliases: ['azure-blob', 'azureblob'], defaultPort: 443 },
  { id: 'minio', name: 'MinIO', aliases: [], defaultPort: 9000 },
  
  // Time Series
  { id: 'influxdb', name: 'InfluxDB', aliases: ['influx'], defaultPort: 8086 },
  { id: 'timescaledb', name: 'TimescaleDB', aliases: ['timescale'], defaultPort: 5432 },
  { id: 'prometheus', name: 'Prometheus', aliases: ['prom'], defaultPort: 9090 },
  { id: 'questdb', name: 'QuestDB', aliases: ['quest'], defaultPort: 8812 },
  { id: 'victoriametrics', name: 'VictoriaMetrics', aliases: ['vm', 'victoria'], defaultPort: 8428 },
  
  // Cloud Data Warehouses
  { id: 'bigquery', name: 'Google BigQuery', aliases: ['bq'], defaultPort: 443 },
  { id: 'redshift', name: 'Amazon Redshift', aliases: ['aws-redshift'], defaultPort: 5439 },
  { id: 'synapse', name: 'Azure Synapse Analytics', aliases: ['azure-synapse'], defaultPort: 1433 },
  
  // Analytics Platforms
  { id: 'databricks', name: 'Databricks', aliases: ['databricks-sql'], defaultPort: 443 },
  { id: 'druid', name: 'Apache Druid', aliases: ['druid'], defaultPort: 8888 },
  { id: 'apachepinot', name: 'Apache Pinot', aliases: ['pinot'], defaultPort: 8099 },
];

// Create a map for fast lookups
const databaseTypeMap = new Map<string, DatabaseCapability>();

// Initialize the map with all supported database types and their aliases
SUPPORTED_DATABASES.forEach(db => {
  // Add the main ID
  databaseTypeMap.set(db.id.toLowerCase(), db);
  
  // Add all aliases
  db.aliases.forEach(alias => {
    databaseTypeMap.set(alias.toLowerCase(), db);
  });
});

/**
 * Validates if a database type is supported
 * @param dbType - The database type to validate (e.g., 'postgres', 'postgresql', 'mysql')
 * @returns true if the database type is supported, false otherwise
 */
export function isSupportedDatabaseType(dbType: string): boolean {
  if (!dbType) return false;
  return databaseTypeMap.has(dbType.toLowerCase());
}

/**
 * Gets the canonical database capability for a given type or alias
 * @param dbType - The database type or alias
 * @returns The database capability or undefined if not found
 */
export function getDatabaseCapability(dbType: string): DatabaseCapability | undefined {
  if (!dbType) return undefined;
  return databaseTypeMap.get(dbType.toLowerCase());
}

/**
 * Extracts the database type from a connection string
 * @param connectionString - The connection string (e.g., 'postgresql://...')
 * @returns The database type (scheme) or null if invalid
 */
export function extractDatabaseTypeFromConnectionString(connectionString: string): string | null {
  if (!connectionString) return null;
  
  try {
    // Extract the scheme from the connection string
    const schemeMatch = connectionString.match(/^([a-zA-Z][a-zA-Z0-9+.-]*):\/\//);
    if (schemeMatch && schemeMatch[1]) {
      return schemeMatch[1].toLowerCase();
    }
  } catch (error) {
    // Invalid format
  }
  
  return null;
}

/**
 * Validates a connection string and returns validation result
 * @param connectionString - The connection string to validate
 * @returns Object with isValid flag and optional error message
 */
export function validateConnectionString(connectionString: string): { 
  isValid: boolean; 
  error?: string;
  databaseType?: string;
} {
  if (!connectionString || !connectionString.trim()) {
    return { isValid: false, error: 'Connection string is required' };
  }

  const dbType = extractDatabaseTypeFromConnectionString(connectionString);
  
  if (!dbType) {
    return { 
      isValid: false, 
      error: 'Invalid connection string format. Expected format: <database-type>://...' 
    };
  }

  if (!isSupportedDatabaseType(dbType)) {
    return { 
      isValid: false, 
      error: `Unsupported database type: "${dbType}". Please check the supported database types.`,
      databaseType: dbType
    };
  }

  return { isValid: true, databaseType: dbType };
}

/**
 * Gets a formatted list of all supported database types (for display)
 */
export function getSupportedDatabaseTypesList(): string[] {
  return SUPPORTED_DATABASES.map(db => db.name).sort();
}

