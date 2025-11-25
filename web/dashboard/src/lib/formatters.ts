
export const formatVendor = (vendor: string | undefined): string => {
    if (!vendor) return 'Unknown Vendor';

    const map: Record<string, string> = {
        'custom': 'Self-hosted',
        'aws-rds': 'AWS RDS',
        'aws-aurora': 'AWS Aurora',
        'gcp-cloudsql': 'GCP Cloud SQL',
        'azure-database': 'Azure Database',
        'supabase': 'Supabase',
        'mariadb-corporation': 'MariaDB Corporation',
        'oracle-cloud': 'Oracle Cloud',
        'cockroach-cloud': 'CockroachDB Cloud',
        'elastic-cloud': 'Elastic Cloud',
        'mongodb-atlas': 'MongoDB Atlas',
        'neo4j-aura': 'Neo4j Aura',
        'tidb-cloud': 'TiDB Cloud',
        'clickhouse-cloud': 'ClickHouse Cloud',
        'influxdata-cloud': 'InfluxData Cloud',
        'timescale-cloud': 'Timescale Cloud',
        'questdb-cloud': 'QuestDB Cloud',
        'victoriametrics-cloud': 'VictoriaMetrics Cloud',
        'zilliz-cloud': 'Zilliz Cloud',
        'weaviate-cloud': 'Weaviate Cloud',
        'edgedb-cloud': 'EdgeDB Cloud',
        'azure-cosmosdb': 'Azure Cosmos DB',
        'aws-dynamodb': 'AWS DynamoDB',
        'aws-elasticache': 'AWS ElastiCache',
        'aws-keyspaces': 'AWS Keyspaces',
        'aws-opensearch': 'AWS OpenSearch',
        'aws-redshift': 'AWS Redshift',
        'aws-s3': 'AWS S3',
        'gcp-bigquery': 'GCP BigQuery',
        'gcp-memorystore': 'GCP Memorystore',
        'gcp-storage': 'GCP Storage',
        'azure-blob': 'Azure Blob Storage',
        'azure-sql': 'Azure SQL',
        'azure-synapse': 'Azure Synapse',
        'ibm-cloud': 'IBM Cloud',
        'sap-cloud': 'SAP Cloud',
        'snowflake': 'Snowflake',
        'datastax-astara': 'DataStax Astra',
        'azure-cosmosdb-mongo': 'Azure Cosmos DB for MongoDB',
        'azure-redis': 'Azure Cache for Redis',
        'azure-elasticsearch': 'Azure Elasticsearch',
        'azure-oracle': 'Azure for Oracle',
        'pingcap-cloud': 'PingCAP Cloud',
        'altinity': 'Altinity',
        'heroku-postgres': 'Heroku Postgres',
    };

    return map[vendor.toLowerCase()] || vendor.split('-').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
};

export const formatDatabaseType = (type: string | undefined): string => {
    if (!type) return 'Unknown Type';

    const map: Record<string, string> = {
        'postgres': 'PostgreSQL',
        'postgresql': 'PostgreSQL',
        'mysql': 'MySQL',
        'mariadb': 'MariaDB',
        'mssql': 'SQL Server',
        'sqlserver': 'SQL Server',
        'oracle': 'Oracle',
        'tidb': 'TiDB',
        'clickhouse': 'ClickHouse',
        'db2': 'DB2',
        'cockroach': 'CockroachDB',
        'cockroachdb': 'CockroachDB',
        'cassandra': 'Cassandra',
        'dynamodb': 'DynamoDB',
        'mongodb': 'MongoDB',
        'redis': 'Redis',
        'neo4j': 'Neo4j',
        'elasticsearch': 'Elasticsearch',
        'opensearch': 'OpenSearch',
        'solr': 'Solr',
        'cosmosdb': 'Cosmos DB',
        'snowflake': 'Snowflake',
        'iceberg': 'Iceberg',
        'milvus': 'Milvus',
        'weaviate': 'Weaviate',
        'pinecone': 'Pinecone',
        'chroma': 'Chroma',
        'lancedb': 'LanceDB',
        'duckdb': 'DuckDB',
        'hana': 'HANA',
        'edgedb': 'EdgeDB',
        's3': 'S3',
        'gcs': 'GCS',
        'azure_blob': 'Azure Blob',
        'minio': 'MinIO',
        'influxdb': 'InfluxDB',
        'timescaledb': 'TimescaleDB',
        'prometheus': 'Prometheus',
        'questdb': 'QuestDB',
        'victoriametrics': 'VictoriaMetrics',
        'bigquery': 'BigQuery',
        'redshift': 'Redshift',
        'synapse': 'Synapse',
        'databricks': 'Databricks',
        'druid': 'Druid',
        'apachepinot': 'Apache Pinot',
    };

    return map[type.toLowerCase()] || type.split('-').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
};

export const formatStreamPlatform = (platform: string | undefined): string => {
    if (!platform) return 'Unknown Platform';

    const map: Record<string, string> = {
        'kafka': 'Apache Kafka',
        'kinesis': 'AWS Kinesis',
        'pubsub': 'GCP Pub/Sub',
        'rabbitmq': 'RabbitMQ',
        'pulsar': 'Apache Pulsar',
        'redpanda': 'Redpanda',
        'confluent': 'Confluent Cloud',
        'azure-event-hubs': 'Azure Event Hubs',
        'mqtt': 'MQTT',
        'mqtt_server': 'MQTT Server',
        'nats': 'NATS',
        'sqs': 'AWS SQS',
        'sns': 'AWS SNS',
        'activemq': 'ActiveMQ',
    };

    return map[platform.toLowerCase()] || platform.split('-').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
};

export const formatContainerCategory = (category: string | undefined): string => {
    if (!category) return 'General';

    const map: Record<string, string> = {
        'entity_storage': 'Entity Storage',
        'relationship_storage': 'Relationship Storage',
        'schema_flexible': 'Schema Flexible',
        'time_series': 'Time Series',
        'full_text_search': 'Full Text Search',
        'vector_similarity': 'Vector Similarity',
        'metadata_system': 'Metadata System',
    };

    return map[category.toLowerCase()] || category.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
};

export const formatCommitDate = (dateString: string | undefined): string => {
    if (!dateString) return 'N/A';
    try {
        return new Date(dateString).toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
        });
    } catch {
        return dateString;
    }
};
