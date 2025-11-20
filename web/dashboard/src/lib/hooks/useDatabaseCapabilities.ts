'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api/endpoints';

export interface DatabaseCapabilities {
  name: string;
  id: string;
  hasSystemDatabase: boolean;
  systemDatabases?: string[];
  supportsCDC: boolean;
  cdcMechanisms?: string[];
  hasUniqueIdentifier: boolean;
  supportsClustering: boolean;
  clusteringMechanisms?: string[];
  supportedVendors?: string[];
  defaultPort: number;
  defaultSSLPort: number;
  connectionStringTemplate: string;
  paradigms: string[];
  aliases?: string[];
}

interface DatabaseCapabilitiesResponse {
  capabilities: DatabaseCapabilities;
}

export function useDatabaseCapabilities(databaseType: string | undefined) {
  const [capabilities, setCapabilities] = useState<DatabaseCapabilities | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!databaseType) {
      setCapabilities(null);
      return;
    }

    const fetchCapabilities = async () => {
      setIsLoading(true);
      setError(null);

      try {
        // Try to fetch from the backend capabilities endpoint
        // For now, we'll use a local mapping since the backend endpoint might not exist yet
        const caps = getDatabaseCapabilitiesLocal(databaseType);
        setCapabilities(caps);
      } catch (err) {
        console.error('Failed to fetch database capabilities:', err);
        setError(err instanceof Error ? err : new Error('Unknown error'));
        // Fallback to local mapping
        const caps = getDatabaseCapabilitiesLocal(databaseType);
        setCapabilities(caps);
      } finally {
        setIsLoading(false);
      }
    };

    fetchCapabilities();
  }, [databaseType]);

  return { capabilities, isLoading, error };
}

// Local mapping of database types to their capabilities
// This mirrors the pkg/dbcapabilities/capabilities.go structure
function getDatabaseCapabilitiesLocal(databaseType: string): DatabaseCapabilities {
  const capabilitiesMap: Record<string, DatabaseCapabilities> = {
    postgres: {
      name: 'PostgreSQL',
      id: 'postgres',
      hasSystemDatabase: true,
      systemDatabases: ['postgres'],
      supportsCDC: true,
      cdcMechanisms: ['logical_decoding', 'wal2json', 'pgoutput'],
      hasUniqueIdentifier: true,
      supportsClustering: false,
      supportedVendors: ['custom', 'aws-rds', 'aws-aurora', 'azure-database', 'gcp-cloudsql', 'supabase'],
      defaultPort: 5432,
      defaultSSLPort: 5432,
      connectionStringTemplate: 'postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}',
      paradigms: ['relational'],
      aliases: ['postgresql', 'pgsql'],
    },
    mysql: {
      name: 'MySQL',
      id: 'mysql',
      hasSystemDatabase: true,
      systemDatabases: ['mysql'],
      supportsCDC: true,
      cdcMechanisms: ['binlog'],
      hasUniqueIdentifier: true,
      supportsClustering: false,
      supportedVendors: ['custom', 'aws-rds', 'aws-aurora', 'azure-database', 'gcp-cloudsql'],
      defaultPort: 3306,
      defaultSSLPort: 3306,
      connectionStringTemplate: 'mysql://{username}:{password}@{host}:{port}/{database}?tls={tls}',
      paradigms: ['relational'],
      aliases: ['aurora-mysql'],
    },
    mongodb: {
      name: 'MongoDB',
      id: 'mongodb',
      hasSystemDatabase: true,
      systemDatabases: ['admin'],
      supportsCDC: true,
      cdcMechanisms: ['change_streams'],
      hasUniqueIdentifier: true,
      supportsClustering: true,
      clusteringMechanisms: ['active-active', 'active-passive'],
      supportedVendors: ['custom', 'mongodb-atlas', 'azure-cosmosdb-mongo'],
      defaultPort: 27017,
      defaultSSLPort: 27017,
      connectionStringTemplate: 'mongodb://{username}:{password}@{host}:{port}/{database}?ssl={ssl}',
      paradigms: ['document'],
    },
    redis: {
      name: 'Redis',
      id: 'redis',
      hasSystemDatabase: false,
      supportsCDC: false,
      hasUniqueIdentifier: false,
      supportsClustering: true,
      clusteringMechanisms: ['cluster', 'sentinel'],
      supportedVendors: ['custom', 'aws-elasticache', 'azure-cache', 'gcp-memorystore', 'redis-enterprise'],
      defaultPort: 6379,
      defaultSSLPort: 6380,
      connectionStringTemplate: 'redis://{username}:{password}@{host}:{port}/{database}',
      paradigms: ['keyvalue'],
    },
    neo4j: {
      name: 'Neo4j',
      id: 'neo4j',
      hasSystemDatabase: true,
      systemDatabases: ['system', 'neo4j'],
      supportsCDC: true,
      cdcMechanisms: ['change_data_capture'],
      hasUniqueIdentifier: true,
      supportsClustering: true,
      clusteringMechanisms: ['causal-cluster'],
      supportedVendors: ['custom', 'neo4j-aura', 'gcp-neo4j'],
      defaultPort: 7687,
      defaultSSLPort: 7687,
      connectionStringTemplate: 'bolt://{host}:{port}',
      paradigms: ['graph'],
    },
    elasticsearch: {
      name: 'Elasticsearch',
      id: 'elasticsearch',
      hasSystemDatabase: false,
      supportsCDC: false,
      hasUniqueIdentifier: true,
      supportsClustering: true,
      clusteringMechanisms: ['cluster'],
      supportedVendors: ['custom', 'elastic-cloud', 'aws-elasticsearch', 'azure-search'],
      defaultPort: 9200,
      defaultSSLPort: 9200,
      connectionStringTemplate: 'https://{host}:{port}',
      paradigms: ['searchindex'],
    },
    milvus: {
      name: 'Milvus',
      id: 'milvus',
      hasSystemDatabase: false,
      supportsCDC: false,
      hasUniqueIdentifier: true,
      supportsClustering: true,
      clusteringMechanisms: ['cluster'],
      supportedVendors: ['custom', 'zilliz-cloud'],
      defaultPort: 19530,
      defaultSSLPort: 19530,
      connectionStringTemplate: '{host}:{port}',
      paradigms: ['vector'],
    },
    influxdb: {
      name: 'InfluxDB',
      id: 'influxdb',
      hasSystemDatabase: false,
      supportsCDC: false,
      hasUniqueIdentifier: true,
      supportsClustering: false,
      supportedVendors: ['custom', 'influxdb-cloud'],
      defaultPort: 8086,
      defaultSSLPort: 8086,
      connectionStringTemplate: 'http://{host}:{port}',
      paradigms: ['timeseries'],
    },
    s3: {
      name: 'Amazon S3',
      id: 's3',
      hasSystemDatabase: false,
      supportsCDC: false,
      hasUniqueIdentifier: true,
      supportsClustering: false,
      supportedVendors: ['aws-s3'],
      defaultPort: 443,
      defaultSSLPort: 443,
      connectionStringTemplate: 's3://{bucket}/{key}',
      paradigms: ['objectstorage'],
    },
    dynamodb: {
      name: 'Amazon DynamoDB',
      id: 'dynamodb',
      hasSystemDatabase: false,
      supportsCDC: true,
      cdcMechanisms: ['streams'],
      hasUniqueIdentifier: true,
      supportsClustering: false,
      supportedVendors: ['aws-dynamodb'],
      defaultPort: 443,
      defaultSSLPort: 443,
      connectionStringTemplate: 'dynamodb://{username}:{password}@{host}?endpoint={endpoint}&table={table}',
      paradigms: ['keyvalue', 'widecolumn'],
    },
  };

  // Return the capabilities or a default for unknown types
  return capabilitiesMap[databaseType.toLowerCase()] || {
    name: databaseType,
    id: databaseType,
    hasSystemDatabase: false,
    supportsCDC: false,
    hasUniqueIdentifier: false,
    supportsClustering: false,
    defaultPort: 0,
    defaultSSLPort: 0,
    connectionStringTemplate: '',
    paradigms: ['relational'], // Default to relational
  };
}

