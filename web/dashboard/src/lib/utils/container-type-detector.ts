import { ContainerType } from '@/lib/api/types';

// Map database paradigms to container types
const PARADIGM_TO_CONTAINER_TYPE: Record<string, ContainerType> = {
  relational: 'tabular-record-set',
  document: 'document',
  keyvalue: 'keyvalue-item',
  graph: 'graph-node', // Default to nodes for graph databases
  columnar: 'tabular-record-set', // Columnar stores are still tabular
  widecolumn: 'keyvalue-item', // Wide-column stores are like key-value
  searchindex: 'search-document',
  vector: 'vector',
  timeseries: 'timeseries-point',
  objectstorage: 'blob-object',
};

// Map database types directly to container types (overrides paradigm mapping if needed)
const DATABASE_TYPE_TO_CONTAINER_TYPE: Record<string, ContainerType> = {
  postgres: 'tabular-record-set',
  postgresql: 'tabular-record-set',
  mysql: 'tabular-record-set',
  mariadb: 'tabular-record-set',
  mssql: 'tabular-record-set',
  oracle: 'tabular-record-set',
  mongodb: 'document',
  redis: 'keyvalue-item',
  dynamodb: 'keyvalue-item',
  cassandra: 'keyvalue-item',
  neo4j: 'graph-node',
  elasticsearch: 'search-document',
  opensearch: 'search-document',
  solr: 'search-document',
  milvus: 'vector',
  weaviate: 'vector',
  pinecone: 'vector',
  chroma: 'vector',
  lancedb: 'vector',
  influxdb: 'timeseries-point',
  timescaledb: 'timeseries-point',
  prometheus: 'timeseries-point',
  questdb: 'timeseries-point',
  s3: 'blob-object',
  gcs: 'blob-object',
  azure_blob: 'blob-object',
  minio: 'blob-object',
};

/**
 * Detects the appropriate container type based on database type and paradigm
 */
export function detectContainerType(
  databaseType: string | undefined,
  paradigm?: string
): ContainerType {
  if (!databaseType) {
    return 'tabular-record-set'; // Default
  }

  const normalizedType = databaseType.toLowerCase().replace(/[_-]/g, '');

  // First try direct database type mapping
  const directMapping = DATABASE_TYPE_TO_CONTAINER_TYPE[normalizedType];
  if (directMapping) {
    return directMapping;
  }

  // Then try paradigm mapping
  if (paradigm) {
    const normalizedParadigm = paradigm.toLowerCase();
    const paradigmMapping = PARADIGM_TO_CONTAINER_TYPE[normalizedParadigm];
    if (paradigmMapping) {
      return paradigmMapping;
    }
  }

  // Default to tabular-record-set for unknown types
  return 'tabular-record-set';
}

/**
 * Gets all supported container types for a given database paradigm
 */
export function getSupportedContainerTypes(paradigm: string): ContainerType[] {
  const normalizedParadigm = paradigm.toLowerCase();

  const supportedTypesMap: Record<string, ContainerType[]> = {
    relational: ['tabular-record-set'],
    document: ['document'],
    keyvalue: ['keyvalue-item'],
    graph: ['graph-node', 'graph-relationship'],
    columnar: ['tabular-record-set'],
    widecolumn: ['keyvalue-item'],
    searchindex: ['search-document'],
    vector: ['vector'],
    timeseries: ['timeseries-point'],
    objectstorage: ['blob-object'],
  };

  return supportedTypesMap[normalizedParadigm] || ['tabular-record-set'];
}

/**
 * Checks if a source container type can be mapped to a target container type
 */
export function areContainerTypesCompatible(
  sourceType: ContainerType,
  targetType: ContainerType
): { compatible: boolean; warning?: string } {
  // Same types are always compatible
  if (sourceType === targetType) {
    return { compatible: true };
  }

  // Define compatible mappings with optional warnings
  const compatibilityMatrix: Record<
    ContainerType,
    Record<ContainerType, { compatible: boolean; warning?: string }>
  > = {
    'tabular-record-set': {
      'tabular-record-set': { compatible: true },
      document: { compatible: true, warning: 'Converting tabular data to document format' },
      'keyvalue-item': {
        compatible: true,
        warning: 'Tabular rows will be flattened to key-value pairs',
      },
      'graph-node': {
        compatible: true,
        warning: 'Each row will become a graph node',
      },
      'graph-relationship': {
        compatible: true,
        warning: 'Rows must contain relationship data (source, target)',
      },
      'search-document': { compatible: true, warning: 'Rows will be indexed for search' },
      vector: { compatible: true, warning: 'Requires vector embedding column' },
      'timeseries-point': {
        compatible: true,
        warning: 'Requires timestamp column',
      },
      'blob-object': { compatible: true, warning: 'Will serialize rows as objects' },
    },
    document: {
      'tabular-record-set': {
        compatible: true,
        warning: 'Document fields will be flattened to columns',
      },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: true, warning: 'Document will become a graph node' },
      'graph-relationship': {
        compatible: true,
        warning: 'Document must contain relationship data',
      },
      'search-document': { compatible: true },
      vector: { compatible: true, warning: 'Requires embedding field' },
      'timeseries-point': { compatible: true, warning: 'Requires timestamp field' },
      'blob-object': { compatible: true, warning: 'Will serialize document as object' },
    },
    'keyvalue-item': {
      'tabular-record-set': {
        compatible: true,
        warning: 'Key-value pairs will become table rows',
      },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: false },
      'graph-relationship': { compatible: false },
      'search-document': { compatible: true },
      vector: { compatible: false },
      'timeseries-point': { compatible: false },
      'blob-object': { compatible: true },
    },
    'graph-node': {
      'tabular-record-set': {
        compatible: true,
        warning: 'Node properties will become table columns',
      },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: true },
      'graph-relationship': { compatible: false },
      'search-document': { compatible: true },
      vector: { compatible: true, warning: 'Requires embedding property' },
      'timeseries-point': { compatible: false },
      'blob-object': { compatible: true },
    },
    'graph-relationship': {
      'tabular-record-set': {
        compatible: true,
        warning: 'Relationships will become table rows',
      },
      document: { compatible: true },
      'keyvalue-item': { compatible: false },
      'graph-node': { compatible: false },
      'graph-relationship': { compatible: true },
      'search-document': { compatible: true },
      vector: { compatible: false },
      'timeseries-point': { compatible: false },
      'blob-object': { compatible: true },
    },
    'search-document': {
      'tabular-record-set': {
        compatible: true,
        warning: 'Search documents will be de-normalized to rows',
      },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: true },
      'graph-relationship': { compatible: false },
      'search-document': { compatible: true },
      vector: { compatible: true, warning: 'Requires embedding field' },
      'timeseries-point': { compatible: false },
      'blob-object': { compatible: true },
    },
    vector: {
      'tabular-record-set': { compatible: true },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: true },
      'graph-relationship': { compatible: false },
      'search-document': { compatible: true },
      vector: { compatible: true },
      'timeseries-point': { compatible: false },
      'blob-object': { compatible: true },
    },
    'timeseries-point': {
      'tabular-record-set': { compatible: true },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: false },
      'graph-relationship': { compatible: false },
      'search-document': { compatible: true },
      vector: { compatible: false },
      'timeseries-point': { compatible: true },
      'blob-object': { compatible: true },
    },
    'blob-object': {
      'tabular-record-set': {
        compatible: true,
        warning: 'Objects will be deserialized to rows',
      },
      document: { compatible: true },
      'keyvalue-item': { compatible: true },
      'graph-node': { compatible: true },
      'graph-relationship': { compatible: false },
      'search-document': { compatible: true },
      vector: { compatible: false },
      'timeseries-point': { compatible: false },
      'blob-object': { compatible: true },
    },
  };

  const result = compatibilityMatrix[sourceType]?.[targetType];
  return result || { compatible: false };
}

/**
 * Gets a human-readable name for a container type
 */
export function getContainerTypeName(type: ContainerType): string {
  const names: Record<ContainerType, string> = {
    'tabular-record-set': 'Tabular Record Set',
    document: 'Document',
    'keyvalue-item': 'Key-Value Item',
    'graph-node': 'Graph Node',
    'graph-relationship': 'Graph Relationship',
    'search-document': 'Search Document',
    vector: 'Vector',
    'timeseries-point': 'Time-Series Point',
    'blob-object': 'Blob/Object',
  };

  return names[type] || type;
}

/**
 * Gets a human-readable description for a container type
 */
export function getContainerTypeDescription(type: ContainerType): string {
  const descriptions: Record<ContainerType, string> = {
    'tabular-record-set':
      'Structured data organized in rows and columns (SQL tables, CSV files)',
    document: 'Semi-structured documents with flexible schemas (JSON, BSON)',
    'keyvalue-item': 'Simple key-value pairs with optional metadata',
    'graph-node': 'Nodes in a graph database with properties and labels',
    'graph-relationship':
      'Edges connecting nodes in a graph database with properties',
    'search-document': 'Indexed documents optimized for full-text search',
    vector: 'High-dimensional vector embeddings for similarity search',
    'timeseries-point': 'Time-stamped data points for temporal analysis',
    'blob-object': 'Binary large objects or unstructured file data',
  };

  return descriptions[type] || type;
}

