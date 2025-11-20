import { ResourceType, ContainerType, EndpointType } from '../api/types';
import { areContainerTypesCompatible } from './container-type-detector';

/**
 * Valid mapping type pairs for container types
 * Container types can map to other container types with some restrictions
 */
const CONTAINER_VALID_TARGETS: Record<ContainerType, ResourceType[]> = {
  'tabular-record-set': [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'graph-node',
    'graph-relationship',
    'search-document',
    'vector',
    'timeseries-point',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  document: [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'graph-node',
    'graph-relationship',
    'search-document',
    'vector',
    'timeseries-point',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  'keyvalue-item': [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'search-document',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  'graph-node': [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'graph-node',
    'search-document',
    'vector',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  'graph-relationship': [
    'tabular-record-set',
    'document',
    'graph-relationship',
    'search-document',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  'search-document': [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'graph-node',
    'search-document',
    'vector',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  vector: [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'graph-node',
    'search-document',
    'vector',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  'timeseries-point': [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'search-document',
    'timeseries-point',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
  'blob-object': [
    'tabular-record-set',
    'document',
    'keyvalue-item',
    'graph-node',
    'search-document',
    'blob-object',
    'mcp-resource',
    'mcp-tool',
    'webhook',
    'stream',
  ],
};

/**
 * Valid mapping type pairs for endpoint types
 * Streams and webhooks can be sources, MCP resources/tools can only be targets
 */
const ENDPOINT_VALID_SOURCES: Record<EndpointType, ResourceType[]> = {
  stream: ['tabular-record-set', 'document', 'keyvalue-item', 'search-document', 'stream'],
  webhook: ['tabular-record-set', 'document', 'keyvalue-item', 'search-document', 'stream'],
  'mcp-resource': [], // MCP resources cannot be sources
  'mcp-tool': [], // MCP tools cannot be sources
};

/**
 * Legacy table type support - maps to tabular-record-set
 */
const LEGACY_TABLE_MAPPINGS: ResourceType[] = [
  'tabular-record-set',
  'document',
  'keyvalue-item',
  'graph-node',
  'graph-relationship',
  'search-document',
  'vector',
  'timeseries-point',
  'blob-object',
  'mcp-resource',
  'mcp-tool',
  'webhook',
  'stream',
  'table', // Can map to itself for backward compatibility
];

/**
 * Check if a source-target mapping pair is valid
 */
export function validateMappingPair(
  sourceType: ResourceType,
  targetType: ResourceType
): boolean {
  // Database to database is always valid
  if (sourceType === 'database' && targetType === 'database') {
    return true;
  }

  // Database to anything else is not valid (use database mapping wizard)
  if (sourceType === 'database' || targetType === 'database') {
    return false;
  }

  // MCP resources and tools can only be targets
  if (sourceType === 'mcp-resource' || sourceType === 'mcp-tool') {
    return false;
  }

  // Legacy table type support
  if (sourceType === 'table') {
    return LEGACY_TABLE_MAPPINGS.includes(targetType);
  }

  if (targetType === 'table') {
    return LEGACY_TABLE_MAPPINGS.includes(sourceType);
  }

  // Check if source is a container type
  if (sourceType in CONTAINER_VALID_TARGETS) {
    return CONTAINER_VALID_TARGETS[sourceType as ContainerType].includes(targetType);
  }

  // Check if source is an endpoint type (stream/webhook)
  if (sourceType in ENDPOINT_VALID_SOURCES) {
    return ENDPOINT_VALID_SOURCES[sourceType as EndpointType].includes(targetType);
  }

  return false;
}

/**
 * Get list of valid target types for a given source type
 */
export function getValidTargetTypes(sourceType: ResourceType): ResourceType[] {
  // Database to database only
  if (sourceType === 'database') {
    return ['database'];
  }

  // MCP resources and tools cannot be sources
  if (sourceType === 'mcp-resource' || sourceType === 'mcp-tool') {
    return [];
  }

  // Legacy table type support
  if (sourceType === 'table') {
    return LEGACY_TABLE_MAPPINGS;
  }

  // Container types
  if (sourceType in CONTAINER_VALID_TARGETS) {
    return CONTAINER_VALID_TARGETS[sourceType as ContainerType];
  }

  // Endpoint types
  if (sourceType in ENDPOINT_VALID_SOURCES) {
    return ENDPOINT_VALID_SOURCES[sourceType as EndpointType];
  }

  return [];
}

/**
 * Determine if auto-mapping makes sense for a source-target pair
 * Auto-mapping is primarily useful for similar container types
 */
export function requiresAutoMapping(
  sourceType: ResourceType,
  targetType: ResourceType
): boolean {
  // Auto-mapping makes sense when both sides have similar structures
  if (sourceType === 'database' && targetType === 'database') {
    return true;
  }

  // Same container types
  if (sourceType === targetType) {
    return true;
  }

  // Tabular to tabular-like types
  if (
    sourceType === 'tabular-record-set' &&
    (targetType === 'document' || targetType === 'search-document')
  ) {
    return true;
  }

  if (
    targetType === 'tabular-record-set' &&
    (sourceType === 'document' || sourceType === 'search-document')
  ) {
    return true;
  }

  // Legacy table support
  if (sourceType === 'table' && targetType === 'table') {
    return true;
  }

  return false;
}

/**
 * Generate a helpful description for a mapping type
 */
export function getMappingDescription(
  sourceType: ResourceType,
  targetType: ResourceType
): string {
  // Database to database
  if (sourceType === 'database' && targetType === 'database') {
    return 'Map all containers from source database to target database with automatic matching';
  }

  // Container to container mappings
  const containerDescriptions: Record<string, string> = {
    // Tabular
    'tabular-record-set-tabular-record-set':
      'Map source table columns to target table columns with optional transformations',
    'tabular-record-set-document':
      'Convert relational rows to document format',
    'tabular-record-set-graph-node':
      'Each row becomes a graph node with properties',
    'tabular-record-set-search-document':
      'Index table rows for full-text search',
    'tabular-record-set-stream':
      'Publish CDC events from source table to stream',

    // Document
    'document-tabular-record-set':
      'Flatten document fields to table columns',
    'document-document':
      'Map document fields with optional transformations',
    'document-graph-node':
      'Convert documents to graph nodes',
    'document-stream':
      'Publish document changes to stream',

    // Stream
    'stream-tabular-record-set':
      'Map incoming stream events to table inserts/updates',
    'stream-document':
      'Map stream events to document updates',
    'stream-stream':
      'Route and transform events between streams',

    // Webhook
    'webhook-tabular-record-set':
      'Map incoming webhook events to table inserts/updates',
    'webhook-document':
      'Map webhook payloads to document updates',
    'webhook-stream':
      'Forward webhook events to stream',

    // MCP targets
    'tabular-record-set-mcp-resource':
      'Expose table data as MCP resource endpoint',
    'document-mcp-resource':
      'Expose documents as MCP resource endpoint',
    'tabular-record-set-mcp-tool':
      'Expose table operations as MCP tool functions',
  };

  const key = `${sourceType}-${targetType}`;
  return containerDescriptions[key] || `Map data from ${sourceType} to ${targetType}`;
}

/**
 * Get user-friendly name for resource type
 */
export function getResourceTypeName(type: ResourceType): string {
  const names: Record<ResourceType, string> = {
    database: 'Database',
    table: 'Table', // Legacy
    'tabular-record-set': 'Tabular Record Set',
    document: 'Document',
    'keyvalue-item': 'Key-Value Item',
    'graph-node': 'Graph Node',
    'graph-relationship': 'Graph Relationship',
    'search-document': 'Search Document',
    vector: 'Vector',
    'timeseries-point': 'Time-Series Point',
    'blob-object': 'Blob/Object',
    'mcp-resource': 'MCP Resource',
    'mcp-tool': 'MCP Tool',
    webhook: 'Webhook',
    stream: 'Stream',
  };

  return names[type] || type;
}

/**
 * Check if a resource type supports schemas
 */
export function supportsSchema(type: ResourceType): boolean {
  return [
    'database',
    'table',
    'tabular-record-set',
    'document',
    'graph-node',
    'graph-relationship',
  ].includes(type);
}

/**
 * Get icon name for resource type (for lucide-react icons)
 */
export function getResourceTypeIcon(type: ResourceType): string {
  const icons: Record<ResourceType, string> = {
    database: 'Database',
    table: 'Table',
    'tabular-record-set': 'Table',
    document: 'FileJson',
    'keyvalue-item': 'Key',
    'graph-node': 'Network',
    'graph-relationship': 'GitBranch',
    'search-document': 'Search',
    vector: 'Layers',
    'timeseries-point': 'Clock',
    'blob-object': 'Archive',
    'mcp-resource': 'Box',
    'mcp-tool': 'Wrench',
    webhook: 'Webhook',
    stream: 'Activity',
  };

  return icons[type] || 'HelpCircle';
}
