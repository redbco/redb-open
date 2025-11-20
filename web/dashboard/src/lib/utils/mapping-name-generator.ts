import { ResourceSelection, ResourceType, ContainerType } from '@/lib/api/types';
import { getContainerTypeName } from './container-type-detector';

/**
 * Generates an auto-generated mapping name based on source and target resources
 * This follows the same logic as the CLI implementation
 */
export function generateMappingName(
  source: ResourceSelection,
  target: ResourceSelection
): string {
  const sourceDB = source.databaseName || source.resourceName;
  const sourceContainer = source.tableName || source.containerName || '';
  const targetDB = target.databaseName || target.resourceName;
  const targetContainer = target.tableName || target.containerName || '';

  // Database to Database
  if (source.type === 'database' && target.type === 'database') {
    return `${sourceDB}_to_${targetDB}`;
  }

  // Container to Container (including legacy table type)
  const isSourceContainer =
    source.type === 'table' ||
    source.containerType ||
    [
      'tabular-record-set',
      'document',
      'keyvalue-item',
      'graph-node',
      'graph-relationship',
      'search-document',
      'vector',
      'timeseries-point',
      'blob-object',
    ].includes(source.type as any);

  const isTargetContainer =
    target.type === 'table' ||
    target.containerType ||
    [
      'tabular-record-set',
      'document',
      'keyvalue-item',
      'graph-node',
      'graph-relationship',
      'search-document',
      'vector',
      'timeseries-point',
      'blob-object',
    ].includes(target.type as any);

  if (isSourceContainer && isTargetContainer) {
    return `${sourceDB}_${sourceContainer}_to_${targetDB}_${targetContainer}`;
  }

  // Container to MCP Resource
  if (isSourceContainer && target.type === 'mcp-resource') {
    return `${sourceDB}_${sourceContainer}_to_mcp_${target.resourceName}`;
  }

  // Container to MCP Tool
  if (isSourceContainer && target.type === 'mcp-tool') {
    return `${sourceDB}_${sourceContainer}_to_mcp_${target.resourceName}`;
  }

  // Container to Webhook
  if (isSourceContainer && target.type === 'webhook') {
    return `${sourceDB}_${sourceContainer}_to_webhook_${target.resourceName}`;
  }

  // Container to Stream
  if (isSourceContainer && target.type === 'stream') {
    const targetName = (target as any).topicName || target.resourceName;
    return `${sourceDB}_${sourceContainer}_to_stream_${targetName}`;
  }

  // Webhook to Container
  if (source.type === 'webhook' && isTargetContainer) {
    return `webhook_${source.resourceName}_to_${targetDB}_${targetContainer}`;
  }

  // Stream to Container
  if (source.type === 'stream' && isTargetContainer) {
    const sourceName = (source as any).topicName || source.resourceName;
    return `stream_${sourceName}_to_${targetDB}_${targetContainer}`;
  }

  // Stream to Stream
  if (source.type === 'stream' && target.type === 'stream') {
    const sourceName = (source as any).topicName || source.resourceName;
    const targetName = (target as any).topicName || target.resourceName;
    return `stream_${sourceName}_to_stream_${targetName}`;
  }

  // Generic fallback (shouldn't normally reach here)
  return `${source.resourceName}_to_${target.resourceName}`;
}

/**
 * Generates an auto-generated mapping description based on source and target resources
 * This follows the same logic as the CLI implementation
 */
export function generateMappingDescription(
  source: ResourceSelection,
  target: ResourceSelection
): string {
  const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';

  const sourceDB = source.databaseName || source.resourceName;
  const sourceContainer = source.tableName || source.containerName || '';
  const targetDB = target.databaseName || target.resourceName;
  const targetContainer = target.tableName || target.containerName || '';

  // Get friendly type names
  const sourceTypeName = source.containerType
    ? getContainerTypeName(source.containerType)
    : getResourceTypeDisplayName(source.type);
  const targetTypeName = target.containerType
    ? getContainerTypeName(target.containerType)
    : getResourceTypeDisplayName(target.type);

  // Database to Database
  if (source.type === 'database' && target.type === 'database') {
    return `Auto-generated database mapping from '${sourceDB}' to '${targetDB}' created on ${timestamp}`;
  }

  // Container to Container
  const isSourceContainer =
    source.type === 'table' ||
    source.containerType ||
    [
      'tabular-record-set',
      'document',
      'keyvalue-item',
      'graph-node',
      'graph-relationship',
      'search-document',
      'vector',
      'timeseries-point',
      'blob-object',
    ].includes(source.type as any);

  const isTargetContainer =
    target.type === 'table' ||
    target.containerType ||
    [
      'tabular-record-set',
      'document',
      'keyvalue-item',
      'graph-node',
      'graph-relationship',
      'search-document',
      'vector',
      'timeseries-point',
      'blob-object',
    ].includes(target.type as any);

  if (isSourceContainer && isTargetContainer) {
    return `Auto-generated ${sourceTypeName} to ${targetTypeName} mapping from '${sourceDB}.${sourceContainer}' to '${targetDB}.${targetContainer}' created on ${timestamp}`;
  }

  // Container to MCP Resource
  if (isSourceContainer && target.type === 'mcp-resource') {
    return `Auto-generated MCP resource mapping from ${sourceTypeName} '${sourceDB}.${sourceContainer}' to MCP resource '${target.resourceName}' created on ${timestamp}`;
  }

  // Container to MCP Tool
  if (isSourceContainer && target.type === 'mcp-tool') {
    return `Auto-generated MCP tool mapping from ${sourceTypeName} '${sourceDB}.${sourceContainer}' to MCP tool '${target.resourceName}' created on ${timestamp}`;
  }

  // Container to Webhook
  if (isSourceContainer && target.type === 'webhook') {
    return `Auto-generated webhook mapping from ${sourceTypeName} '${sourceDB}.${sourceContainer}' to webhook '${target.resourceName}' for CDC events created on ${timestamp}`;
  }

  // Container to Stream
  if (isSourceContainer && target.type === 'stream') {
    const targetName = (target as any).topicName || target.resourceName;
    return `Auto-generated stream mapping from ${sourceTypeName} '${sourceDB}.${sourceContainer}' to stream '${targetName}' for CDC events created on ${timestamp}`;
  }

  // Webhook to Container
  if (source.type === 'webhook' && isTargetContainer) {
    return `Auto-generated webhook-to-${targetTypeName} mapping from webhook '${source.resourceName}' to '${targetDB}.${targetContainer}' created on ${timestamp}`;
  }

  // Stream to Container
  if (source.type === 'stream' && isTargetContainer) {
    const sourceName = (source as any).topicName || source.resourceName;
    return `Auto-generated stream-to-${targetTypeName} mapping from stream '${sourceName}' to '${targetDB}.${targetContainer}' created on ${timestamp}`;
  }

  // Stream to Stream
  if (source.type === 'stream' && target.type === 'stream') {
    const sourceName = (source as any).topicName || source.resourceName;
    const targetName = (target as any).topicName || target.resourceName;
    return `Auto-generated stream-to-stream mapping from stream '${sourceName}' to stream '${targetName}' created on ${timestamp}`;
  }

  // Generic fallback
  return `Auto-generated ${sourceTypeName} to ${targetTypeName} mapping from '${source.resourceName}' to '${target.resourceName}' created on ${timestamp}`;
}

/**
 * Get display name for resource type
 */
function getResourceTypeDisplayName(type: ResourceType): string {
  const names: Record<ResourceType, string> = {
    database: 'Database',
    table: 'Table',
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
 * Sanitizes a mapping name to ensure it's valid (lowercase, replace spaces/special chars with underscores)
 */
export function sanitizeMappingName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}
