import { ResourceSelection, ResourceType } from '@/lib/api/types';

/**
 * Generates an auto-generated mapping name based on source and target resources
 * This follows the same logic as the CLI implementation
 */
export function generateMappingName(
  source: ResourceSelection,
  target: ResourceSelection
): string {
  const sourceDB = source.databaseName || source.resourceName;
  const sourceTable = source.tableName || '';
  const targetDB = target.databaseName || target.resourceName;
  const targetTable = target.tableName || '';

  // Database to Database
  if (source.type === 'database' && target.type === 'database') {
    return `${sourceDB}_to_${targetDB}`;
  }

  // Table to Table
  if (source.type === 'table' && target.type === 'table') {
    return `${sourceDB}_${sourceTable}_to_${targetDB}_${targetTable}`;
  }

  // Table to MCP Resource
  if (source.type === 'table' && target.type === 'mcp-resource') {
    return `${sourceDB}_${sourceTable}_to_mcp_${target.resourceName}`;
  }

  // Table to MCP Tool
  if (source.type === 'table' && target.type === 'mcp-tool') {
    return `${sourceDB}_${sourceTable}_to_mcp_${target.resourceName}`;
  }

  // Database to MCP Resource
  if (source.type === 'database' && target.type === 'mcp-resource') {
    return `${sourceDB}_to_mcp_${target.resourceName}`;
  }

  // Database to MCP Tool
  if (source.type === 'database' && target.type === 'mcp-tool') {
    return `${sourceDB}_to_mcp_${target.resourceName}`;
  }

  // Table to Webhook
  if (source.type === 'table' && target.type === 'webhook') {
    return `${sourceDB}_${sourceTable}_to_webhook_${target.resourceName}`;
  }

  // Table to Stream
  if (source.type === 'table' && target.type === 'stream') {
    return `${sourceDB}_${sourceTable}_to_stream_${target.resourceName}`;
  }

  // Webhook to Table
  if (source.type === 'webhook' && target.type === 'table') {
    return `webhook_${source.resourceName}_to_${targetDB}_${targetTable}`;
  }

  // Stream to Table
  if (source.type === 'stream' && target.type === 'table') {
    return `stream_${source.resourceName}_to_${targetDB}_${targetTable}`;
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
  const sourceTable = source.tableName || '';
  const targetDB = target.databaseName || target.resourceName;
  const targetTable = target.tableName || '';

  // Database to Database
  if (source.type === 'database' && target.type === 'database') {
    return `Auto-generated database mapping from '${sourceDB}' to '${targetDB}' created on ${timestamp}`;
  }

  // Table to Table
  if (source.type === 'table' && target.type === 'table') {
    return `Auto-generated table mapping from '${sourceDB}.${sourceTable}' to '${targetDB}.${targetTable}' created on ${timestamp}`;
  }

  // Table to MCP Resource
  if (source.type === 'table' && target.type === 'mcp-resource') {
    return `Auto-generated MCP resource mapping from '${sourceDB}.${sourceTable}' to MCP resource '${target.resourceName}' created on ${timestamp}`;
  }

  // Table to MCP Tool
  if (source.type === 'table' && target.type === 'mcp-tool') {
    return `Auto-generated MCP tool mapping from '${sourceDB}.${sourceTable}' to MCP tool '${target.resourceName}' created on ${timestamp}`;
  }

  // Database to MCP Resource
  if (source.type === 'database' && target.type === 'mcp-resource') {
    return `Auto-generated MCP resource mapping from database '${sourceDB}' to MCP resource '${target.resourceName}' created on ${timestamp}`;
  }

  // Database to MCP Tool
  if (source.type === 'database' && target.type === 'mcp-tool') {
    return `Auto-generated MCP tool mapping from database '${sourceDB}' to MCP tool '${target.resourceName}' created on ${timestamp}`;
  }

  // Table to Webhook
  if (source.type === 'table' && target.type === 'webhook') {
    return `Auto-generated webhook mapping from '${sourceDB}.${sourceTable}' to webhook '${target.resourceName}' for CDC events created on ${timestamp}`;
  }

  // Table to Stream
  if (source.type === 'table' && target.type === 'stream') {
    return `Auto-generated stream mapping from '${sourceDB}.${sourceTable}' to stream '${target.resourceName}' for CDC events created on ${timestamp}`;
  }

  // Webhook to Table
  if (source.type === 'webhook' && target.type === 'table') {
    return `Auto-generated webhook-to-table mapping from webhook '${source.resourceName}' to '${targetDB}.${targetTable}' created on ${timestamp}`;
  }

  // Stream to Table
  if (source.type === 'stream' && target.type === 'table') {
    return `Auto-generated stream-to-table mapping from stream '${source.resourceName}' to '${targetDB}.${targetTable}' created on ${timestamp}`;
  }

  // Generic fallback
  return `Auto-generated mapping from '${source.resourceName}' to '${target.resourceName}' created on ${timestamp}`;
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

