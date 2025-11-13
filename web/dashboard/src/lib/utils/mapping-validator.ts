import { ResourceType } from '../api/types';

/**
 * Valid mapping type pairs
 */
const VALID_MAPPING_PAIRS: Record<ResourceType, ResourceType[]> = {
  database: ['database'],
  table: ['table', 'mcp-resource', 'mcp-tool', 'webhook', 'stream'],
  'mcp-resource': ['table'],
  'mcp-tool': ['table'],
  webhook: ['table'],
  stream: ['table'],
};

/**
 * Check if a source-target mapping pair is valid
 */
export function validateMappingPair(
  sourceType: ResourceType,
  targetType: ResourceType
): boolean {
  const validTargets = VALID_MAPPING_PAIRS[sourceType] || [];
  return validTargets.includes(targetType);
}

/**
 * Get list of valid target types for a given source type
 */
export function getValidTargetTypes(sourceType: ResourceType): ResourceType[] {
  return VALID_MAPPING_PAIRS[sourceType] || [];
}

/**
 * Determine if auto-mapping makes sense for a source-target pair
 * Auto-mapping is primarily useful for database-to-database and table-to-table mappings
 */
export function requiresAutoMapping(
  sourceType: ResourceType,
  targetType: ResourceType
): boolean {
  // Auto-mapping makes sense when both sides have similar structures
  return (
    (sourceType === 'database' && targetType === 'database') ||
    (sourceType === 'table' && targetType === 'table')
  );
}

/**
 * Generate a helpful description for a mapping type
 */
export function getMappingDescription(
  sourceType: ResourceType,
  targetType: ResourceType
): string {
  const descriptions: Record<string, string> = {
    'database-database':
      'Map all tables from source database to target database with automatic column matching',
    'table-table':
      'Map source table columns to target table columns with optional transformations',
    'table-mcp-resource':
      'Expose source table as an MCP resource with virtual schema representation',
    'table-mcp-tool':
      'Expose source table operations as MCP tool functions',
    'table-webhook':
      'Publish CDC events from source table to webhook endpoint',
    'table-stream':
      'Publish CDC events from source table to stream',
    'webhook-table':
      'Map incoming webhook events to target table inserts/updates',
    'stream-table':
      'Map incoming stream events to target table inserts/updates',
    'mcp-resource-table':
      'Map MCP resource data to target table',
    'mcp-tool-table':
      'Map MCP tool results to target table',
  };

  const key = `${sourceType}-${targetType}`;
  return (
    descriptions[key] ||
    `Map data from ${sourceType} to ${targetType}`
  );
}

/**
 * Get user-friendly name for resource type
 */
export function getResourceTypeName(type: ResourceType): string {
  const names: Record<ResourceType, string> = {
    database: 'Database',
    table: 'Table',
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
  return ['database', 'table'].includes(type);
}

/**
 * Get icon name for resource type (for lucide-react icons)
 */
export function getResourceTypeIcon(type: ResourceType): string {
  const icons: Record<ResourceType, string> = {
    database: 'Database',
    table: 'Table',
    'mcp-resource': 'Box',
    'mcp-tool': 'Wrench',
    webhook: 'Webhook',
    stream: 'Activity',
  };

  return icons[type] || 'HelpCircle';
}

