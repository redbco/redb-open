import { ResourceType } from '../api/types';

/**
 * Build a resource URI for a database
 * Format: redb://data/database/{database_id}
 */
export function buildDatabaseURI(databaseId: string): string {
  return `redb://data/database/${databaseId}`;
}

/**
 * Build a resource URI for a table
 * Format: redb://data/database/{database_id}/table/{table_name}
 */
export function buildTableURI(databaseId: string, tableName: string): string {
  return `redb://data/database/${databaseId}/table/${tableName}`;
}

/**
 * Build a resource URI for an MCP resource
 * Format: redb://mcp/resource/{mcp_resource_id}
 */
export function buildMCPResourceURI(mcpResourceId: string): string {
  return `redb://mcp/resource/${mcpResourceId}`;
}

/**
 * Build a resource URI for an MCP tool
 * Format: redb://mcp/tool/{mcp_tool_id}
 */
export function buildMCPToolURI(mcpToolId: string): string {
  return `redb://mcp/tool/${mcpToolId}`;
}

/**
 * Build a resource URI for a webhook
 * Format: redb://webhook/{webhook_id}
 */
export function buildWebhookURI(webhookId: string): string {
  return `redb://webhook/${webhookId}`;
}

/**
 * Build a resource URI for a stream
 * Format: redb://stream/{stream_id}
 */
export function buildStreamURI(streamId: string): string {
  return `redb://stream/${streamId}`;
}

/**
 * Build a resource URI based on type and identifiers
 */
export function buildResourceURI(
  type: ResourceType,
  ...identifiers: string[]
): string {
  switch (type) {
    case 'database':
      if (identifiers.length < 1) {
        throw new Error('Database URI requires database_id');
      }
      return buildDatabaseURI(identifiers[0]);

    case 'table':
      if (identifiers.length < 2) {
        throw new Error('Table URI requires database_id and table_name');
      }
      return buildTableURI(identifiers[0], identifiers[1]);

    case 'mcp-resource':
      if (identifiers.length < 1) {
        throw new Error('MCP Resource URI requires mcp_resource_id');
      }
      return buildMCPResourceURI(identifiers[0]);

    case 'mcp-tool':
      if (identifiers.length < 1) {
        throw new Error('MCP Tool URI requires mcp_tool_id');
      }
      return buildMCPToolURI(identifiers[0]);

    case 'webhook':
      if (identifiers.length < 1) {
        throw new Error('Webhook URI requires webhook_id');
      }
      return buildWebhookURI(identifiers[0]);

    case 'stream':
      if (identifiers.length < 1) {
        throw new Error('Stream URI requires stream_id');
      }
      return buildStreamURI(identifiers[0]);

    default:
      throw new Error(`Unknown resource type: ${type}`);
  }
}

/**
 * Parse a resource URI into its components
 */
export interface ParsedResourceURI {
  protocol: string;
  type: ResourceType | null;
  databaseId?: string;
  databaseName?: string;
  tableName?: string;
  columnName?: string;
  resourceId?: string;
  full: string;
}

/**
 * Parse a resource URI string into components
 */
export function parseResourceURI(uri: string): ParsedResourceURI | null {
  if (!uri || !uri.startsWith('redb://')) {
    return null;
  }

  try {
    // Remove protocol
    const withoutProtocol = uri.replace('redb://', '');
    const parts = withoutProtocol.split('/');

    const result: ParsedResourceURI = {
      protocol: 'redb',
      type: null,
      full: uri,
    };

    // Parse based on first part
    if (parts[0] === 'data' && parts[1] === 'database') {
      result.type = 'database';
      result.databaseId = parts[2];

      if (parts.length > 4 && parts[3] === 'table') {
        result.type = 'table';
        result.tableName = parts[4];

        if (parts.length > 6 && parts[5] === 'column') {
          result.columnName = parts[6];
        }
      }
    } else if (parts[0] === 'mcp') {
      if (parts[1] === 'resource') {
        result.type = 'mcp-resource';
        result.resourceId = parts[2];
      } else if (parts[1] === 'tool') {
        result.type = 'mcp-tool';
        result.resourceId = parts[2];
      }
    } else if (parts[0] === 'webhook') {
      result.type = 'webhook';
      result.resourceId = parts[1];
    } else if (parts[0] === 'stream') {
      result.type = 'stream';
      result.resourceId = parts[1];
    }

    return result;
  } catch (error) {
    console.error('Failed to parse resource URI:', uri, error);
    return null;
  }
}

/**
 * Extract a human-readable name from a resource URI
 */
export function getResourceNameFromURI(uri: string): string {
  const parsed = parseResourceURI(uri);
  if (!parsed) return uri;

  if (parsed.columnName) {
    return parsed.columnName;
  }

  if (parsed.tableName) {
    return parsed.tableName;
  }

  if (parsed.databaseName) {
    return parsed.databaseName;
  }

  if (parsed.resourceId) {
    return parsed.resourceId;
  }

  return uri;
}

/**
 * Check if a URI is valid
 */
export function isValidResourceURI(uri: string): boolean {
  const parsed = parseResourceURI(uri);
  return parsed !== null && parsed.type !== null;
}

