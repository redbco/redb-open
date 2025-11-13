/**
 * URI Parser Utility
 * Parses redb:// resource URIs to extract database, table, and column information
 * Format: redb://database_id/dbname/table/table_name/column/column_name
 */

export interface ParsedResourceURI {
  databaseId: string;
  databaseName: string;
  tableName?: string;
  columnName?: string;
  resourceType: 'column' | 'table' | 'database';
  isValid: boolean;
  rawUri: string;
}

/**
 * Parses a redb:// resource URI into its components
 * @param uri - The resource URI to parse
 * @returns Parsed URI object or null if invalid
 */
export function parseResourceURI(uri: string): ParsedResourceURI | null {
  if (!uri || typeof uri !== 'string') {
    return null;
  }

  // Check if it starts with redb:// or redb:/
  if (!uri.startsWith('redb://') && !uri.startsWith('redb:/')) {
    return null;
  }

  try {
    // Remove the redb:// or redb:/ prefix
    let withoutProtocol: string;
    if (uri.startsWith('redb://')) {
      withoutProtocol = uri.substring(7);
    } else {
      withoutProtocol = uri.substring(6);
    }
    
    // Split by forward slashes
    const parts = withoutProtocol.split('/').filter(Boolean);
    
    if (parts.length < 2) {
      return null;
    }

    // Initialize result
    let databaseId = '';
    let databaseName = '';
    let tableName: string | undefined;
    let columnName: string | undefined;
    let resourceType: 'column' | 'table' | 'database' = 'database';

    // Parse based on the structure
    // Could be: data/database/db_id/table/table_name/column/column_name
    // Or: database_id/dbname/table/table_name/column/column_name
    
    let i = 0;
    
    // Skip 'data' if present
    if (parts[i] === 'data') {
      i++;
    }
    
    // Handle 'database' keyword followed by database ID
    if (parts[i] === 'database' && i + 1 < parts.length) {
      i++; // Skip 'database' keyword
      databaseId = parts[i];
      databaseName = parts[i]; // Use ID as name if no explicit name found
      i++;
    } else {
      // Fallback: first part is database ID, second is database name
      databaseId = parts[i] || '';
      databaseName = parts[i + 1] || parts[i] || '';
      i += 2;
    }

    // Parse remaining segments based on keywords
    while (i < parts.length) {
      const segment = parts[i];
      
      if (segment === 'table' && i + 1 < parts.length) {
        tableName = parts[i + 1];
        resourceType = 'table';
        i += 2;
      } else if (segment === 'column' && i + 1 < parts.length) {
        columnName = parts[i + 1];
        resourceType = 'column';
        i += 2;
      } else {
        i++;
      }
    }

    const result: ParsedResourceURI = {
      databaseId,
      databaseName,
      tableName,
      columnName,
      resourceType,
      isValid: true,
      rawUri: uri,
    };

    // Debug log for development
    if (process.env.NODE_ENV === 'development') {
      console.debug('[URI Parser]', { uri, parsed: result });
    }

    return result;
  } catch (error) {
    console.error('Error parsing resource URI:', error);
    return null;
  }
}

/**
 * Extracts database name from a resource URI
 * @param uri - The resource URI
 * @returns Database name or null
 */
export function extractDatabaseName(uri: string): string | null {
  const parsed = parseResourceURI(uri);
  return parsed?.databaseName || null;
}

/**
 * Extracts table name from a resource URI
 * @param uri - The resource URI
 * @returns Table name or null
 */
export function extractTableName(uri: string): string | null {
  const parsed = parseResourceURI(uri);
  return parsed?.tableName || null;
}

/**
 * Extracts column name from a resource URI
 * @param uri - The resource URI
 * @returns Column name or null
 */
export function extractColumnName(uri: string): string | null {
  const parsed = parseResourceURI(uri);
  return parsed?.columnName || null;
}

/**
 * Formats a parsed URI into a human-readable string
 * @param parsed - Parsed URI object
 * @returns Human-readable string
 */
export function formatParsedURI(parsed: ParsedResourceURI): string {
  if (!parsed.isValid) {
    return parsed.rawUri;
  }

  switch (parsed.resourceType) {
    case 'column':
      return parsed.tableName && parsed.columnName
        ? `${parsed.tableName}.${parsed.columnName}`
        : parsed.columnName || parsed.rawUri;
    case 'table':
      return parsed.tableName || parsed.rawUri;
    case 'database':
      return parsed.databaseName || parsed.rawUri;
    default:
      return parsed.rawUri;
  }
}

/**
 * Creates a display label for a resource URI with optional type information
 * @param uri - The resource URI
 * @param includeDatabase - Whether to include database name in the label
 * @returns Display label
 */
export function createDisplayLabel(uri: string, includeDatabase = false): string {
  const parsed = parseResourceURI(uri);
  
  if (!parsed) {
    return uri;
  }

  let label = '';
  
  if (includeDatabase && parsed.databaseName) {
    label += `${parsed.databaseName}.`;
  }
  
  if (parsed.tableName) {
    label += `${parsed.tableName}.`;
  }
  
  if (parsed.columnName) {
    label += parsed.columnName;
  } else if (parsed.tableName) {
    label = label.slice(0, -1); // Remove trailing dot
  } else if (parsed.databaseName) {
    label = label.slice(0, -1); // Remove trailing dot
  }

  return label || uri;
}

