import { useState, useEffect, useCallback, useMemo } from 'react';
import { api } from '@/lib/api/endpoints';
import type { DatabaseSchema, SchemaTable, SchemaColumn } from '@/lib/api/types';

interface DatabaseSchemaCache {
  [workspaceId: string]: {
    [databaseName: string]: {
      schema: DatabaseSchema;
      fetchedAt: number;
    };
  };
}

// Global cache with 5-minute TTL
const schemaCache: DatabaseSchemaCache = {};
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

/**
 * Hook to fetch and cache database schema information
 * Provides lightweight access to schema data with caching to avoid redundant API calls
 */
export function useDatabaseSchemaInfo(workspaceId: string, databaseName: string | null) {
  const [schema, setSchema] = useState<DatabaseSchema | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchSchema = useCallback(async () => {
    if (!workspaceId || !databaseName) {
      setIsLoading(false);
      return;
    }

    // Check cache first
    const cached = schemaCache[workspaceId]?.[databaseName];
    if (cached && Date.now() - cached.fetchedAt < CACHE_TTL) {
      setSchema(cached.schema);
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      // Get database details which includes the schema
      const response = await api.databases.show(workspaceId, databaseName);
      
      // Parse the schema from the database object
      let schemaData: DatabaseSchema = {};
      
      if (response.database.database_schema) {
        try {
          // The schema is double-encoded JSON string that needs parsing twice
          let schemaString = response.database.database_schema;
          
          // First parse if it's a string
          if (typeof schemaString === 'string') {
            schemaString = JSON.parse(schemaString);
          }
          
          // Second parse if it's still a string (double-encoded)
          if (typeof schemaString === 'string') {
            schemaData = JSON.parse(schemaString);
          } else {
            schemaData = schemaString as DatabaseSchema;
          }
          
          // Convert the schema format if needed
          // The new API returns containers, but we may need to handle legacy tables format
          if (schemaData.containers && Array.isArray(schemaData.containers)) {
            // New format with containers - use as is
            schemaData = { ...schemaData };
          } else if (schemaData.tables && !Array.isArray(schemaData.tables)) {
            // Legacy format: tables as object with table names as keys - convert to array
            const tablesObj = schemaData.tables as any;
            schemaData.tables = Object.values(tablesObj).map((table: any) => ({
              name: table.name,
              schema: table.schema,
              engine: table.engine,
              // Columns are also in object format, convert to array
              columns: table.columns ? Object.values(table.columns).map((col: any) => ({
                ...col, // Preserve all original fields
                name: col.name,
                dataType: col.data_type || col.dataType,
                type: col.type || col.data_type,
                isNullable: col.is_nullable ?? col.isNullable ?? col.nullable,
                isPrimaryKey: col.is_primary_key || col.isPrimaryKey,
                isUnique: col.is_unique || col.isUnique,
                isAutoIncrement: col.is_auto_increment || col.auto_increment || col.isAutoIncrement,
                columnDefault: col.column_default || col.columnDefault || col.default,
                varcharLength: col.varchar_length || col.varcharLength,
              })) : [],
              indexes: table.indexes,
              constraints: table.constraints,
              tableType: table.table_type,
              primaryCategory: table.primary_category,
              classificationScores: table.classification_scores,
              classificationConfidence: table.classification_confidence,
            }));
          }
        } catch (parseError) {
          console.error('[useDatabaseSchemaInfo] Error parsing schema JSON:', parseError);
          console.error('[useDatabaseSchemaInfo] Raw schema data:', response.database.database_schema);
          schemaData = {};
        }
      }

      // Update cache
      if (!schemaCache[workspaceId]) {
        schemaCache[workspaceId] = {};
      }
      schemaCache[workspaceId][databaseName] = {
        schema: schemaData,
        fetchedAt: Date.now(),
      };

      setSchema(schemaData);
      setError(null);
    } catch (err) {
      console.error('[useDatabaseSchemaInfo] Error fetching schema:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch schema'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, databaseName]);

  useEffect(() => {
    fetchSchema();
  }, [fetchSchema]);

  return {
    schema,
    isLoading,
    error,
    refetch: fetchSchema,
  };
}

/**
 * Hook to get column information from a database schema
 * Provides efficient lookup of column details by table and column name
 */
export function useColumnInfo(
  workspaceId: string,
  databaseName: string | null,
  tableName: string | null,
  columnName: string | null
) {
  const { schema, isLoading, error } = useDatabaseSchemaInfo(workspaceId, databaseName);

  const columnInfo = useMemo(() => {
    if (!schema || !tableName || !columnName) {
      return null;
    }

    const table = schema.tables?.find((t) => t.name === tableName);
    if (!table) {
      return null;
    }

    const column = table.columns?.find((c) => c.name === columnName);
    return column || null;
  }, [schema, tableName, columnName]);

  const tableInfo = useMemo(() => {
    if (!schema || !tableName) {
      return null;
    }

    return schema.tables?.find((t) => t.name === tableName) || null;
  }, [schema, tableName]);

  return {
    column: columnInfo,
    table: tableInfo,
    schema,
    isLoading,
    error,
  };
}

/**
 * Hook to fetch multiple database schemas in parallel
 * Useful when you need to fetch source and target database schemas simultaneously
 */
export function useMultipleDatabaseSchemas(
  workspaceId: string,
  databaseNames: (string | null)[]
) {
  const [schemas, setSchemas] = useState<{ [databaseName: string]: DatabaseSchema }>({});
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchSchemas = useCallback(async () => {
    if (!workspaceId || databaseNames.length === 0) {
      setIsLoading(false);
      return;
    }

    const validDatabaseNames = databaseNames.filter((name): name is string => name !== null);
    
    if (validDatabaseNames.length === 0) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      
      // Fetch all schemas in parallel
      const fetchPromises = validDatabaseNames.map(async (dbName) => {
        // Check cache first
        const cached = schemaCache[workspaceId]?.[dbName];
        if (cached && Date.now() - cached.fetchedAt < CACHE_TTL) {
          return { dbName, schema: cached.schema };
        }

        // Fetch from API - get database details which includes schema
        const response = await api.databases.show(workspaceId, dbName);
        
        // Parse the schema from the database object
        let schemaData: DatabaseSchema = {};
        
        if (response.database.database_schema) {
          try {
            // The schema is double-encoded JSON string that needs parsing twice
            let schemaString = response.database.database_schema;
            
            // First parse if it's a string
            if (typeof schemaString === 'string') {
              schemaString = JSON.parse(schemaString);
            }
            
            // Second parse if it's still a string (double-encoded)
            if (typeof schemaString === 'string') {
              schemaData = JSON.parse(schemaString);
            } else {
              schemaData = schemaString as DatabaseSchema;
            }
            
            // Convert the schema format if needed
            // The new API returns containers, but we may need to handle legacy tables format
            if (schemaData.containers && Array.isArray(schemaData.containers)) {
              // New format with containers - use as is
              schemaData = { ...schemaData };
            } else if (schemaData.tables && !Array.isArray(schemaData.tables)) {
              // Legacy format: tables as object with table names as keys - convert to array
              const tablesObj = schemaData.tables as any;
              schemaData.tables = Object.values(tablesObj).map((table: any) => ({
                name: table.name,
                schema: table.schema,
                engine: table.engine,
                // Columns are also in object format, convert to array
                columns: table.columns ? Object.values(table.columns).map((col: any) => ({
                  ...col, // Preserve all original fields
                  name: col.name,
                  dataType: col.data_type || col.dataType,
                  type: col.type || col.data_type,
                  isNullable: col.is_nullable ?? col.isNullable ?? col.nullable,
                  isPrimaryKey: col.is_primary_key || col.isPrimaryKey,
                  isUnique: col.is_unique || col.isUnique,
                  isAutoIncrement: col.is_auto_increment || col.auto_increment || col.isAutoIncrement,
                  columnDefault: col.column_default || col.columnDefault || col.default,
                  varcharLength: col.varchar_length || col.varcharLength,
                })) : [],
                indexes: table.indexes,
                constraints: table.constraints,
                tableType: table.table_type,
                primaryCategory: table.primary_category,
                classificationScores: table.classification_scores,
                classificationConfidence: table.classification_confidence,
              }));
            }
          } catch (parseError) {
            console.error('[useMultipleDatabaseSchemas] Error parsing schema JSON for', dbName, ':', parseError);
            console.error('[useMultipleDatabaseSchemas] Raw schema data:', response.database.database_schema);
            schemaData = {};
          }
        }

        // Update cache
        if (!schemaCache[workspaceId]) {
          schemaCache[workspaceId] = {};
        }
        schemaCache[workspaceId][dbName] = {
          schema: schemaData,
          fetchedAt: Date.now(),
        };

        return { dbName, schema: schemaData };
      });

      const results = await Promise.all(fetchPromises);
      
      // Build schemas object
      const schemasObj: { [key: string]: DatabaseSchema } = {};
      results.forEach(({ dbName, schema }) => {
        schemasObj[dbName] = schema;
      });

      setSchemas(schemasObj);
      setError(null);
    } catch (err) {
      console.error('[useMultipleDatabaseSchemas] Error fetching schemas:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch schemas'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, databaseNames.join(',')]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    fetchSchemas();
  }, [fetchSchemas]);

  return {
    schemas,
    isLoading,
    error,
    refetch: fetchSchemas,
  };
}

/**
 * Helper function to get column data type display string
 */
export function getColumnTypeDisplay(column: SchemaColumn): string {
  // Try both camelCase and snake_case field names
  let typeStr = column.dataType || column.type || (column as any).data_type || 'unknown';
  
  // Add varchar length if available
  const length = column.varcharLength || column.varchar_length || (column as any).varchar_length;
  if (length) {
    // Check if the type already includes length
    if (!typeStr.includes('(')) {
      typeStr += `(${length})`;
    }
  }

  return typeStr.toUpperCase();
}

/**
 * Helper function to get column constraint badges
 */
export function getColumnConstraints(column: SchemaColumn): string[] {
  const constraints: string[] = [];
  
  // Try both camelCase and snake_case field names
  const isPrimaryKey = column.isPrimaryKey || column.is_primary_key || (column as any).is_primary_key;
  const isNullable = column.isNullable ?? (column as any).is_nullable;
  const isAutoIncrement = column.isAutoIncrement || column.is_auto_increment || (column as any).is_auto_increment || (column as any).auto_increment;
  
  if (isPrimaryKey) {
    constraints.push('PK');
  }
  
  if (column.isUnique || (column as any).is_unique) {
    constraints.push('UNIQUE');
  }
  
  if (isNullable === false || (column as any).nullable === false) {
    constraints.push('NOT NULL');
  }
  
  if (isAutoIncrement) {
    constraints.push('AUTO_INCREMENT');
  }

  return constraints;
}

/**
 * Clear the schema cache for a specific workspace and database
 */
export function clearSchemaCache(workspaceId?: string, databaseName?: string) {
  if (workspaceId && databaseName) {
    if (schemaCache[workspaceId]) {
      delete schemaCache[workspaceId][databaseName];
    }
  } else if (workspaceId) {
    delete schemaCache[workspaceId];
  } else {
    // Clear all
    Object.keys(schemaCache).forEach((key) => delete schemaCache[key]);
  }
}

