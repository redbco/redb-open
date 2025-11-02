'use client';

import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import { ApiClientError } from '@/lib/api/client';
import type { 
  Database, 
  ConnectDatabaseRequest, 
  ModifyDatabaseRequest,
  DisconnectDatabaseRequest,
  DatabaseSchema
} from '@/lib/api/types';

export function useDatabases(workspaceName: string) {
  const [databases, setDatabases] = useState<Database[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchDatabases = useCallback(async () => {
    if (!workspaceName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      const response = await api.databases.list(workspaceName);
      setDatabases(response.databases);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch databases'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName]);

  useEffect(() => {
    fetchDatabases();
  }, [fetchDatabases]);

  return { databases, isLoading, error, refetch: fetchDatabases };
}

export function useDatabase(workspaceName: string, databaseName: string) {
  const [database, setDatabase] = useState<Database | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchDatabase = useCallback(async () => {
    if (!workspaceName || !databaseName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      const response = await api.databases.show(workspaceName, databaseName);
      setDatabase(response.database);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch database'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName, databaseName]);

  useEffect(() => {
    fetchDatabase();
  }, [fetchDatabase]);

  return { database, isLoading, error, refetch: fetchDatabase };
}

export function useConnectDatabase(workspaceName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const connect = async (request: ConnectDatabaseRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.databases.connect(workspaceName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to connect database');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { connect, isLoading, error };
}

export function useModifyDatabase(workspaceName: string, databaseName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const modify = async (request: ModifyDatabaseRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.databases.modify(workspaceName, databaseName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to modify database');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { modify, isLoading, error };
}

export function useDisconnectDatabase(workspaceName: string, databaseName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const disconnect = async (request?: DisconnectDatabaseRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.databases.disconnect(workspaceName, databaseName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to disconnect database');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { disconnect, isLoading, error };
}

export function useDatabaseSchema(workspaceName: string, databaseName: string) {
  const [schema, setSchema] = useState<DatabaseSchema | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchSchema = useCallback(async () => {
    if (!workspaceName || !databaseName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      // Fetch the database details which includes schema and tables data
      const response = await api.databases.show(workspaceName, databaseName);
      
      // Parse the schema from database_tables (primary) or database_schema (fallback)
      let parsedSchema: DatabaseSchema | null = null;
      
      if (response.database.database_tables) {
        try {
          // The database_tables field contains the schema with table information
          let tablesString = response.database.database_tables;
          
          // Unescape if needed (handle double-encoded JSON)
          if (tablesString.startsWith('"') && tablesString.endsWith('"')) {
            tablesString = tablesString.slice(1, -1);
            tablesString = tablesString.replace(/\\"/g, '"').replace(/\\\\/g, '\\');
          }
          
          const tablesData = JSON.parse(tablesString);
          
          // The parsed data might be a TablesData structure with a 'tables' field,
          // or it might already be a DatabaseSchema structure
          if (tablesData.tables && Array.isArray(tablesData.tables)) {
            parsedSchema = tablesData;
          } else if (Array.isArray(tablesData)) {
            // If it's directly an array, wrap it in the expected structure
            parsedSchema = { tables: tablesData };
          } else {
            parsedSchema = tablesData;
          }
          
          console.log('Parsed schema from database_tables:', parsedSchema);
        } catch (parseErr) {
          console.error('Failed to parse database_tables:', parseErr, response.database.database_tables);
        }
      }
      
      // Fallback to database_schema if database_tables is not available
      if (!parsedSchema && response.database.database_schema) {
        try {
          let schemaString = response.database.database_schema;
          
          // Unescape if needed (handle double-encoded JSON)
          if (schemaString.startsWith('"') && schemaString.endsWith('"')) {
            schemaString = schemaString.slice(1, -1);
            schemaString = schemaString.replace(/\\"/g, '"').replace(/\\\\/g, '\\');
          }
          
          parsedSchema = JSON.parse(schemaString);
          console.log('Parsed schema from database_schema:', parsedSchema);
        } catch (parseErr) {
          console.error('Failed to parse database_schema:', parseErr, response.database.database_schema);
        }
      }
      
      if (!parsedSchema) {
        throw new Error('No schema data available for this database');
      }
      
      // Ensure we have a tables array
      if (!parsedSchema.tables) {
        console.warn('Schema does not contain tables array:', parsedSchema);
        parsedSchema = { ...parsedSchema, tables: [] };
      }
      
      setSchema(parsedSchema);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch schema'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName, databaseName]);

  useEffect(() => {
    fetchSchema();
  }, [fetchSchema]);

  return { schema, isLoading, error, refetch: fetchSchema };
}

