'use client';

import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import { ApiClientError } from '@/lib/api/client';
import type { 
  Database, 
  ConnectDatabaseRequest,
  ConnectDatabaseStringRequest,
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

  const connectString = async (request: ConnectDatabaseStringRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.databases.connectString(workspaceName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to connect database');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { connect, connectString, isLoading, error };
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
      // Use the dedicated schema endpoint which enriches data from resource_items
      const response = await api.databases.getSchema(workspaceName, databaseName);
      
      if (!response.success) {
        throw new Error(response.message || 'Failed to fetch schema');
      }
      
      // The schema is already parsed and enriched with resource_items data
      const parsedSchema = response.schema;
      
      if (!parsedSchema) {
        throw new Error('No schema data available for this database');
      }
      
      // Check for new containers structure first, then fall back to legacy tables
      if (!parsedSchema.containers && !parsedSchema.tables) {
        console.warn('Schema does not contain containers or tables:', parsedSchema);
        setSchema({ ...parsedSchema, containers: [], tables: [] });
      } else {
        setSchema(parsedSchema);
      }
      
      setError(null);
    } catch (err) {
      console.error('Failed to fetch schema:', err);
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

