import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Environment } from '@/lib/api/types';

export function useEnvironments(workspaceId: string) {
  const [environments, setEnvironments] = useState<Environment[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchEnvironments = useCallback(async () => {
    if (!workspaceId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.environments.list(workspaceId);
      setEnvironments(response.environments || []);
      setError(null);
    } catch (err) {
      console.error('[useEnvironments] Error fetching environments:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch environments'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchEnvironments();
  }, [fetchEnvironments]);

  return {
    environments,
    isLoading,
    error,
    refetch: fetchEnvironments,
  };
}

export function useEnvironment(workspaceId: string, environmentName: string) {
  const [environment, setEnvironment] = useState<Environment | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchEnvironment = useCallback(async () => {
    if (!workspaceId || !environmentName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.environments.show(workspaceId, environmentName);
      setEnvironment(response.environment);
      setError(null);
    } catch (err) {
      console.error('[useEnvironment] Error fetching environment:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch environment'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, environmentName]);

  useEffect(() => {
    fetchEnvironment();
  }, [fetchEnvironment]);

  return {
    environment,
    isLoading,
    error,
    refetch: fetchEnvironment,
  };
}

