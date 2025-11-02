import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Instance } from '@/lib/api/types';

export function useInstances(workspaceId: string) {
  const [instances, setInstances] = useState<Instance[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchInstances = useCallback(async () => {
    if (!workspaceId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      console.log('[useInstances] Fetching instances for workspace:', workspaceId);
      const response = await api.instances.list(workspaceId);
      console.log('[useInstances] Received instances:', response.instances?.length || 0);
      setInstances(response.instances || []);
      setError(null);
    } catch (err) {
      console.error('[useInstances] Error fetching instances:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch instances'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchInstances();
  }, [fetchInstances]);

  return {
    instances,
    isLoading,
    error,
    refetch: fetchInstances,
  };
}

export function useInstance(workspaceId: string, instanceName: string) {
  const [instance, setInstance] = useState<Instance | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchInstance = useCallback(async () => {
    if (!workspaceId || !instanceName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.instances.show(workspaceId, instanceName);
      setInstance(response.instance);
      setError(null);
    } catch (err) {
      console.error('[useInstance] Error fetching instance:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch instance'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, instanceName]);

  useEffect(() => {
    fetchInstance();
  }, [fetchInstance]);

  return {
    instance,
    isLoading,
    error,
    refetch: fetchInstance,
  };
}

