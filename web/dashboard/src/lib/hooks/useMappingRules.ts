import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { MappingRule } from '@/lib/api/types';

export function useMappingRules(workspaceId: string, mappingName: string) {
  const [mappingRules, setMappingRules] = useState<MappingRule[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMappingRules = useCallback(async () => {
    if (!workspaceId || !mappingName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mappingRules.list(workspaceId, mappingName);
      setMappingRules(response.rules || []);
      setError(null);
    } catch (err) {
      console.error('[useMappingRules] Error fetching mapping rules:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch mapping rules'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, mappingName]);

  useEffect(() => {
    fetchMappingRules();
  }, [fetchMappingRules]);

  return {
    mappingRules,
    isLoading,
    error,
    refetch: fetchMappingRules,
  };
}

