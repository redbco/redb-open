import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Mapping } from '@/lib/api/types';

export function useMappings(workspaceId: string) {
  const [mappings, setMappings] = useState<Mapping[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isFetching = useRef(false);

  const fetchMappings = useCallback(async () => {
    console.log('[useMappings] fetchMappings called with workspaceId:', workspaceId, 'isEmpty:', !workspaceId);
    
    if (!workspaceId) {
      console.log('[useMappings] Skipping fetch - no workspaceId');
      setIsLoading(false);
      return;
    }

    // Prevent concurrent fetches
    if (isFetching.current) {
      console.log('[useMappings] Already fetching, skipping...');
      return;
    }

    // Check if token is available before making the request
    const token = typeof window !== 'undefined' ? localStorage.getItem('auth_token') : null;
    console.log('[useMappings] About to fetch, token available:', !!token, 'length:', token?.length || 0);

    try {
      isFetching.current = true;
      setIsLoading(true);
      console.log('[useMappings] Fetching mappings for workspace:', workspaceId);
      const response = await api.mappings.list(workspaceId);
      console.log('[useMappings] Received mappings:', response.mappings?.length || 0);
      setMappings(response.mappings || []);
      setError(null);
    } catch (err) {
      console.error('[useMappings] Error fetching mappings:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch mappings'));
    } finally {
      setIsLoading(false);
      isFetching.current = false;
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchMappings();
  }, [fetchMappings]);

  return {
    mappings,
    isLoading,
    error,
    refetch: fetchMappings,
  };
}

export function useMapping(workspaceId: string, mappingId: string) {
  const [mapping, setMapping] = useState<Mapping | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMapping = useCallback(async () => {
    if (!workspaceId || !mappingId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mappings.show(workspaceId, mappingId);
      setMapping(response.mapping);
      setError(null);
    } catch (err) {
      console.error('[useMapping] Error fetching mapping:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch mapping'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, mappingId]);

  useEffect(() => {
    fetchMapping();
  }, [fetchMapping]);

  return {
    mapping,
    isLoading,
    error,
    refetch: fetchMapping,
  };
}

