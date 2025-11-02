import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Relationship } from '@/lib/api/types';

export function useRelationships(workspaceId: string) {
  const [relationships, setRelationships] = useState<Relationship[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchRelationships = useCallback(async () => {
    if (!workspaceId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      console.log('[useRelationships] Fetching relationships for workspace:', workspaceId);
      const response = await api.relationships.list(workspaceId);
      console.log('[useRelationships] Received relationships:', response.relationships?.length || 0);
      setRelationships(response.relationships || []);
      setError(null);
    } catch (err) {
      console.error('[useRelationships] Error fetching relationships:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch relationships'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchRelationships();
  }, [fetchRelationships]);

  return {
    relationships,
    isLoading,
    error,
    refetch: fetchRelationships,
  };
}

export function useRelationship(workspaceId: string, relationshipId: string) {
  const [relationship, setRelationship] = useState<Relationship | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchRelationship = useCallback(async () => {
    if (!workspaceId || !relationshipId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.relationships.show(workspaceId, relationshipId);
      setRelationship(response.relationship);
      setError(null);
    } catch (err) {
      console.error('[useRelationship] Error fetching relationship:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch relationship'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, relationshipId]);

  useEffect(() => {
    fetchRelationship();
  }, [fetchRelationship]);

  return {
    relationship,
    isLoading,
    error,
    refetch: fetchRelationship,
  };
}

