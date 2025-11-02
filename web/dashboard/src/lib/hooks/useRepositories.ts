import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Repository } from '@/lib/api/types';

export function useRepositories(workspaceId: string) {
  const [repositories, setRepositories] = useState<Repository[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchRepositories = useCallback(async () => {
    if (!workspaceId) {
      setIsLoading(false);
      return;
    }

      try {
        setIsLoading(true);
        const response = await api.repositories.list(workspaceId);
        setRepositories(response.repos || []);
        setError(null);
      } catch (err) {
      console.error('[useRepositories] Error fetching repositories:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch repositories'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchRepositories();
  }, [fetchRepositories]);

  return {
    repositories,
    isLoading,
    error,
    refetch: fetchRepositories,
  };
}

export function useRepository(workspaceId: string, repoName: string) {
  const [repository, setRepository] = useState<Repository | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchRepository = useCallback(async () => {
    if (!workspaceId || !repoName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.repositories.show(workspaceId, repoName);
      setRepository(response.repo);
      setError(null);
    } catch (err) {
      console.error('[useRepository] Error fetching repository:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch repository'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, repoName]);

  useEffect(() => {
    fetchRepository();
  }, [fetchRepository]);

  return {
    repository,
    isLoading,
    error,
    refetch: fetchRepository,
  };
}

