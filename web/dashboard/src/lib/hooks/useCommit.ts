import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Commit } from '@/lib/api/types';

export function useCommit(
  workspaceId: string,
  repoName: string,
  branchName: string,
  commitCode: string
) {
  const [commit, setCommit] = useState<Commit | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchCommit = useCallback(async () => {
    if (!workspaceId || !repoName || !branchName || !commitCode) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.commits.show(workspaceId, repoName, branchName, commitCode);
      setCommit(response.commit);
      setError(null);
    } catch (err) {
      console.error('[useCommit] Error fetching commit:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch commit'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, repoName, branchName, commitCode]);

  useEffect(() => {
    fetchCommit();
  }, [fetchCommit]);

  return {
    commit,
    isLoading,
    error,
    refetch: fetchCommit,
  };
}

