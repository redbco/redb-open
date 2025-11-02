import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Branch } from '@/lib/api/types';

export function useBranch(workspaceId: string, repoName: string, branchName: string) {
  const [branch, setBranch] = useState<Branch | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchBranch = useCallback(async () => {
    if (!workspaceId || !repoName || !branchName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.branches.show(workspaceId, repoName, branchName);
      setBranch(response.branch);
      setError(null);
    } catch (err) {
      console.error('[useBranch] Error fetching branch:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch branch'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, repoName, branchName]);

  useEffect(() => {
    fetchBranch();
  }, [fetchBranch]);

  return {
    branch,
    isLoading,
    error,
    refetch: fetchBranch,
  };
}

