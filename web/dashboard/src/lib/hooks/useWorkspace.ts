'use client';

import { useState, useEffect } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Workspace } from '@/lib/api/types';

export function useWorkspaces() {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchWorkspaces = async () => {
      try {
        setIsLoading(true);
        console.log('[useWorkspaces] Fetching workspaces...');
        const response = await api.workspaces.list();
        console.log('[useWorkspaces] Received workspaces:', response.workspaces?.length || 0);
        setWorkspaces(response.workspaces);
        setError(null);
      } catch (err) {
        console.error('[useWorkspaces] Error fetching workspaces:', err);
        setError(err instanceof Error ? err : new Error('Failed to fetch workspaces'));
      } finally {
        setIsLoading(false);
      }
    };

    fetchWorkspaces();
  }, []);

  return { workspaces, isLoading, error };
}

export function useWorkspace(workspaceId: string) {
  const { workspaces, isLoading, error } = useWorkspaces();
  const workspace = workspaces.find(w => w.workspace_id === workspaceId || w.workspace_name === workspaceId);

  return { workspace, isLoading, error };
}

