import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { MCPResource } from '@/lib/api/types';

export function useMCPResources(workspaceId: string) {
  const [mcpResources, setMCPResources] = useState<MCPResource[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMCPResources = useCallback(async () => {
    if (!workspaceId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mcpResources.list(workspaceId);
      setMCPResources(response.mcp_resources || []);
      setError(null);
    } catch (err) {
      console.error('[useMCPResources] Error fetching MCP resources:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP resources'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchMCPResources();
  }, [fetchMCPResources]);

  return {
    mcpResources,
    isLoading,
    error,
    refetch: fetchMCPResources,
  };
}

export function useMCPResource(workspaceId: string, resourceName: string) {
  const [mcpResource, setMCPResource] = useState<MCPResource | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMCPResource = useCallback(async () => {
    if (!workspaceId || !resourceName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mcpResources.show(workspaceId, resourceName);
      setMCPResource(response.mcp_resource);
      setError(null);
    } catch (err) {
      console.error('[useMCPResource] Error fetching MCP resource:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP resource'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, resourceName]);

  useEffect(() => {
    fetchMCPResource();
  }, [fetchMCPResource]);

  return {
    mcpResource,
    isLoading,
    error,
    refetch: fetchMCPResource,
  };
}

