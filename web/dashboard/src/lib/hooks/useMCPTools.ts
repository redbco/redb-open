import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { MCPTool } from '@/lib/api/types';

export function useMCPTools(workspaceId: string) {
  const [mcpTools, setMCPTools] = useState<MCPTool[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMCPTools = useCallback(async () => {
    if (!workspaceId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mcpTools.list(workspaceId);
      setMCPTools(response.mcp_tools || []);
      setError(null);
    } catch (err) {
      console.error('[useMCPTools] Error fetching MCP tools:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP tools'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    fetchMCPTools();
  }, [fetchMCPTools]);

  return {
    mcpTools,
    isLoading,
    error,
    refetch: fetchMCPTools,
  };
}

export function useMCPTool(workspaceId: string, toolName: string) {
  const [mcpTool, setMCPTool] = useState<MCPTool | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMCPTool = useCallback(async () => {
    if (!workspaceId || !toolName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mcpTools.show(workspaceId, toolName);
      setMCPTool(response.mcp_tool);
      setError(null);
    } catch (err) {
      console.error('[useMCPTool] Error fetching MCP tool:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP tool'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceId, toolName]);

  useEffect(() => {
    fetchMCPTool();
  }, [fetchMCPTool]);

  return {
    mcpTool,
    isLoading,
    error,
    refetch: fetchMCPTool,
  };
}

