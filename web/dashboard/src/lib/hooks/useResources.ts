'use client';

import { useState, useEffect } from 'react';
import { api } from '../api/endpoints';
import type {
  Database,
  MCPResource,
  MCPTool,
  Webhook,
  Stream,
  ResourceContainer,
  ResourceItem,
} from '../api/types';

/**
 * Hook for fetching resource containers
 */
export function useResourceContainers(workspaceName: string, type?: string) {
  const [containers, setContainers] = useState<ResourceContainer[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchContainers = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.resources.listContainers(workspaceName, type ? { object_type: type } : undefined);
      setContainers(response.containers || []);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch resource containers'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchContainers();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName, type]);

  return {
    containers,
    isLoading,
    error,
    refetch: fetchContainers,
  };
}

/**
 * Hook for fetching resource items in a container
 */
export function useResourceItems(workspaceName: string, containerId: string) {
  const [items, setItems] = useState<ResourceItem[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchItems = async () => {
    if (!workspaceName || !containerId) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.resources.listItems(workspaceName, containerId ? { container_id: containerId } : undefined);
      setItems(response.items || []);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch resource items'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchItems();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName, containerId]);

  return {
    items,
    isLoading,
    error,
    refetch: fetchItems,
  };
}

/**
 * Hook for fetching databases (wrapper around database endpoint)
 */
export function useDatabases(workspaceName: string) {
  const [databases, setDatabases] = useState<Database[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchDatabases = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.databases.list(workspaceName);
      setDatabases(response.databases || []);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch databases'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchDatabases();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName]);

  return {
    databases,
    isLoading,
    error,
    refetch: fetchDatabases,
  };
}

/**
 * Hook for fetching MCP resources
 */
export function useMCPResources(workspaceName: string) {
  const [mcpResources, setMcpResources] = useState<MCPResource[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMCPResources = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.mcpResources.list(workspaceName);
      setMcpResources(response.mcp_resources || []);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP resources'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchMCPResources();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName]);

  return {
    mcpResources,
    isLoading,
    error,
    refetch: fetchMCPResources,
  };
}

/**
 * Hook for fetching MCP tools
 */
export function useMCPTools(workspaceName: string) {
  const [mcpTools, setMcpTools] = useState<MCPTool[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMCPTools = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.mcpTools.list(workspaceName);
      setMcpTools(response.mcp_tools || []);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP tools'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchMCPTools();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName]);

  return {
    mcpTools,
    isLoading,
    error,
    refetch: fetchMCPTools,
  };
}

/**
 * Hook for fetching webhooks (placeholder)
 */
export function useWebhooks(workspaceName: string) {
  const [webhooks, setWebhooks] = useState<Webhook[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchWebhooks = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.webhooks.list();
      setWebhooks(response.webhooks || []);
    } catch (err) {
      console.error(err);
      // Expected for placeholder
      setWebhooks([]);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchWebhooks();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName]);

  return {
    webhooks,
    isLoading,
    error,
    refetch: fetchWebhooks,
  };
}

/**
 * Hook for fetching streams (placeholder)
 */
export function useStreams(workspaceName: string) {
  const [streams, setStreams] = useState<Stream[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchStreams = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    
    try {
      const response = await api.streams.list(workspaceName);
      setStreams(response.streams || []);
    } catch (err) {
      console.error(err);
      // Expected for placeholder
      setStreams([]);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchStreams();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceName]);

  return {
    streams,
    isLoading,
    error,
    refetch: fetchStreams,
  };
}

