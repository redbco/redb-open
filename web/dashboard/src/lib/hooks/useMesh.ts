import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Mesh, Node } from '@/lib/api/types';

export function useMesh() {
  const [mesh, setMesh] = useState<Mesh | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchMesh = useCallback(async () => {
    try {
      setIsLoading(true);
      const response = await api.mesh.show();
      setMesh(response.mesh);
      setError(null);
    } catch (err) {
      console.error('[useMesh] Error fetching mesh:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch mesh'));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchMesh();
  }, [fetchMesh]);

  return {
    mesh,
    isLoading,
    error,
    refetch: fetchMesh,
  };
}

export function useNodes() {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchNodes = useCallback(async () => {
    try {
      setIsLoading(true);
      const response = await api.mesh.listNodes();
      setNodes(response.nodes || []);
      setError(null);
    } catch (err) {
      console.error('[useNodes] Error fetching nodes:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch nodes'));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchNodes();
  }, [fetchNodes]);

  return {
    nodes,
    isLoading,
    error,
    refetch: fetchNodes,
  };
}

export function useNode(nodeId: string) {
  const [node, setNode] = useState<Node | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchNode = useCallback(async () => {
    if (!nodeId) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.mesh.showNode(nodeId);
      setNode(response.node);
      setError(null);
    } catch (err) {
      console.error('[useNode] Error fetching node:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch node'));
    } finally {
      setIsLoading(false);
    }
  }, [nodeId]);

  useEffect(() => {
    fetchNode();
  }, [fetchNode]);

  return {
    node,
    isLoading,
    error,
    refetch: fetchNode,
  };
}

