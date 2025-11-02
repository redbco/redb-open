import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Transformation } from '@/lib/api/types';

export function useTransformations() {
  const [transformations, setTransformations] = useState<Transformation[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchTransformations = useCallback(async () => {
    // Check if token is available before making the request
    const token = typeof window !== 'undefined' ? localStorage.getItem('auth_token') : null;
    console.log('[useTransformations] About to fetch, token available:', !!token, 'length:', token?.length || 0);
    
    try {
      setIsLoading(true);
      console.log('[useTransformations] Fetching transformations...');
      const response = await api.transformations.list();
      console.log('[useTransformations] Received transformations:', response.transformations?.length || 0);
      setTransformations(response.transformations || []);
      setError(null);
    } catch (err) {
      console.error('[useTransformations] Error fetching transformations:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch transformations'));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTransformations();
  }, [fetchTransformations]);

  return {
    transformations,
    isLoading,
    error,
    refetch: fetchTransformations,
  };
}

export function useTransformation(transformationName: string) {
  const [transformation, setTransformation] = useState<Transformation | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchTransformation = useCallback(async () => {
    if (!transformationName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.transformations.show(transformationName);
      setTransformation(response.transformation);
      setError(null);
    } catch (err) {
      console.error('[useTransformation] Error fetching transformation:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch transformation'));
    } finally {
      setIsLoading(false);
    }
  }, [transformationName]);

  useEffect(() => {
    fetchTransformation();
  }, [fetchTransformation]);

  return {
    transformation,
    isLoading,
    error,
    refetch: fetchTransformation,
  };
}

