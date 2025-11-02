import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Region } from '@/lib/api/types';

export function useRegions() {
  const [regions, setRegions] = useState<Region[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchRegions = useCallback(async () => {
    try {
      setIsLoading(true);
      const response = await api.regions.list();
      setRegions(response.regions || []);
      setError(null);
    } catch (err) {
      console.error('[useRegions] Error fetching regions:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch regions'));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRegions();
  }, [fetchRegions]);

  return {
    regions,
    isLoading,
    error,
    refetch: fetchRegions,
  };
}

export function useRegion(regionName: string) {
  const [region, setRegion] = useState<Region | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchRegion = useCallback(async () => {
    if (!regionName) {
      setIsLoading(false);
      return;
    }

    try {
      setIsLoading(true);
      const response = await api.regions.show(regionName);
      setRegion(response.region);
      setError(null);
    } catch (err) {
      console.error('[useRegion] Error fetching region:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch region'));
    } finally {
      setIsLoading(false);
    }
  }, [regionName]);

  useEffect(() => {
    fetchRegion();
  }, [fetchRegion]);

  return {
    region,
    isLoading,
    error,
    refetch: fetchRegion,
  };
}

