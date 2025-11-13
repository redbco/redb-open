import { useState, useEffect } from 'react';
import type { RelationshipMetrics } from '@/lib/api/types';
import { generateDummyMetrics } from '@/lib/utils/dummyMetrics';
// import { api } from '@/lib/api/endpoints'; // Uncomment when backend is ready

interface UseRelationshipMetricsResult {
  metrics: RelationshipMetrics | null;
  isLoading: boolean;
  error: Error | null;
  refetch: () => void;
}

/**
 * Hook to fetch relationship metrics
 * Currently returns dummy data with simulated loading delay
 * 
 * TODO: Once backend implements the metrics endpoint, replace the dummy data
 * with actual API call: api.relationships.getMetrics(workspaceId, relationshipName)
 * 
 * @param workspaceId - The workspace ID
 * @param relationshipName - The relationship name
 * @returns Metrics data, loading state, error state, and refetch function
 */
export function useRelationshipMetrics(
  workspaceId: string,
  relationshipName: string
): UseRelationshipMetricsResult {
  const [metrics, setMetrics] = useState<RelationshipMetrics | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<Error | null>(null);
  const [refetchTrigger, setRefetchTrigger] = useState(0);

  useEffect(() => {
    if (!workspaceId || !relationshipName) {
      return;
    }

    const fetchMetrics = async () => {
      setIsLoading(true);
      setError(null);

      try {
        // Simulate network delay
        await new Promise(resolve => setTimeout(resolve, 800));

        // TODO: Replace with actual API call when backend is ready:
        // const response = await api.relationships.getMetrics(workspaceId, relationshipName);
        // setMetrics(response.metrics);

        // For now, use dummy data
        const dummyData = generateDummyMetrics(relationshipName);
        setMetrics(dummyData);
      } catch (err) {
        setError(err instanceof Error ? err : new Error('Failed to fetch metrics'));
        setMetrics(null);
      } finally {
        setIsLoading(false);
      }
    };

    fetchMetrics();
  }, [workspaceId, relationshipName, refetchTrigger]);

  const refetch = () => {
    setRefetchTrigger(prev => prev + 1);
  };

  return {
    metrics,
    isLoading,
    error,
    refetch,
  };
}

