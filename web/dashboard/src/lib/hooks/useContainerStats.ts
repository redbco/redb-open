import { useState, useEffect } from 'react';
import { api } from '../api/endpoints';
import type { ResourceType, ContainerStatEntry } from '@/lib/api/types';

interface ContainerTypeStats {
  [key: string]: number; // Mapped resource type -> count
}

// Map backend container types to frontend resource types
function mapContainerTypeToResourceType(containerType: string): ResourceType | null {
  const mapping: Record<string, ResourceType> = {
    'table': 'tabular-record-set',
    'collection': 'document',
    'node': 'graph-node',
    'relationship': 'graph-relationship',
    'embedding': 'vector',
    'key-value-pair': 'keyvalue-item',
    'search-document': 'search-document',
    'time-series-point': 'timeseries-point',
    'blob': 'blob-object',
    'topic': 'stream',
    'webhook': 'webhook',
  };

  return mapping[containerType] || null;
}

export function useContainerStats(workspaceName: string) {
  const [stats, setStats] = useState<ContainerTypeStats>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!workspaceName) {
      setLoading(false);
      return;
    }

    const fetchStats = async () => {
      try {
        setLoading(true);
        setError(null);

        const data = await api.resources.getContainerStats(workspaceName);

        if (!data.success) {
          throw new Error(data.message || 'Failed to fetch container stats');
        }

        // Aggregate counts by resource type
        const typeStats: ContainerTypeStats = {};
        
        data.containers.forEach((container: ContainerStatEntry) => {
          const resourceType = mapContainerTypeToResourceType(container.container_type);
          if (resourceType) {
            typeStats[resourceType] = (typeStats[resourceType] || 0) + container.item_count;
          }
        });

        setStats(typeStats);
      } catch (err) {
        console.error('Error fetching container stats:', err);
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
  }, [workspaceName]);

  return { stats, loading, error };
}

