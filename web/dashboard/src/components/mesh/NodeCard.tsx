'use client';

import { Node } from '@/lib/api/types';
import { Server, MapPin, Activity, Calendar, Network } from 'lucide-react';

interface NodeCardProps {
  node: Node;
}

export function NodeCard({ node }: NodeCardProps) {
  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    } catch {
      return dateString;
    }
  };

  const getStatusColor = (status?: string) => {
    switch (status?.toLowerCase()) {
      case 'healthy':
      case 'active':
        return 'bg-green-500';
      case 'unhealthy':
      case 'error':
        return 'bg-red-500';
      case 'unknown':
        return 'bg-gray-500';
      default:
        return 'bg-yellow-500';
    }
  };

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <Server className="h-6 w-6 text-primary" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-foreground flex items-center">
              {node.node_name || node.node_id}
              <span className={`ml-2 w-2 h-2 rounded-full ${getStatusColor(node.status)}`} />
            </h3>
            {node.node_name && (
              <p className="text-sm text-muted-foreground font-mono">{node.node_id}</p>
            )}
            {node.is_local && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 mt-1">
                Local Node
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Connection Info */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <MapPin className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-foreground font-mono">
            {node.node_address}:{node.node_port || 'N/A'}
          </span>
        </div>
        {node.node_region && (
          <div className="flex items-center text-sm">
            <MapPin className="h-4 w-4 mr-2 text-muted-foreground" />
            <span className="text-muted-foreground">
              Region: {node.node_region}
            </span>
          </div>
        )}
        {node.connections && node.connections.length > 0 && (
          <div className="flex items-center text-sm">
            <Network className="h-4 w-4 mr-2 text-muted-foreground" />
            <span className="text-muted-foreground">
              {node.connections.length} connection{node.connections.length !== 1 ? 's' : ''}
            </span>
          </div>
        )}
      </div>

      {/* Resource Counts */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center space-x-4 text-sm text-muted-foreground">
          <span>{node.instance_count || 0} instances</span>
          <span>{node.database_count || 0} databases</span>
        </div>
        <span
          className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
            node.status === 'healthy' || node.status === 'active'
              ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
              : node.status === 'unhealthy' || node.status === 'error'
              ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400'
              : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
          }`}
        >
          {node.status || 'Unknown'}
        </span>
      </div>
    </div>
  );
}

