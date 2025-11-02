'use client';

import { useMesh, useNodes } from '@/lib/hooks/useMesh';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Network, RefreshCw, Server } from 'lucide-react';
import { NodeCard } from '@/components/mesh/NodeCard';

export default function MeshPage() {
  const { showToast } = useToast();
  const { mesh, isLoading: meshLoading, error: meshError, refetch: refetchMesh } = useMesh();
  const { nodes, isLoading: nodesLoading, error: nodesError, refetch: refetchNodes } = useNodes();

  const isLoading = meshLoading || nodesLoading;
  const error = meshError || nodesError;

  const handleRefresh = () => {
    refetchMesh();
    refetchNodes();
    showToast({
      type: 'info',
      title: 'Refreshing mesh information...',
    });
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Mesh Network</h2>
          <p className="text-muted-foreground mt-2">
            View your distributed mesh topology and node status
          </p>
        </div>
        <button
          onClick={handleRefresh}
          className="inline-flex items-center px-4 py-2 bg-background border border-border text-foreground rounded-md hover:bg-accent transition-colors"
          disabled={isLoading}
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4">
          <p className="text-destructive text-sm">{error.message}</p>
        </div>
      )}

      {/* Mesh Info */}
      {mesh && (
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center space-x-3 mb-4">
            <Network className="h-6 w-6 text-primary" />
            <h3 className="text-xl font-semibold text-foreground">
              {mesh.mesh_name || 'Mesh Network'}
            </h3>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-sm text-muted-foreground mb-1">Mesh ID</p>
              <p className="text-lg font-semibold text-foreground font-mono">
                {mesh.mesh_id}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Total Nodes</p>
              <p className="text-lg font-semibold text-foreground">
                {mesh.node_count || nodes.length || 0}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Status</p>
              <p className="text-lg font-semibold text-foreground">
                {mesh.status || 'Active'}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Created</p>
              <p className="text-lg font-semibold text-foreground">
                {mesh.created
                  ? new Date(mesh.created).toLocaleDateString()
                  : 'N/A'}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Nodes */}
      <div>
        <h3 className="text-xl font-semibold text-foreground mb-4">Network Nodes</h3>
        
        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
                <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
                <div className="h-4 bg-muted rounded w-full mb-2"></div>
                <div className="h-4 bg-muted rounded w-2/3"></div>
              </div>
            ))}
          </div>
        ) : nodes.length === 0 ? (
          <div className="bg-card border border-border rounded-lg p-12 text-center">
            <Server className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-xl font-semibold text-foreground mb-2">No Nodes</h3>
            <p className="text-muted-foreground">
              No nodes found in the mesh network
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {nodes.map((node) => (
              <NodeCard key={node.node_id} node={node} />
            ))}
          </div>
        )}
      </div>

      {/* Mesh Topology Visualization Placeholder */}
      {nodes.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4">Network Topology</h3>
          <div className="bg-muted/30 border-2 border-dashed border-border rounded-lg p-12 text-center">
            <Network className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <p className="text-muted-foreground">
              Interactive topology visualization coming soon
            </p>
            <p className="text-sm text-muted-foreground mt-2">
              {nodes.length} nodes • {nodes.reduce((acc, node) => acc + (node.connections?.length || 0), 0)} connections
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

