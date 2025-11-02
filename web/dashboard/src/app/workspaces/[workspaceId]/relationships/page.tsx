'use client';

import { useState, useEffect } from 'react';
import { useRelationships } from '@/lib/hooks/useRelationships';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Link as LucideLink, Plus, RefreshCw, Play, Pause, Activity, CheckCircle } from 'lucide-react';

interface RelationshipsPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function RelationshipsPage({ params }: RelationshipsPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const { showToast } = useToast();
  
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const { relationships, isLoading, error, refetch } = useRelationships(workspaceId);

  if (!workspaceId) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold text-foreground">Relationships</h2>
            <p className="text-muted-foreground mt-2">
              Data replication and migration relationships
            </p>
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Relationships</h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <button
            onClick={() => refetch()}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Calculate metrics
  const activeRelationships = relationships.filter(r => r.status === 'active').length;
  const replicationTypes = relationships.filter(r => r.relationship_type === 'replication').length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Relationships</h2>
          <p className="text-muted-foreground mt-2">
            Manage CDC-based data replication and migration relationships
          </p>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={() => refetch()}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => showToast({ type: 'info', title: 'Coming Soon', message: 'Create relationship dialog will be available soon' })}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Create Relationship
          </button>
        </div>
      </div>

      {/* Overview Metrics */}
      {!isLoading && relationships.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          {[
            {
              title: 'Total Relationships',
              value: relationships.length.toString(),
              change: `${replicationTypes} replication`,
              icon: LucideLink,
              color: 'text-blue-600 dark:text-blue-400'
            },
            {
              title: 'Active',
              value: activeRelationships.toString(),
              change: 'Currently running',
              icon: Play,
              color: 'text-green-600 dark:text-green-400'
            },
            {
              title: 'Paused',
              value: (relationships.length - activeRelationships).toString(),
              change: 'Stopped relationships',
              icon: Pause,
              color: 'text-yellow-600 dark:text-yellow-400'
            },
            {
              title: 'Healthy',
              value: activeRelationships.toString(),
              change: 'No issues detected',
              icon: CheckCircle,
              color: 'text-green-600 dark:text-green-400'
            }
          ].map((metric, index) => (
            <div key={index} className="bg-card border border-border rounded-lg p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-muted-foreground">{metric.title}</p>
                  <p className="text-2xl font-bold text-foreground mt-1">{metric.value}</p>
                  <p className="text-sm text-muted-foreground mt-1">{metric.change}</p>
                </div>
                <div className={`w-12 h-12 rounded-lg bg-muted/50 flex items-center justify-center ${metric.color}`}>
                  <metric.icon className="h-6 w-6" />
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Relationship List */}
      {isLoading ? (
        <div className="grid grid-cols-1 gap-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
              <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
              <div className="h-4 bg-muted rounded w-full mb-2"></div>
              <div className="h-4 bg-muted rounded w-2/3"></div>
            </div>
          ))}
        </div>
      ) : relationships.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <LucideLink className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No Relationships Created</h3>
          <p className="text-muted-foreground mb-6">
            Create your first relationship to start replicating data between databases
          </p>
          <button
            onClick={() => showToast({ type: 'info', title: 'Coming Soon', message: 'Create relationship dialog will be available soon' })}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Create Relationship
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4">
          {relationships.map((relationship) => (
            <div key={relationship.relationship_id} className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all">
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center space-x-3 mb-2">
                    <h3 className="text-lg font-semibold text-foreground">{relationship.relationship_name}</h3>
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      relationship.status === 'active'
                        ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                        : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
                    }`}>
                      {relationship.status}
                    </span>
                  </div>
                  {relationship.relationship_description && (
                    <p className="text-sm text-muted-foreground mb-3">{relationship.relationship_description}</p>
                  )}
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <span className="text-muted-foreground">Type:</span>
                      <span className="ml-2 text-foreground capitalize">{relationship.relationship_type}</span>
                    </div>
                    <div>
                      <span className="text-muted-foreground">Mapping:</span>
                      <span className="ml-2 text-foreground font-mono text-xs">{relationship.mapping_id}</span>
                    </div>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  {relationship.status === 'active' ? (
                    <button
                      onClick={() => showToast({ type: 'info', title: 'Coming Soon', message: 'Stop functionality will be available soon' })}
                      className="px-3 py-1.5 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors inline-flex items-center"
                    >
                      <Pause className="h-4 w-4 mr-1" />
                      Stop
                    </button>
                  ) : (
                    <button
                      onClick={() => showToast({ type: 'info', title: 'Coming Soon', message: 'Start functionality will be available soon' })}
                      className="px-3 py-1.5 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
                    >
                      <Play className="h-4 w-4 mr-1" />
                      Start
                    </button>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

