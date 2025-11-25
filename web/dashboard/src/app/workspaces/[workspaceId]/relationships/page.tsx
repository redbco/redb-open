'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { useRelationships } from '@/lib/hooks/useRelationships';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Link as LucideLink, Plus, RefreshCw, Play, Pause, CheckCircle, AlertCircle } from 'lucide-react';
import { CreateRelationshipDialog } from '@/components/relationships/CreateRelationshipDialog';
import { api } from '@/lib/api/endpoints';

interface RelationshipsPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function RelationshipsPage({ params }: RelationshipsPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [operationInProgress, setOperationInProgress] = useState<string | null>(null);
  const { showToast } = useToast();
  
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const { relationships, isLoading, error, refetch } = useRelationships(workspaceId);

  const handleStartRelationship = async (relationshipName: string) => {
    setOperationInProgress(relationshipName);
    try {
      await api.relationships.start(workspaceId, relationshipName, {
        batch_size: 1000,
        parallel_workers: 4
      });
      
      showToast({
        type: 'success',
        title: 'Relationship Started',
        message: `Successfully started relationship '${relationshipName}'.`
      });
      
      // Refetch relationships to update status
      refetch();
    } catch (error: unknown) {
      showToast({
        type: 'error',
        title: 'Failed to Start Relationship',
        message: error instanceof Error ? error.message : 'An error occurred while starting the relationship.'
      });
    } finally {
      setOperationInProgress(null);
    }
  };

  const handleStopRelationship = async (relationshipName: string) => {
    setOperationInProgress(relationshipName);
    try {
      await api.relationships.stop(workspaceId, relationshipName);
      
      showToast({
        type: 'success',
        title: 'Relationship Stopped',
        message: `Successfully stopped relationship '${relationshipName}'.`
      });
      
      // Refetch relationships to update status
      refetch();
    } catch (error: unknown) {
      showToast({
        type: 'error',
        title: 'Failed to Stop Relationship',
        message: error instanceof Error ? error.message : 'An error occurred while stopping the relationship.'
      });
    } finally {
      setOperationInProgress(null);
    }
  };

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
  const pausedRelationships = relationships.length - activeRelationships;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between bg-gradient-to-r from-emerald-50/50 to-transparent dark:from-emerald-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div>
          <div className="flex items-center gap-3">
            <LucideLink className="w-8 h-8 text-emerald-600 dark:text-emerald-400" />
            <h2 className="text-3xl font-bold text-foreground">Relationships</h2>
          </div>
          <div className="flex items-center gap-2 mt-2">
            <p className="text-muted-foreground">
              Manage CDC-based data replication and migration relationships
            </p>
            {!isLoading && relationships.length > 0 && (
              <>
                <span className="text-muted-foreground/30">•</span>
                {pausedRelationships > 0 ? (
                  <div className="flex items-center gap-1.5 text-rose-600 dark:text-rose-400 text-sm font-medium animate-in fade-in duration-300">
                    <AlertCircle className="w-4 h-4" />
                    {pausedRelationships} paused
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5 text-emerald-600 dark:text-emerald-400 text-sm font-medium animate-in fade-in duration-300">
                    <CheckCircle className="w-4 h-4" />
                    All relationships active
                  </div>
                )}
              </>
            )}
          </div>
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
            onClick={() => setIsCreateDialogOpen(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Create Relationship
          </button>
        </div>
      </div>

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
            onClick={() => setIsCreateDialogOpen(true)}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Create Relationship
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4">
          {relationships.map((relationship) => (
            <div key={relationship.relationship_id} className="group bg-card border border-border rounded-xl p-6 hover:shadow-lg hover:border-primary/20 transition-all duration-200">
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center space-x-4 mb-3">
                    <div className="w-12 h-12 bg-gradient-to-br from-emerald-50 to-emerald-100 dark:from-emerald-900/20 dark:to-emerald-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
                      <LucideLink className="h-6 w-6 text-emerald-600 dark:text-emerald-400" />
                    </div>
                    <div className="flex-1">
                      <div className="flex items-center space-x-3 mb-2">
                        <Link 
                          href={`/workspaces/${workspaceId}/relationships/${encodeURIComponent(relationship.relationship_name)}`}
                          className="text-lg font-semibold text-foreground hover:text-primary transition-colors"
                        >
                          {relationship.relationship_name}
                        </Link>
                        <div className="flex items-center gap-2">
                          <div className="relative flex h-2.5 w-2.5">
                            <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${
                              relationship.status === 'active' ? 'bg-emerald-500' : 'bg-slate-500'
                            }`}></span>
                            <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${
                              relationship.status === 'active' ? 'bg-emerald-500' : 'bg-slate-500'
                            }`}></span>
                          </div>
                          <span className="text-xs font-medium text-muted-foreground capitalize">{relationship.status}</span>
                        </div>
                      </div>
                      {relationship.relationship_description && (
                        <p className="text-sm text-muted-foreground mb-3">{relationship.relationship_description}</p>
                      )}
                      <div className="flex items-center gap-4 text-sm">
                        <div>
                          <span className="text-muted-foreground">Type:</span>
                          <span className="ml-2 text-foreground capitalize">{relationship.relationship_type}</span>
                        </div>
                        <div className="flex items-center gap-2 text-foreground">
                          <span className="font-medium">
                            {relationship.relationship_source_database_name}.{relationship.relationship_source_table_name} ({relationship.relationship_source_database_type})
                          </span>
                          <span className="text-muted-foreground">→</span>
                          <Link 
                            href={`/workspaces/${workspaceId}/mappings/${relationship.mapping_name}`}
                            className="text-primary hover:text-primary/80 font-mono text-xs underline underline-offset-2 transition-colors"
                          >
                            {relationship.mapping_name}
                          </Link>
                          <span className="text-muted-foreground">→</span>
                          <span className="font-medium">
                            {relationship.relationship_target_database_name}.{relationship.relationship_target_table_name} ({relationship.relationship_target_database_type})
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
                <div className="flex items-center space-x-2 ml-4">
                  {relationship.status === 'active' ? (
                    <button
                      onClick={() => handleStopRelationship(relationship.relationship_name)}
                      disabled={operationInProgress === relationship.relationship_name}
                      className="px-3 py-1.5 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <Pause className="h-4 w-4 mr-1" />
                      {operationInProgress === relationship.relationship_name ? 'Stopping...' : 'Stop'}
                    </button>
                  ) : (
                    <button
                      onClick={() => handleStartRelationship(relationship.relationship_name)}
                      disabled={operationInProgress === relationship.relationship_name}
                      className="px-3 py-1.5 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <Play className="h-4 w-4 mr-1" />
                      {operationInProgress === relationship.relationship_name ? 'Starting...' : 'Start'}
                    </button>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Create Relationship Dialog */}
      <CreateRelationshipDialog
        isOpen={isCreateDialogOpen}
        onClose={() => setIsCreateDialogOpen(false)}
        workspaceName={workspaceId}
        onSuccess={() => {
          refetch();
        }}
      />
    </div>
  );
}
