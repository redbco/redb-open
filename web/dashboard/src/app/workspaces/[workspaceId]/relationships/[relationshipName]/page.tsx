'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { 
  ArrowLeft, 
  Play, 
  Pause, 
  Edit, 
  Trash2, 
  RefreshCw,
  ArrowRight,
  Clock,
  BarChart3,
} from 'lucide-react';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { RelationshipMetricsOverview } from '@/components/relationships/RelationshipMetricsOverview';
import { RelationshipMetricsCharts } from '@/components/relationships/RelationshipMetricsCharts';
import { UpdateRelationshipDialog } from '@/components/relationships/UpdateRelationshipDialog';
import { useRelationships } from '@/lib/hooks/useRelationships';
import { useRelationshipMetrics } from '@/lib/hooks/useRelationshipMetrics';
import { api } from '@/lib/api/endpoints';
import { formatNumber, formatBytes } from '@/lib/utils/dummyMetrics';

interface RelationshipDetailPageProps {
  params: Promise<{
    workspaceId: string;
    relationshipName: string;
  }>;
}

export default function RelationshipDetailPage({ params }: RelationshipDetailPageProps) {
  const router = useRouter();
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [relationshipName, setRelationshipName] = useState<string>('');
  const [isUpdateDialogOpen, setIsUpdateDialogOpen] = useState(false);
  const [operationInProgress, setOperationInProgress] = useState<string | null>(null);
  const { showToast } = useToast();

  useEffect(() => {
    params.then(({ workspaceId: wsId, relationshipName: relName }) => {
      setWorkspaceId(wsId);
      setRelationshipName(decodeURIComponent(relName));
    });
  }, [params]);

  const { relationships, isLoading: relationshipsLoading, refetch: refetchRelationships } = useRelationships(workspaceId);
  const { metrics, isLoading: metricsLoading, refetch: refetchMetrics } = useRelationshipMetrics(workspaceId, relationshipName);

  const relationship = relationships.find(r => r.relationship_name === relationshipName);

  const handleStartRelationship = async () => {
    if (!relationship) return;
    
    setOperationInProgress('start');
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
      
      refetchRelationships();
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

  const handleStopRelationship = async () => {
    if (!relationship) return;
    
    setOperationInProgress('stop');
    try {
      await api.relationships.stop(workspaceId, relationshipName);
      
      showToast({
        type: 'success',
        title: 'Relationship Stopped',
        message: `Successfully stopped relationship '${relationshipName}'.`
      });
      
      refetchRelationships();
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

  const handleRemoveRelationship = async () => {
    if (!relationship) return;
    
    if (!confirm(
      `Are you sure you want to remove relationship "${relationshipName}"?\n\n` +
      `This will:\n` +
      `• Stop CDC replication immediately\n` +
      `• Delete the relationship configuration\n` +
      `• Keep existing data in the target database\n\n` +
      `This action cannot be undone.`
    )) {
      return;
    }

    setOperationInProgress('remove');
    try {
      await api.relationships.remove(workspaceId, relationshipName);
      
      showToast({
        type: 'success',
        title: 'Relationship Removed',
        message: `Successfully removed relationship '${relationshipName}'.`
      });
      
      // Navigate back to relationships list
      router.push(`/workspaces/${workspaceId}/relationships`);
    } catch (error: unknown) {
      showToast({
        type: 'error',
        title: 'Failed to Remove Relationship',
        message: error instanceof Error ? error.message : 'An error occurred while removing the relationship.'
      });
      setOperationInProgress(null);
    }
  };

  const handleUpdateRelationship = async (description: string, batchSize: number, parallelWorkers: number) => {
    if (!relationship) return;
    
    try {
      await api.relationships.modify(workspaceId, relationshipName, {
        relationship_description: description,
        // Note: batch_size and parallel_workers would be added to ModifyRelationshipRequest
        // when the backend supports them
      });
      
      showToast({
        type: 'success',
        title: 'Relationship Updated',
        message: `Successfully updated relationship '${relationshipName}'.`
      });
      
      refetchRelationships();
    } catch (error: unknown) {
      throw error;
    }
  };

  const handleRefresh = () => {
    refetchRelationships();
    refetchMetrics();
    showToast({
      type: 'info',
      title: 'Refreshing',
      message: 'Refreshing relationship data...',
    });
  };

  if (!workspaceId || !relationshipName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (relationshipsLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (!relationship) {
    return (
      <div className="space-y-6">
        <div className="flex items-center">
          <Link
            href={`/workspaces/${workspaceId}/relationships`}
            className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Relationships
          </Link>
        </div>
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <h3 className="text-xl font-semibold text-foreground mb-2">Relationship Not Found</h3>
          <p className="text-muted-foreground mb-4">
            The relationship "{relationshipName}" could not be found in workspace "{workspaceId}".
          </p>
          <Link
            href={`/workspaces/${workspaceId}/relationships`}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            View All Relationships
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          href={`/workspaces/${workspaceId}/relationships`}
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground transition-colors mb-4"
        >
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back to Relationships
        </Link>
        
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-2">
              <h2 className="text-3xl font-bold text-foreground">{relationship.relationship_name}</h2>
              <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${
                relationship.status === 'active'
                  ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                  : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
              }`}>
                {relationship.status}
              </span>
            </div>
            {relationship.relationship_description && (
              <p className="text-muted-foreground mb-4">{relationship.relationship_description}</p>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex items-center gap-2">
            <button
              onClick={handleRefresh}
              className="px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors inline-flex items-center"
              disabled={!!operationInProgress}
            >
              <RefreshCw className={`h-4 w-4 ${metricsLoading ? 'animate-spin' : ''}`} />
            </button>
            
            {relationship.status === 'active' ? (
              <button
                onClick={handleStopRelationship}
                disabled={!!operationInProgress}
                className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Pause className="h-4 w-4 mr-2" />
                {operationInProgress === 'stop' ? 'Stopping...' : 'Stop'}
              </button>
            ) : (
              <button
                onClick={handleStartRelationship}
                disabled={!!operationInProgress}
                className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Play className="h-4 w-4 mr-2" />
                {operationInProgress === 'start' ? 'Starting...' : 'Start'}
              </button>
            )}
            
            <button
              onClick={() => setIsUpdateDialogOpen(true)}
              disabled={!!operationInProgress}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Edit className="h-4 w-4 mr-2" />
              Edit
            </button>
            
            <button
              onClick={handleRemoveRelationship}
              disabled={!!operationInProgress}
              className="px-4 py-2 border border-destructive text-destructive rounded-md hover:bg-destructive hover:text-destructive-foreground transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Trash2 className="h-4 w-4 mr-2" />
              {operationInProgress === 'remove' ? 'Removing...' : 'Remove'}
            </button>
          </div>
        </div>
      </div>

      {/* Relationship Flow */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Data Flow</h3>
        <div className="flex items-center gap-4 flex-wrap">
          <div className="flex-1 min-w-[200px]">
            <p className="text-xs text-muted-foreground mb-1">Source</p>
            <div className="bg-muted/50 rounded-lg p-3">
              <p className="text-sm font-medium text-foreground">
                {relationship.relationship_source_database_name}
              </p>
              <p className="text-xs text-muted-foreground">
                {relationship.relationship_source_table_name}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {relationship.relationship_source_database_type}
              </p>
            </div>
          </div>

          <ArrowRight className="h-6 w-6 text-muted-foreground flex-shrink-0" />

          <div className="flex-1 min-w-[200px]">
            <p className="text-xs text-muted-foreground mb-1">Mapping</p>
            <Link
              href={`/workspaces/${workspaceId}/mappings/${relationship.mapping_name}`}
              className="block bg-primary/10 rounded-lg p-3 hover:bg-primary/20 transition-colors"
            >
              <p className="text-sm font-medium text-primary font-mono">
                {relationship.mapping_name}
              </p>
              <p className="text-xs text-muted-foreground">
                View mapping details →
              </p>
            </Link>
          </div>

          <ArrowRight className="h-6 w-6 text-muted-foreground flex-shrink-0" />

          <div className="flex-1 min-w-[200px]">
            <p className="text-xs text-muted-foreground mb-1">Target</p>
            <div className="bg-muted/50 rounded-lg p-3">
              <p className="text-sm font-medium text-foreground">
                {relationship.relationship_target_database_name}
              </p>
              <p className="text-xs text-muted-foreground">
                {relationship.relationship_target_table_name}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {relationship.relationship_target_database_type}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Metrics Overview */}
      {metricsLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
              <div className="h-12 bg-muted rounded w-12 mb-4"></div>
              <div className="h-4 bg-muted rounded w-24 mb-2"></div>
              <div className="h-8 bg-muted rounded w-20 mb-2"></div>
              <div className="h-3 bg-muted rounded w-32"></div>
            </div>
          ))}
        </div>
      ) : metrics ? (
        <>
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-xl font-semibold text-foreground">Current Metrics</h3>
              <div className="flex items-center text-sm text-muted-foreground">
                <Clock className="h-4 w-4 mr-2" />
                Last updated: {new Date(metrics.last_sync_timestamp).toLocaleString()}
              </div>
            </div>
            <RelationshipMetricsOverview metrics={metrics} />
          </div>

          {/* Charts */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-xl font-semibold text-foreground">Performance Trends (24 Hours)</h3>
              <div className="flex items-center text-sm text-muted-foreground">
                <BarChart3 className="h-4 w-4 mr-2" />
                288 data points (5-minute intervals)
              </div>
            </div>
            <RelationshipMetricsCharts metrics={metrics} />
          </div>

          {/* Aggregate Stats */}
          <div className="bg-card border border-border rounded-lg p-6">
            <h3 className="text-lg font-semibold text-foreground mb-4">Aggregate Statistics</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <div>
                <p className="text-sm text-muted-foreground mb-1">Total Records Replicated</p>
                <p className="text-2xl font-bold text-foreground">{formatNumber(metrics.total_records_replicated)}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-1">Total Data Transferred</p>
                <p className="text-2xl font-bold text-foreground">{formatBytes(metrics.total_bytes_transferred)}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-1">Total Errors</p>
                <p className="text-2xl font-bold text-foreground">{formatNumber(metrics.total_errors)}</p>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-1">Uptime</p>
                <p className="text-2xl font-bold text-foreground">{metrics.uptime_percentage.toFixed(2)}%</p>
              </div>
            </div>
          </div>
        </>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <p className="text-muted-foreground">No metrics available</p>
        </div>
      )}

      {/* Configuration */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Configuration</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div>
            <p className="text-sm text-muted-foreground mb-1">Relationship Type</p>
            <p className="text-base font-medium text-foreground capitalize">{relationship.relationship_type}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground mb-1">Batch Size</p>
            <p className="text-base font-medium text-foreground">1,000 records</p>
            <p className="text-xs text-muted-foreground mt-1">Default configuration</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground mb-1">Parallel Workers</p>
            <p className="text-base font-medium text-foreground">4 workers</p>
            <p className="text-xs text-muted-foreground mt-1">Default configuration</p>
          </div>
        </div>
      </div>

      {/* Update Dialog */}
      {relationship && (
        <UpdateRelationshipDialog
          isOpen={isUpdateDialogOpen}
          onClose={() => setIsUpdateDialogOpen(false)}
          relationship={relationship}
          onUpdate={handleUpdateRelationship}
        />
      )}
    </div>
  );
}

