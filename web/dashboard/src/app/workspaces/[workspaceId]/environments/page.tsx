'use client';

import { useState, useEffect } from 'react';
import { useEnvironments } from '@/lib/hooks/useEnvironments';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Layers, Plus, RefreshCw } from 'lucide-react';
import { EnvironmentCard } from '@/components/environments/EnvironmentCard';
import { CreateEnvironmentDialog } from '@/components/environments/CreateEnvironmentDialog';

interface EnvironmentsPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function EnvironmentsPage({ params }: EnvironmentsPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const { showToast } = useToast();

  useEffect(() => {
    params.then(({ workspaceId }) => setWorkspaceId(workspaceId));
  }, [params]);

  const { environments, isLoading, error, refetch } = useEnvironments(workspaceId);

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing environments...',
    });
  };

  if (!workspaceId) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // Sort environments: production first, then by criticality
  const sortedEnvironments = [...environments].sort((a, b) => {
    if (a.environment_production && !b.environment_production) return -1;
    if (!a.environment_production && b.environment_production) return 1;
    return (b.environment_criticality || 0) - (a.environment_criticality || 0);
  });

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Environments</h2>
          <p className="text-muted-foreground mt-2">
            Manage deployment environments for instances and databases
          </p>
        </div>
        <div className="flex items-center space-x-3">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => setShowCreateDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Create Environment
          </button>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4">
          <p className="text-destructive text-sm">{error.message}</p>
        </div>
      )}

      {/* Environment List */}
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
      ) : sortedEnvironments.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Layers className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No Environments</h3>
          <p className="text-muted-foreground mb-6">
            Get started by creating your first environment
          </p>
          <button
            onClick={() => setShowCreateDialog(true)}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Create Environment
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {sortedEnvironments.map((environment) => (
            <EnvironmentCard
              key={environment.environment_id}
              environment={environment}
              workspaceId={workspaceId}
              onUpdate={refetch}
            />
          ))}
        </div>
      )}

      {/* Create Environment Dialog */}
      {showCreateDialog && (
        <CreateEnvironmentDialog
          workspaceId={workspaceId}
          onClose={() => setShowCreateDialog(false)}
          onSuccess={refetch}
        />
      )}
    </div>
  );
}

