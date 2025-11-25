'use client';

import { useState, useEffect } from 'react';
import { useInstances } from '@/lib/hooks/useInstances';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Server, Plus, Database, Activity, RefreshCw, HardDrive, Cloud, Layers, CheckCircle2, AlertCircle } from 'lucide-react';
import { InstanceCard } from '@/components/instances/InstanceCard';
import { ConnectInstanceDialog } from '@/components/instances/ConnectInstanceDialog';
import { formatVendor, formatDatabaseType } from '@/lib/formatters';

interface InstancesPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function InstancesPage({ params }: InstancesPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [showConnectDialog, setShowConnectDialog] = useState(false);
  const { showToast } = useToast();

  // Initialize workspace ID from params
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const { instances, isLoading, error, refetch } = useInstances(workspaceId);

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing instances...',
    });
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
            <h2 className="text-3xl font-bold text-foreground">Instances</h2>
            <p className="text-muted-foreground mt-2">
              Manage your database instances
            </p>
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Instances</h3>
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
  const totalDatabases = instances.reduce((sum, inst) => sum + (inst.database_count || 0), 0);
  const connectedInstances = instances.filter(i => i.status === 'healthy' || i.status === 'connected').length;
  const unhealthyInstances = instances.length - connectedInstances;

  // Calculate Primary Provider (Mode)
  const vendors = instances.map(i => i.instance_vendor);
  const primaryProvider = vendors.sort((a, b) =>
    vendors.filter(v => v === a).length - vendors.filter(v => v === b).length
  ).pop();
  const primaryProviderCount = instances.filter(i => i.instance_vendor === primaryProvider).length;

  // Calculate Dominant Tech (Mode)
  const types = instances.map(i => i.instance_type);
  const dominantTech = types.sort((a, b) =>
    types.filter(v => v === a).length - types.filter(v => v === b).length
  ).pop();
  const dominantTechCount = instances.filter(i => i.instance_type === dominantTech).length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between bg-gradient-to-r from-purple-50/50 to-transparent dark:from-purple-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div>
          <div className="flex items-center gap-3">
            <Server className="w-8 h-8 text-purple-600 dark:text-purple-400" />
            <h2 className="text-3xl font-bold text-foreground">Instances</h2>
          </div>
          <div className="flex items-center gap-2 mt-2">
            <p className="text-muted-foreground">
              Manage database server instances
            </p>
            {!isLoading && instances.length > 0 && (
              <>
                <span className="text-muted-foreground/30">•</span>
                {unhealthyInstances > 0 ? (
                  <div className="flex items-center gap-1.5 text-rose-600 dark:text-rose-400 text-sm font-medium animate-in fade-in duration-300">
                    <AlertCircle className="w-4 h-4" />
                    {unhealthyInstances} require attention
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5 text-emerald-600 dark:text-emerald-400 text-sm font-medium animate-in fade-in duration-300">
                    <CheckCircle2 className="w-4 h-4" />
                    All systems operational
                  </div>
                )}
              </>
            )}
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => setShowConnectDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Connect Instance
          </button>
        </div>
      </div>



      {/* Instance List */}
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
      ) : instances.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Server className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No Instances Connected</h3>
          <p className="text-muted-foreground mb-6">
            Get started by connecting your first database instance
          </p>
          <button
            onClick={() => setShowConnectDialog(true)}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Connect Instance
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {instances.map((instance) => (
            <InstanceCard
              key={instance.instance_id}
              instance={instance}
              workspaceId={workspaceId}
              onUpdate={refetch}
            />
          ))}
        </div>
      )}

      {/* Connect Instance Dialog */}
      {showConnectDialog && (
        <ConnectInstanceDialog
          workspaceId={workspaceId}
          onClose={() => setShowConnectDialog(false)}
          onSuccess={() => {
            setShowConnectDialog(false);
            refetch();
            showToast({
              type: 'success',
              title: 'Instance Connected',
              message: 'Your database instance has been successfully connected',
            });
          }}
        />
      )}
    </div>
  );
}

