'use client';

import { useState, useEffect } from 'react';
import { useDatabases } from '@/lib/hooks/useDatabases';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Database, Plus, Server, Activity, RefreshCw, Layers, Clock, CheckCircle2, AlertCircle } from 'lucide-react';
import { DatabaseCard } from '@/components/databases/DatabaseCard';
import { ConnectDatabaseDialog } from '@/components/databases/ConnectDatabaseDialog';
import { formatDatabaseType } from '@/lib/formatters';

interface DatabasesPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function DatabasesPage({ params }: DatabasesPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [showConnectDialog, setShowConnectDialog] = useState(false);
  const { showToast } = useToast();

  // Initialize workspace ID from params
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const { databases, isLoading, error, refetch } = useDatabases(workspaceId);

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing databases...',
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
            <h2 className="text-3xl font-bold text-foreground">Databases</h2>
            <p className="text-muted-foreground mt-2">
              Manage your database connections
            </p>
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Databases</h3>
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
  const connectedDatabases = databases.filter(d => d.status?.toLowerCase() === 'healthy' || d.status?.toLowerCase() === 'connected').length;
  const disconnectedDatabases = databases.length - connectedDatabases;

  const uniqueInstances = new Set(databases.map(d => d.instance_id)).size;

  const uniqueTypes = Array.from(new Set(databases.map(d => d.database_type)));
  const techStackDisplay = uniqueTypes.slice(0, 2).map(t => formatDatabaseType(t)).join(', ') + (uniqueTypes.length > 2 ? '...' : '');

  const latestDatabase = [...databases].sort((a, b) => {
    return new Date(b.created || 0).getTime() - new Date(a.created || 0).getTime();
  })[0];

  const getTimeAgo = (dateString: string | undefined) => {
    if (!dateString) return 'Unknown';
    const date = new Date(dateString);
    const now = new Date();
    const diffInSeconds = Math.floor((now.getTime() - date.getTime()) / 1000);

    if (diffInSeconds < 60) return 'Just now';
    if (diffInSeconds < 3600) return `${Math.floor(diffInSeconds / 60)}m ago`;
    if (diffInSeconds < 86400) return `${Math.floor(diffInSeconds / 3600)}h ago`;
    return `${Math.floor(diffInSeconds / 86400)}d ago`;
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between bg-gradient-to-r from-blue-50/50 to-transparent dark:from-blue-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div>
          <div className="flex items-center gap-3">
            <Database className="w-8 h-8 text-blue-600 dark:text-blue-400" />
            <h2 className="text-3xl font-bold text-foreground">Databases</h2>
          </div>
          <div className="flex items-center gap-2 mt-2">
            <p className="text-muted-foreground">
              Manage database connections across instances
            </p>
            {!isLoading && databases.length > 0 && (
              <>
                <span className="text-muted-foreground/30">•</span>
                {disconnectedDatabases > 0 ? (
                  <div className="flex items-center gap-1.5 text-rose-600 dark:text-rose-400 text-sm font-medium animate-in fade-in duration-300">
                    <AlertCircle className="w-4 h-4" />
                    {disconnectedDatabases} disconnected
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5 text-emerald-600 dark:text-emerald-400 text-sm font-medium animate-in fade-in duration-300">
                    <CheckCircle2 className="w-4 h-4" />
                    All databases connected
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
            Connect Database
          </button>
        </div>
      </div>



      {/* Database List */}
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
      ) : databases.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Database className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No Databases Connected</h3>
          <p className="text-muted-foreground mb-6">
            Get started by connecting your first database
          </p>
          <button
            onClick={() => setShowConnectDialog(true)}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Connect Database
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {databases.map((database) => (
            <DatabaseCard
              key={database.database_id}
              database={database}
              workspaceId={workspaceId}
              onUpdate={refetch}
            />
          ))}
        </div>
      )}

      {/* Connect Database Dialog */}
      {showConnectDialog && (
        <ConnectDatabaseDialog
          workspaceId={workspaceId}
          onClose={() => setShowConnectDialog(false)}
          onSuccess={() => {
            setShowConnectDialog(false);
            refetch();
            showToast({
              type: 'success',
              title: 'Database Connected',
              message: 'Your database has been successfully connected',
            });
          }}
        />
      )}
    </div>
  );
}

