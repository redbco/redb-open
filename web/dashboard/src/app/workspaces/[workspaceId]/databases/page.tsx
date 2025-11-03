'use client';

import { useState, useEffect } from 'react';
import { useDatabases } from '@/lib/hooks/useDatabases';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Database, Plus, Server, Table, Activity, RefreshCw } from 'lucide-react';
import { DatabaseCard } from '@/components/databases/DatabaseCard';
import { ConnectDatabaseDialog } from '@/components/databases/ConnectDatabaseDialog';

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

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Databases</h2>
          <p className="text-muted-foreground mt-2">
            Manage database connections across instances
          </p>
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

      {/* Overview Metrics */}
      {!isLoading && databases.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          {[
            {
              title: 'Total Databases',
              value: databases.length.toString(),
              change: `Across ${new Set(databases.map(d => d.instance_id)).size} instances`,
              icon: Database,
              color: 'text-blue-600 dark:text-blue-400'
            },
            {
              title: 'Connected',
              value: databases.filter(d => d.status?.toLowerCase() === 'healthy' || d.status?.toLowerCase() === 'connected').length.toString(),
              change: 'Active connections',
              icon: Server,
              color: 'text-green-600 dark:text-green-400'
            },
            {
              title: 'Total Tables',
              value: '-',
              change: 'Across all databases',
              icon: Table,
              color: 'text-purple-600 dark:text-purple-400'
            },
            {
              title: 'Active',
              value: databases.filter(d => d.database_enabled).length.toString(),
              change: 'Enabled databases',
              icon: Activity,
              color: 'text-orange-600 dark:text-orange-400'
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

