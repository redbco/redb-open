'use client';

import { useState, useEffect } from 'react';
import { useMappings } from '@/lib/hooks/useMappings';
import { useTransformations } from '@/lib/hooks/useTransformations';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { ArrowRightLeft, Plus, RefreshCw, Table, Database, Activity, CheckCircle, Sparkles } from 'lucide-react';
import { MappingCard } from '@/components/mappings/MappingCard';
import { CreateMappingDialog } from '@/components/mappings/CreateMappingDialog';
import { TransformationsList } from '@/components/mappings/TransformationsList';

interface MappingsPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

type Tab = 'mappings' | 'transformations';

export default function MappingsPage({ params }: MappingsPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [activeTab, setActiveTab] = useState<Tab>('mappings');
  const { showToast } = useToast();
  
  // Initialize workspace ID from params
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const { mappings, isLoading: mappingsLoading, error: mappingsError, refetch: refetchMappings } = useMappings(workspaceId);
  const { transformations, isLoading: transformationsLoading, error: transformationsError, refetch: refetchTransformations } = useTransformations();

  if (!workspaceId) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  const handleRefresh = () => {
    if (activeTab === 'mappings') {
      refetchMappings();
      showToast({
        type: 'info',
        title: 'Refreshing',
        message: 'Refreshing mappings...',
      });
    } else {
      refetchTransformations();
      showToast({
        type: 'info',
        title: 'Refreshing',
        message: 'Refreshing transformations...',
      });
    }
  };

  const error = activeTab === 'mappings' ? mappingsError : transformationsError;
  const isLoading = activeTab === 'mappings' ? mappingsLoading : transformationsLoading;

  // Calculate metrics
  const validatedMappings = mappings.filter(m => m.validated).length;
  const tableMappings = mappings.filter(m => m.mapping_type === 'table').length;
  const databaseMappings = mappings.filter(m => m.mapping_type === 'database').length;
  const totalRules = mappings.reduce((sum, m) => sum + (m.mapping_rule_count || 0), 0);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Mappings</h2>
          <p className="text-muted-foreground mt-2">
            Define schema and column mappings for data migration and replication
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
          {activeTab === 'mappings' && (
            <button
              onClick={() => setShowCreateDialog(true)}
              className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              <Plus className="h-4 w-4 mr-2" />
              Create Mapping
            </button>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-border">
        <div className="flex space-x-8">
          <button
            onClick={() => setActiveTab('mappings')}
            className={`pb-4 px-1 border-b-2 font-medium text-sm transition-colors ${
              activeTab === 'mappings'
                ? 'border-primary text-primary'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-border'
            }`}
          >
            <div className="flex items-center space-x-2">
              <ArrowRightLeft className="h-4 w-4" />
              <span>Mappings</span>
              <span className="px-2 py-0.5 rounded-full text-xs bg-muted">
                {mappings.length}
              </span>
            </div>
          </button>
          <button
            onClick={() => setActiveTab('transformations')}
            className={`pb-4 px-1 border-b-2 font-medium text-sm transition-colors ${
              activeTab === 'transformations'
                ? 'border-primary text-primary'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-border'
            }`}
          >
            <div className="flex items-center space-x-2">
              <Sparkles className="h-4 w-4" />
              <span>Transformations</span>
              <span className="px-2 py-0.5 rounded-full text-xs bg-muted">
                {transformations.length}
              </span>
            </div>
          </button>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">
            Failed to Load {activeTab === 'mappings' ? 'Mappings' : 'Transformations'}
          </h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <button
            onClick={handleRefresh}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Retry
          </button>
        </div>
      )}

      {/* Mappings Tab Content */}
      {activeTab === 'mappings' && !error && (
        <>
          {/* Overview Metrics */}
          {!mappingsLoading && mappings.length > 0 && (
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
              {[
                {
                  title: 'Total Mappings',
                  value: mappings.length.toString(),
                  change: `${tableMappings} table, ${databaseMappings} database`,
                  icon: ArrowRightLeft,
                  color: 'text-blue-600 dark:text-blue-400'
                },
                {
                  title: 'Mapping Rules',
                  value: totalRules.toString(),
                  change: 'Total rules defined',
                  icon: Activity,
                  color: 'text-purple-600 dark:text-purple-400'
                },
                {
                  title: 'Table Mappings',
                  value: tableMappings.toString(),
                  change: 'Table-level mappings',
                  icon: Table,
                  color: 'text-green-600 dark:text-green-400'
                },
                {
                  title: 'Validated',
                  value: validatedMappings.toString(),
                  change: 'Ready for use',
                  icon: CheckCircle,
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

          {/* Mapping List */}
          {mappingsLoading ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {[...Array(3)].map((_, i) => (
                <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
                  <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
                  <div className="h-4 bg-muted rounded w-full mb-2"></div>
                  <div className="h-4 bg-muted rounded w-2/3"></div>
                </div>
              ))}
            </div>
          ) : mappings.length === 0 ? (
            <div className="bg-card border border-border rounded-lg p-12 text-center">
              <ArrowRightLeft className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
              <h3 className="text-2xl font-semibold text-foreground mb-2">No Mappings Created</h3>
              <p className="text-muted-foreground mb-6">
                Create your first mapping to start migrating or replicating data
              </p>
              <button
                onClick={() => setShowCreateDialog(true)}
                className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              >
                <Plus className="h-5 w-5 mr-2" />
                Create Mapping
              </button>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {mappings.map((mapping) => (
                <MappingCard
                  key={mapping.mapping_id}
                  mapping={mapping}
                  workspaceId={workspaceId}
                  onUpdate={refetchMappings}
                />
              ))}
            </div>
          )}
        </>
      )}

      {/* Transformations Tab Content */}
      {activeTab === 'transformations' && !error && (
        <TransformationsList
          transformations={transformations}
          isLoading={transformationsLoading}
        />
      )}

      {/* Create Mapping Dialog */}
      {showCreateDialog && (
        <CreateMappingDialog
          workspaceId={workspaceId}
          onClose={() => setShowCreateDialog(false)}
          onSuccess={() => {
            setShowCreateDialog(false);
            refetchMappings();
            showToast({
              type: 'success',
              title: 'Mapping Created',
              message: 'Your mapping has been successfully created',
            });
          }}
        />
      )}
    </div>
  );
}
