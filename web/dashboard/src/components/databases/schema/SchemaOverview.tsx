'use client';

import { Table, Columns, Shield, Activity, RefreshCw, GitBranch } from 'lucide-react';
import type { DatabaseSchema } from '@/lib/api/types';

interface SchemaOverviewProps {
  schema: DatabaseSchema;
  databaseName: string;
  onRefresh?: () => void;
  onDeploySchema?: () => void;
  isRefreshing?: boolean;
}

export function SchemaOverview({
  schema,
  databaseName,
  onRefresh,
  onDeploySchema,
  isRefreshing = false,
}: SchemaOverviewProps) {
  // Helper function to get containers or tables
  const getContainersOrTables = () => {
    if (schema.containers && schema.containers.length > 0) {
      return schema.containers.map(container => ({
        name: container.object_name,
        columns: container.items || [],
        object_type: container.object_type,
      }));
    }
    // Fallback to legacy tables
    return (schema.tables || []).map(table => ({
      name: table.name,
      columns: table.columns || [],
      object_type: 'table',
    }));
  };

  const containers = getContainersOrTables();

  // Calculate statistics
  const tableCount = containers.length;
  const columnCount = containers.reduce((acc, container) => {
    if (schema.containers && schema.containers.length > 0) {
      // New format: items
      return acc + (container.columns?.length || 0);
    }
    // Legacy format: columns
    return acc + (container.columns?.length || 0);
  }, 0);
  
  // Count privileged columns by confidence level (using enriched schema endpoint data)
  const privilegedColumnStats = containers.reduce(
    (acc, container) => {
      const columns = container.columns || [];
      columns.forEach((col: any) => {
        // Check for both new and legacy field names
        const isPrivileged = col.is_privileged || col.isPrivilegedData || col.is_privileged_data;
        const confidence = col.detection_confidence || col.privilegedConfidence || col.privileged_confidence || 0;
        
        if (isPrivileged) {
          acc.total++;
          if (confidence > 0.7) {
            acc.high++;
          } else if (confidence >= 0.4) {
            acc.medium++;
          } else if (confidence > 0) {
            acc.low++;
          }
        }
      });
      return acc;
    },
    { total: 0, high: 0, medium: 0, low: 0 }
  );

  // Count privileged columns (high confidence > 0.7)
  const privilegedColumnCount = privilegedColumnStats.high;

  // Count tables with privileged data
  const privilegedTableCount = containers.filter((container) => {
    const columns = container.columns || [];
    return columns.some((col: any) => {
      const isPrivileged = col.is_privileged || col.isPrivilegedData || col.is_privileged_data;
      const confidence = col.detection_confidence || col.privilegedConfidence || col.privileged_confidence || 0;
      return isPrivileged && confidence > 0.7;
    });
  }).length;
  
  // Count data categories from enriched schema data
  const dataCategoryCounts = containers.reduce((acc, container) => {
    const columns = container.columns || [];
    columns.forEach((col: any) => {
      const category = col.data_category || col.dataCategory;
      const isPrivileged = col.is_privileged || col.isPrivilegedData || col.is_privileged_data;
      if (category && isPrivileged) {
        acc[category] = (acc[category] || 0) + 1;
      }
    });
    return acc;
  }, {} as Record<string, number>);
  
  const topCategories = Object.entries(dataCategoryCounts)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 3);

  return (
    <div className="space-y-6">
      {/* Real-time Indicator & Actions */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* Live Badge */}
          <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400 border border-green-200 dark:border-green-800">
            <div className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
            </div>
            <span className="font-semibold text-sm">LIVE</span>
            <span className="text-sm">Real-time Schema</span>
          </div>

          <div className="text-sm text-muted-foreground">
            Last updated: {new Date().toLocaleTimeString()}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex items-center gap-2">
          {onRefresh && (
            <button
              onClick={onRefresh}
              disabled={isRefreshing}
              className="inline-flex items-center gap-2 px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          )}
          {onDeploySchema && (
            <button
              onClick={onDeploySchema}
              className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              <GitBranch className="h-4 w-4" />
              Deploy to Repository
            </button>
          )}
        </div>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Tables</p>
              <p className="text-3xl font-bold text-foreground mt-1">{tableCount}</p>
              <p className="text-xs text-muted-foreground mt-1">
                {privilegedTableCount} with privileged data
              </p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-blue-100 dark:bg-blue-900/20 flex items-center justify-center">
              <Table className="h-6 w-6 text-blue-600 dark:text-blue-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Columns</p>
              <p className="text-3xl font-bold text-foreground mt-1">{columnCount}</p>
              <p className="text-xs text-muted-foreground mt-1">Across all tables</p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-purple-100 dark:bg-purple-900/20 flex items-center justify-center">
              <Columns className="h-6 w-6 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Privileged Data</p>
              <p className="text-3xl font-bold text-foreground mt-1">{privilegedColumnCount}</p>
              <div className="flex items-center gap-2 mt-1 text-xs">
                <span className="text-red-600 dark:text-red-400" title="High confidence (>70%)">
                  High: {privilegedColumnStats.high}
                </span>
                {privilegedColumnStats.medium > 0 && (
                  <>
                    <span className="text-muted-foreground">•</span>
                    <span className="text-yellow-600 dark:text-yellow-400" title="Medium confidence (40-70%)">
                      Med: {privilegedColumnStats.medium}
                    </span>
                  </>
                )}
                {privilegedColumnStats.low > 0 && (
                  <>
                    <span className="text-muted-foreground">•</span>
                    <span className="text-gray-600 dark:text-gray-400" title="Low confidence (<40%)">
                      Low: {privilegedColumnStats.low}
                    </span>
                  </>
                )}
              </div>
            </div>
            <div className="w-12 h-12 rounded-lg bg-red-100 dark:bg-red-900/20 flex items-center justify-center">
              <Shield className="h-6 w-6 text-red-600 dark:text-red-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Schema Status</p>
              <p className="text-3xl font-bold text-green-600 dark:text-green-400 mt-1">Active</p>
              <p className="text-xs text-muted-foreground mt-1">Database connected</p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-green-100 dark:bg-green-900/20 flex items-center justify-center">
              <Activity className="h-6 w-6 text-green-600 dark:text-green-400" />
            </div>
          </div>
        </div>
      </div>

      {/* Info Banner */}
      <div className="bg-blue-50 dark:bg-blue-900/10 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0 w-5 h-5 rounded-full bg-blue-600 dark:bg-blue-400 flex items-center justify-center mt-0.5">
            <span className="text-white text-xs font-bold">i</span>
          </div>
          <div className="flex-1">
            <p className="text-sm text-blue-900 dark:text-blue-100 font-medium">
              Real-time Schema with Enhanced Privileged Data Detection
            </p>
            <p className="text-sm text-blue-800 dark:text-blue-200 mt-1">
              This view shows the current live state of the database <strong>{databaseName}</strong> with 
              enriched privileged data classifications from automatic detection. 
              {topCategories.length > 0 && (
                <>
                  {' '}Top detected categories:{' '}
                  {topCategories.map(([category, count], idx) => (
                    <span key={category}>
                      <strong>{category}</strong> ({count})
                      {idx < topCategories.length - 1 ? ', ' : ''}
                    </span>
                  ))}
                  .
                </>
              )}
              {' '}Any changes made directly to the database will be reflected here immediately upon refresh.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

