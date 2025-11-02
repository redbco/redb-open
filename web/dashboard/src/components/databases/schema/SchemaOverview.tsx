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
  // Calculate statistics
  const tableCount = schema.tables?.length || 0;
  const columnCount = schema.tables?.reduce((acc, table) => acc + table.columns.length, 0) || 0;
  
  // Count privileged columns (high confidence > 0.7)
  const privilegedColumnCount =
    schema.tables?.reduce(
      (acc, table) =>
        acc +
        table.columns.filter(
          (col) =>
            (col.isPrivilegedData || col.is_privileged_data) &&
            (col.privilegedConfidence || col.privileged_confidence || 0) > 0.7
        ).length,
      0
    ) || 0;

  // Count tables with privileged data
  const privilegedTableCount =
    schema.tables?.filter((table) =>
      table.columns.some(
        (col) =>
          (col.isPrivilegedData || col.is_privileged_data) &&
          (col.privilegedConfidence || col.privileged_confidence || 0) > 0.7
      )
    ).length || 0;

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
              <p className="text-xs text-muted-foreground mt-1">High confidence columns</p>
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
              Real-time Schema View
            </p>
            <p className="text-sm text-blue-800 dark:text-blue-200 mt-1">
              This view shows the current live state of the database <strong>{databaseName}</strong>. 
              Any changes made directly to the database will be reflected here immediately upon refresh. 
              For version-controlled schemas, use the Repository feature.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

