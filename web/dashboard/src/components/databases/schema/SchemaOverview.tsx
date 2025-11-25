'use client';

import { Table, Columns, Shield, GitBranch, AlertCircle, Clock } from 'lucide-react';
import Link from 'next/link';
import type { DatabaseSchema, Database } from '@/lib/api/types';

interface SchemaOverviewProps {
  schema: DatabaseSchema;
  database?: Database;
  workspaceId?: string;
}

export function SchemaOverview({
  schema,
  database,
  workspaceId,
}: SchemaOverviewProps) {
  // Helper function to get containers or tables
  const getContainersOrTables = () => {
    if (schema.containers && schema.containers.length > 0) {
      return schema.containers.map(container => ({
        name: container.object_name,
        columns: container.items || [],
        itemCount: container.item_count, // Use item_count directly from resource_containers
        object_type: container.object_type,
      }));
    }
    // Fallback to legacy tables
    return (schema.tables || []).map(table => ({
      name: table.name,
      columns: table.columns || [],
      itemCount: table.columns?.length || 0, // Calculate from columns for legacy
      object_type: 'table',
    }));
  };

  const containers = getContainersOrTables();

  // Calculate statistics
  const tableCount = containers.length;
  const columnCount = containers.reduce((acc, container) => {
    // Use item_count when available (new format), otherwise count columns (legacy)
    return acc + (container.itemCount || container.columns?.length || 0);
  }, 0);
  
  // Count privileged columns by confidence level (using enriched schema endpoint data)
  const privilegedColumnStats = containers.reduce(
    (acc, container) => {
      const columns = container.columns || [];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return columns.some((col: any) => {
      const isPrivileged = col.is_privileged || col.isPrivilegedData || col.is_privileged_data;
      const confidence = col.detection_confidence || col.privilegedConfidence || col.privileged_confidence || 0;
      return isPrivileged && confidence > 0.7;
    });
  }).length;
  
  // Determine connection status for real-time indicator
  const isConnected = database?.status?.toLowerCase() === 'connected' || database?.status?.toLowerCase() === 'healthy';
  const isError = database?.status?.toLowerCase() === 'error';
  const isDisconnected = database?.status?.toLowerCase() === 'disconnected';
  const hasError = isError || isDisconnected;
  
  // Get appropriate status styling and text
  const getStatusConfig = () => {
    if (isConnected) {
      return {
        bgColor: 'bg-green-100 dark:bg-green-900/20',
        textColor: 'text-green-800 dark:text-green-400',
        borderColor: 'border-green-200 dark:border-green-800',
        dotColor: 'bg-green-500',
        pingColor: 'bg-green-400',
        label: 'LIVE',
        subtitle: 'Real-time Schema'
      };
    } else {
      return {
        bgColor: 'bg-amber-100 dark:bg-amber-900/20',
        textColor: 'text-amber-800 dark:text-amber-400',
        borderColor: 'border-amber-200 dark:border-amber-800',
        dotColor: 'bg-amber-500',
        pingColor: 'bg-amber-400',
        label: 'CACHED',
        subtitle: 'Most Recent Version'
      };
    }
  };
  
  const statusConfig = getStatusConfig();

  return (
    <div className="space-y-6">
      {/* Error Banner */}
      {hasError && database?.database_status_message && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <AlertCircle className="h-5 w-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <h4 className="font-semibold text-red-900 dark:text-red-300 mb-1">
                Connection {isError ? 'Error' : 'Disconnected'}
              </h4>
              <p className="text-sm text-red-800 dark:text-red-400">
                Error: {database.database_status_message}
              </p>
            </div>
          </div>
        </div>
      )}
    
      {/* Real-time Indicator & Actions */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* Status Badge */}
          <div className={`inline-flex items-center gap-2 px-3 py-2 rounded-lg border ${statusConfig.bgColor} ${statusConfig.textColor} ${statusConfig.borderColor}`}>
            <div className="relative flex h-2 w-2">
              {isConnected && (
                <span className={`animate-ping absolute inline-flex h-full w-full rounded-full ${statusConfig.pingColor} opacity-75`}></span>
              )}
              <span className={`relative inline-flex rounded-full h-2 w-2 ${statusConfig.dotColor}`}></span>
            </div>
            <span className="font-semibold text-sm">{statusConfig.label}</span>
            <span className="text-sm">{statusConfig.subtitle}</span>
          </div>

          <div className="text-sm text-muted-foreground flex items-center gap-1.5">
            <Clock className="h-3.5 w-3.5" />
            Last updated: {new Date().toLocaleTimeString()}
          </div>
        </div>

        {/* Action Buttons - Removed, handled in page header */}
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Tables</p>
              <p className="text-3xl font-bold text-foreground mt-1">{tableCount}</p>
              <p className="text-xs text-muted-foreground mt-1">
                {privilegedTableCount} with privileged data
              </p>
            </div>
            <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-900/20 dark:to-blue-800/20 flex items-center justify-center border border-border shadow-sm">
              <Table className="h-6 w-6 text-blue-600 dark:text-blue-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Columns</p>
              <p className="text-3xl font-bold text-foreground mt-1">{columnCount}</p>
              <p className="text-xs text-muted-foreground mt-1">Across all tables</p>
            </div>
            <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-900/20 dark:to-purple-800/20 flex items-center justify-center border border-border shadow-sm">
              <Columns className="h-6 w-6 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
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
            <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-red-50 to-red-100 dark:from-red-900/20 dark:to-red-800/20 flex items-center justify-center border border-border shadow-sm">
              <Shield className="h-6 w-6 text-red-600 dark:text-red-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Connected to Repository</p>
              {database?.connected_repo_name && database?.connected_branch_name ? (
                <>
                  {workspaceId ? (
                    <Link
                      href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(
                        database.connected_repo_name
                      )}/branches/${encodeURIComponent(database.connected_branch_name)}`}
                      className="text-2xl font-bold text-foreground mt-1 hover:underline block"
                    >
                      {database.connected_repo_name}/{database.connected_branch_name}
                    </Link>
                  ) : (
                    <p className="text-2xl font-bold text-foreground mt-1">
                      {database.connected_repo_name}/{database.connected_branch_name}
                    </p>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">Connected</p>
                </>
              ) : (
                <>
                  <p className="text-2xl font-bold text-muted-foreground mt-1">Not Connected</p>
                  <p className="text-xs text-muted-foreground mt-1">No repository attached</p>
                </>
              )}
            </div>
            <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-green-50 to-green-100 dark:from-green-900/20 dark:to-green-800/20 flex items-center justify-center border border-border shadow-sm">
              <GitBranch className="h-6 w-6 text-green-600 dark:text-green-400" />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

