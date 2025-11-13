'use client';

import { useState } from 'react';
import { Table, Columns, Shield, Activity, GitBranch, History, Database, ChevronDown, ChevronUp } from 'lucide-react';
import type { DatabaseSchema } from '@/lib/api/types';

interface CommitSchemaOverviewProps {
  schema: DatabaseSchema;
  commitCode: string;
  commitMessage?: string;
  commitDate?: string;
  branchName: string;
  repoName: string;
  isHead: boolean;
  isDeployed: boolean;
  onDeploySchema?: () => void;
  onViewHistory?: () => void;
}

export function CommitSchemaOverview({
  schema,
  commitCode,
  commitMessage,
  commitDate,
  branchName,
  repoName,
  isHead,
  isDeployed,
  onDeploySchema,
  onViewHistory,
}: CommitSchemaOverviewProps) {
  const [isMessageExpanded, setIsMessageExpanded] = useState(false);

  // Calculate statistics
  // Ensure tables is an array
  const tables = Array.isArray(schema.tables) ? schema.tables : [];
  const tableCount = tables.length;
  const columnCount = tables.reduce((acc, table) => {
    const columns = Array.isArray(table.columns) ? table.columns : [];
    return acc + columns.length;
  }, 0);
  
  // Count privileged columns (high confidence > 0.7)
  const privilegedColumnCount = tables.reduce((acc, table) => {
    const columns = Array.isArray(table.columns) ? table.columns : [];
    return acc + columns.filter(
      (col) =>
        (col.isPrivilegedData || col.is_privileged_data) &&
        (col.privilegedConfidence || col.privileged_confidence || 0) > 0.7
    ).length;
  }, 0);

  // Count tables with privileged data
  const privilegedTableCount = tables.filter((table) => {
    const columns = Array.isArray(table.columns) ? table.columns : [];
    return columns.some(
      (col) =>
        (col.isPrivilegedData || col.is_privileged_data) &&
        (col.privilegedConfidence || col.privileged_confidence || 0) > 0.7
    );
  }).length;

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });
    } catch {
      return dateString;
    }
  };

  return (
    <div className="space-y-6">
      {/* Version-Controlled Indicator & Actions */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          {/* Version-Controlled Badge */}
          <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-100 text-purple-800 dark:bg-purple-900/20 dark:text-purple-400 border border-purple-200 dark:border-purple-800">
            <GitBranch className="h-4 w-4" />
            <span className="font-semibold text-sm">VERSION-CONTROLLED</span>
          </div>

          {/* HEAD Badge */}
          {isHead && (
            <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400 border border-blue-200 dark:border-blue-800">
              <span className="font-semibold text-sm">HEAD</span>
            </div>
          )}

          {/* DEPLOYED Badge */}
          {isDeployed && (
            <div className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400 border border-green-200 dark:border-green-800">
              <Database className="h-4 w-4" />
              <span className="font-semibold text-sm">DEPLOYED</span>
            </div>
          )}
        </div>

        {/* Action Buttons */}
        <div className="flex items-center gap-2">
          {onViewHistory && (
            <button
              onClick={onViewHistory}
              className="inline-flex items-center gap-2 px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              <History className="h-4 w-4" />
              View History
            </button>
          )}
          {onDeploySchema && (
            <button
              onClick={onDeploySchema}
              className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              <Database className="h-4 w-4" />
              Deploy to Database
            </button>
          )}
        </div>
      </div>

      {/* Commit Metadata */}
      <div className="bg-card border border-border rounded-lg p-5">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div>
            <p className="text-sm font-medium text-muted-foreground mb-1">Repository</p>
            <p className="text-base font-mono text-foreground">{repoName}</p>
          </div>
          <div>
            <p className="text-sm font-medium text-muted-foreground mb-1">Branch</p>
            <p className="text-base font-mono text-foreground">{branchName}</p>
          </div>
          <div>
            <p className="text-sm font-medium text-muted-foreground mb-1">Commit</p>
            <p className="text-base font-mono text-foreground">{commitCode}</p>
          </div>
          <div>
            <p className="text-sm font-medium text-muted-foreground mb-1">Date</p>
            <p className="text-base text-foreground">{formatDate(commitDate)}</p>
          </div>
        </div>
        {commitMessage && (
          <div className="mt-4 pt-4 border-t border-border">
            <p className="text-sm font-medium text-muted-foreground mb-2">Commit Message</p>
            <div className="relative">
              <p className={`text-base text-foreground whitespace-pre-line ${!isMessageExpanded ? 'line-clamp-2' : ''}`}>
                {commitMessage}
              </p>
              {commitMessage.split('\n').length > 2 && (
                <button
                  onClick={() => setIsMessageExpanded(!isMessageExpanded)}
                  className="inline-flex items-center gap-1 mt-2 text-sm text-muted-foreground hover:text-foreground transition-colors"
                >
                  {isMessageExpanded ? (
                    <>
                      <ChevronUp className="h-3.5 w-3.5" />
                      Show less
                    </>
                  ) : (
                    <>
                      <ChevronDown className="h-3.5 w-3.5" />
                      Show more
                    </>
                  )}
                </button>
              )}
            </div>
          </div>
        )}
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
              <p className="text-3xl font-bold text-purple-600 dark:text-purple-400 mt-1">
                {isHead ? 'Latest' : 'Historic'}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {isDeployed ? 'Currently deployed' : 'Not deployed'}
              </p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-purple-100 dark:bg-purple-900/20 flex items-center justify-center">
              <Activity className="h-6 w-6 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
        </div>
      </div>

      {/* Info Banner */}
      <div className="bg-purple-50 dark:bg-purple-900/10 border border-purple-200 dark:border-purple-800 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0 w-5 h-5 rounded-full bg-purple-600 dark:bg-purple-400 flex items-center justify-center mt-0.5">
            <span className="text-white text-xs font-bold">i</span>
          </div>
          <div className="flex-1">
            <p className="text-sm text-purple-900 dark:text-purple-100 font-medium">
              Version-Controlled Schema View
            </p>
            <p className="text-sm text-purple-800 dark:text-purple-200 mt-1">
              This view shows a snapshot of the database schema as it was at commit <strong>{commitCode}</strong> in the <strong>{branchName}</strong> branch. 
              {isDeployed 
                ? ' This commit is currently deployed to a database.' 
                : ' This commit is not currently deployed.'
              }
              {' '}Schema modifications create new commits rather than directly modifying the database.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

