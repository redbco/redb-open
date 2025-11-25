'use client';

import { useState } from 'react';
import { Table, Columns, Shield, Activity, GitBranch, History, Database, ChevronDown, ChevronUp, Plus, Edit, Minus } from 'lucide-react';
import type { CommitSchemaStructure } from '@/lib/api/types';

interface CommitSchemaOverviewProps {
  schemaStructure: CommitSchemaStructure;
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
  schemaStructure,
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

  // Calculate statistics from containers and items
  const containerStats = schemaStructure.containers.reduce(
    (acc, container) => {
      if (container.change_status === 'STATUS_CREATED') acc.created++;
      else if (container.change_status === 'STATUS_UPDATED') acc.updated++;
      else if (container.change_status === 'STATUS_DELETED') acc.deleted++;
      else acc.unchanged++;
      return acc;
    },
    { created: 0, updated: 0, deleted: 0, unchanged: 0, total: schemaStructure.containers.length }
  );

  const itemStats = schemaStructure.items.reduce(
    (acc, item) => {
      if (item.change_status === 'STATUS_CREATED') acc.created++;
      else if (item.change_status === 'STATUS_UPDATED') acc.updated++;
      else if (item.change_status === 'STATUS_DELETED') acc.deleted++;
      else acc.unchanged++;
      return acc;
    },
    { created: 0, updated: 0, deleted: 0, unchanged: 0, total: schemaStructure.items.length }
  );

  // Count privileged items (high confidence > 0.7)
  const privilegedItemCount = schemaStructure.items.filter(
    (item) => item.is_privileged && (item.detection_confidence || 0) > 0.7
  ).length;

  // Count containers with privileged data
  const containersWithPrivilegedData = new Set(
    schemaStructure.items
      .filter((item) => item.is_privileged && (item.detection_confidence || 0) > 0.7)
      .map((item) => item.container_uri)
  ).size;

  const hasChanges = containerStats.created > 0 || containerStats.updated > 0 || containerStats.deleted > 0;

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

        {/* Action Buttons - HIDDEN FOR NOW */}
        {/*
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
        */}
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
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Containers</p>
              <p className="text-3xl font-bold text-foreground mt-1">{containerStats.total}</p>
              <p className="text-xs text-muted-foreground mt-1">
                {containersWithPrivilegedData} with privileged data
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
              <p className="text-sm font-medium text-muted-foreground">Items</p>
              <p className="text-3xl font-bold text-foreground mt-1">{itemStats.total}</p>
              <p className="text-xs text-muted-foreground mt-1">
                {privilegedItemCount} privileged items
              </p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-purple-100 dark:bg-purple-900/20 flex items-center justify-center">
              <Columns className="h-6 w-6 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Changes</p>
              <p className="text-3xl font-bold text-foreground mt-1">
                {containerStats.created + containerStats.updated + containerStats.deleted}
              </p>
              <div className="flex items-center gap-2 mt-1 text-xs">
                {containerStats.created > 0 && (
                  <span className="flex items-center gap-0.5 text-green-600 dark:text-green-400">
                    <Plus className="h-3 w-3" />
                    {containerStats.created}
                  </span>
                )}
                {containerStats.updated > 0 && (
                  <span className="flex items-center gap-0.5 text-yellow-600 dark:text-yellow-400">
                    <Edit className="h-3 w-3" />
                    {containerStats.updated}
                  </span>
                )}
                {containerStats.deleted > 0 && (
                  <span className="flex items-center gap-0.5 text-red-600 dark:text-red-400">
                    <Minus className="h-3 w-3" />
                    {containerStats.deleted}
                  </span>
                )}
              </div>
            </div>
            <div className="w-12 h-12 rounded-lg bg-orange-100 dark:bg-orange-900/20 flex items-center justify-center">
              <Activity className="h-6 w-6 text-orange-600 dark:text-orange-400" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Schema Status</p>
              <p className="text-3xl font-bold text-purple-600 dark:text-purple-400 mt-1">
                {isHead ? 'Current' : 'Historic'}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {isDeployed ? 'Currently deployed' : 'Not deployed'}
              </p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-purple-100 dark:bg-purple-900/20 flex items-center justify-center">
              <Database className="h-6 w-6 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
        </div>
      </div>

      {/* Info Banner */}
      <div className={`border rounded-lg p-4 ${
        hasChanges 
          ? 'bg-blue-50 dark:bg-blue-900/10 border-blue-200 dark:border-blue-800' 
          : 'bg-purple-50 dark:bg-purple-900/10 border-purple-200 dark:border-purple-800'
      }`}>
        <div className="flex items-start gap-3">
          <div className={`flex-shrink-0 w-5 h-5 rounded-full flex items-center justify-center mt-0.5 ${
            hasChanges 
              ? 'bg-blue-600 dark:bg-blue-400' 
              : 'bg-purple-600 dark:bg-purple-400'
          }`}>
            <span className="text-white text-xs font-bold">i</span>
          </div>
          <div className="flex-1">
            <p className={`text-sm font-medium ${
              hasChanges 
                ? 'text-blue-900 dark:text-blue-100' 
                : 'text-purple-900 dark:text-purple-100'
            }`}>
              {hasChanges ? 'Schema Diff View' : 'Version-Controlled Schema View'}
            </p>
            <p className={`text-sm mt-1 ${
              hasChanges 
                ? 'text-blue-800 dark:text-blue-200' 
                : 'text-purple-800 dark:text-purple-200'
            }`}>
              This view shows {hasChanges ? 'the schema changes in' : 'a snapshot of the database schema as it was at'} commit <strong>{commitCode}</strong> in the <strong>{branchName}</strong> branch.
              {hasChanges && (
                <> Changes are highlighted with color coding: <span className="text-green-700 dark:text-green-400 font-semibold">green for additions</span>, <span className="text-yellow-700 dark:text-yellow-400 font-semibold">yellow for modifications</span>, and <span className="text-red-700 dark:text-red-400 font-semibold">red for deletions</span>.</>
              )}
              {isDeployed 
                ? ' This commit is currently deployed to a database.' 
                : ' This commit is not currently deployed.'
              }
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

