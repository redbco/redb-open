'use client';

import { useState } from 'react';
import { Repository } from '@/lib/api/types';
import {
  GitBranch,
  Settings,
  Trash2,
  Calendar,
  GitCommit,
  ChevronDown,
  ChevronUp,
  ExternalLink
} from 'lucide-react';

import Link from 'next/link';

interface RepositoryCardProps {
  repository: Repository;
  workspaceId: string;
  onUpdate: () => void;
}

export function RepositoryCard({ repository, workspaceId, onUpdate }: RepositoryCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Calculate branch count from branches array if available, otherwise use branch_count field
  const branchCount = repository.branches?.length ?? repository.branch_count ?? 0;
  
  // Calculate database connection status
  const getConnectionStatus = () => {
    const connections = repository.database_connections || [];
    if (connections.length === 0) return { status: 'offline', label: 'Offline' };
    
    const connectedCount = connections.filter(conn => 
      conn.database_status === 'STATUS_CONNECTED'
    ).length;
    
    if (connectedCount === connections.length) {
      return { status: 'connected', label: 'Connected' };
    } else if (connectedCount > 0) {
      return { status: 'degraded', label: 'Degraded' };
    } else {
      return { status: 'offline', label: 'Offline' };
    }
  };
  
  const connectionStatus = getConnectionStatus();

  const getStatusColor = (status: string = 'connected') => {
    switch (status.toLowerCase()) {
      case 'connected':
        return 'bg-emerald-500';
      case 'degraded':
        return 'bg-amber-500';
      case 'offline':
        return 'bg-rose-500';
      default:
        return 'bg-slate-500';
    }
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    } catch {
      return dateString;
    }
  };
  
  const formatDateTime = (dateString?: string) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });
    } catch {
      return dateString;
    }
  };

  return (
    <div className="group bg-card border border-border rounded-xl p-5 hover:shadow-lg hover:border-primary/20 transition-all duration-200 flex flex-col h-full relative">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-4">
          <div className="w-12 h-12 bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-900/20 dark:to-indigo-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
            <GitBranch className="h-6 w-6 text-indigo-600 dark:text-indigo-400" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <Link
                href={`/workspaces/${workspaceId}/repositories/${repository.repo_name}`}
                className="font-bold text-lg text-foreground hover:text-primary hover:underline transition-colors flex items-center gap-1"
                title="View Repository Details"
              >
                {repository.repo_name}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover:opacity-50 transition-opacity" />
              </Link>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
              <span className="font-medium truncate max-w-[200px]">
                {repository.repo_type || 'Schema Repository'}
              </span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <div className="relative flex h-2.5 w-2.5">
            <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${getStatusColor(connectionStatus.status)}`}></span>
            <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${getStatusColor(connectionStatus.status)}`}></span>
          </div>
          <span className="text-xs font-medium text-muted-foreground capitalize">{connectionStatus.label}</span>
        </div>
      </div>

      {/* Description */}
      <div className="flex-grow">
        <p className="text-sm text-muted-foreground mb-5 line-clamp-2 min-h-[2.5rem]">
          {repository.repo_description || <span className="text-muted-foreground/60 italic">No description provided</span>}
        </p>

        <div className="grid grid-cols-2 gap-y-3 gap-x-4 mb-4">
          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <GitBranch className="h-3.5 w-3.5" />
              Branches
            </span>
            <span className="text-sm font-medium text-foreground font-mono">
              {branchCount}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <GitCommit className="h-3.5 w-3.5" />
              Commits
            </span>
            <span className="text-sm font-medium text-foreground font-mono">
              {repository.commit_count || 0}
            </span>
          </div>
        </div>
        
        {/* Latest Commit Info */}
        {repository.latest_commit_code && (
          <div className="bg-muted/30 rounded-lg p-3 mb-4 border border-border/50">
            <div className="text-xs text-muted-foreground mb-1.5 font-medium">Latest Commit</div>
            <div className="flex items-center justify-between gap-2">
              <Link
                href={`/workspaces/${workspaceId}/repositories/${repository.repo_name}/branches/${repository.latest_commit_branch || 'main'}/commits/${repository.latest_commit_code}`}
                className="font-mono text-xs font-semibold text-foreground hover:text-primary transition-colors truncate"
              >
                {repository.repo_name}/{repository.latest_commit_branch || 'main'}/{repository.latest_commit_code}
              </Link>
            </div>
            <div className="text-xs text-muted-foreground mt-1.5 flex items-center gap-1">
              <Calendar className="h-3 w-3" />
              {formatDateTime(repository.latest_commit_date)}
            </div>
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="mt-auto pt-4 border-t border-border/50 flex items-center justify-between">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-xs text-muted-foreground hover:text-foreground font-medium flex items-center gap-1 transition-colors"
        >
          {isExpanded ? (
            <>
              <ChevronUp className="h-3.5 w-3.5" />
              Hide Details
            </>
          ) : (
            <>
              <ChevronDown className="h-3.5 w-3.5" />
              View Details
            </>
          )}
        </button>

        <div className="flex gap-1">
          <button
            onClick={() => {
              // TODO: Implement modify
            }}
            className="text-muted-foreground hover:text-primary p-1.5 rounded-md hover:bg-primary/10 transition-colors"
            title="Modify Repository"
          >
            <Settings className="h-4 w-4" />
          </button>

          <button
            onClick={() => {
              // TODO: Implement delete
            }}
            className="text-muted-foreground hover:text-red-600 dark:hover:text-red-400 p-1.5 rounded-md hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
            title="Delete Repository"
          >
            <Trash2 className="h-4 w-4" />
          </button>
        </div>
      </div>

      {isExpanded && (
        <div className="mt-3 pt-3 border-t border-border/50 space-y-2.5 text-xs animate-in slide-in-from-top-2 duration-200">
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Repo ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={repository.repo_id}>{repository.repo_id}</span>
          </div>
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Tenant ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={repository.tenant_id}>{repository.tenant_id}</span>
          </div>
          {repository.owner_id && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Owner:</span>
              <span className="font-mono text-foreground truncate">{repository.owner_id}</span>
            </div>
          )}
          {repository.created && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Created:</span>
              <span className="text-foreground truncate">{formatDate(repository.created)}</span>
            </div>
          )}
          {repository.updated && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Last Updated:</span>
              <span className="text-foreground truncate">{formatDate(repository.updated)}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

