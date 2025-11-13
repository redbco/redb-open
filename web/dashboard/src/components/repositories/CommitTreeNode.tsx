'use client';

import { GitCommit, Calendar } from 'lucide-react';
import Link from 'next/link';
import type { Commit } from '@/lib/api/types';

interface CommitTreeNodeProps {
  commit: Commit;
  workspaceId: string;
  repoName: string;
  branchName: string;
  isHead: boolean;
  isDeployed: boolean;
  isCompact?: boolean;
}

export function CommitTreeNode({
  commit,
  workspaceId,
  repoName,
  branchName,
  isHead,
  isDeployed,
  isCompact = false,
}: CommitTreeNodeProps) {
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

  const commitUrl = `/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}/commits/${encodeURIComponent(commit.commit_code)}`;

  return (
    <Link
      href={commitUrl}
      className={`
        block border border-border rounded-md hover:border-primary/50 hover:bg-accent/50 transition-all duration-200
        ${isCompact ? 'p-2' : 'p-3'}
      `}
    >
      <div className="flex items-start gap-3">
        <div className={`p-1.5 bg-primary/10 rounded-md flex-shrink-0 ${isCompact ? '' : 'mt-0.5'}`}>
          <GitCommit className={`text-primary ${isCompact ? 'h-3.5 w-3.5' : 'h-4 w-4'}`} />
        </div>
        
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className={`font-mono font-semibold text-foreground ${isCompact ? 'text-xs' : 'text-sm'}`}>
              {commit.commit_code}
            </span>
            
            {isHead && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 border border-blue-200 dark:border-blue-800">
                HEAD
              </span>
            )}
            
            {isDeployed && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 border border-green-200 dark:border-green-800">
                DEPLOYED
              </span>
            )}
          </div>
          
          {commit.commit_message && (
            <p className={`text-muted-foreground mb-1 ${isCompact ? 'text-xs line-clamp-1' : 'text-sm line-clamp-2'}`}>
              {commit.commit_message}
            </p>
          )}
          
          <div className={`flex items-center text-muted-foreground ${isCompact ? 'text-xs' : 'text-xs'}`}>
            <Calendar className="h-3 w-3 mr-1" />
            <span>{formatDate(commit.created)}</span>
          </div>
        </div>
      </div>
    </Link>
  );
}

