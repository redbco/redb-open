'use client';

import { Commit } from '@/lib/api/types';
import { GitCommit, Calendar, User } from 'lucide-react';
import Link from 'next/link';

interface CommitCardProps {
  commit: Commit;
  workspaceId?: string;
  repoName?: string;
  branchName?: string;
  isHead?: boolean;
  isDeployed?: boolean;
}

export function CommitCard({ 
  commit, 
  workspaceId, 
  repoName, 
  branchName,
  isHead = false,
  isDeployed = false,
}: CommitCardProps) {
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

  const commitUrl = workspaceId && repoName && branchName
    ? `/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}/commits/${encodeURIComponent(commit.commit_code)}`
    : undefined;

  const CardWrapper = commitUrl ? Link : 'div';
  const cardProps = commitUrl 
    ? { href: commitUrl, className: "block bg-card border border-border rounded-lg p-4 hover:border-primary/50 hover:bg-accent/50 transition-all duration-200 cursor-pointer" }
    : { className: "bg-card border border-border rounded-lg p-4" };

  return (
    <CardWrapper {...cardProps as any}>
      <div className="flex items-start space-x-3">
        <div className="p-2 bg-primary/10 rounded-lg flex-shrink-0">
          <GitCommit className="h-5 w-5 text-primary" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <h4 className="text-sm font-semibold text-foreground font-mono">
                {commit.commit_code}
              </h4>
              
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
            
            <span className="text-xs text-muted-foreground">
              {formatDate(commit.created)}
            </span>
          </div>
          
          {commit.commit_message && (
            <p className="text-sm text-foreground mb-2 whitespace-pre-line">
              {commit.commit_message}
            </p>
          )}
          
          {commit.commit_description && (
            <p className="text-sm text-muted-foreground line-clamp-2">
              {commit.commit_description}
            </p>
          )}
          
          {commit.parent_commit_code && (
            <div className="flex items-center text-xs text-muted-foreground mt-2">
              <GitCommit className="h-3 w-3 mr-1" />
              <span>Parent: {commit.parent_commit_code}</span>
            </div>
          )}
        </div>
      </div>
    </CardWrapper>
  );
}

