'use client';

import { Commit } from '@/lib/api/types';
import { GitCommit, Calendar, User } from 'lucide-react';

interface CommitCardProps {
  commit: Commit;
}

export function CommitCard({ commit }: CommitCardProps) {
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
    <div className="bg-card border border-border rounded-lg p-4 hover:border-primary/50 transition-all duration-200">
      <div className="flex items-start space-x-3">
        <div className="p-2 bg-primary/10 rounded-lg flex-shrink-0">
          <GitCommit className="h-5 w-5 text-primary" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-semibold text-foreground font-mono">
              {commit.commit_code}
            </h4>
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
    </div>
  );
}

