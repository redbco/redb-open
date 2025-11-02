'use client';

import { useState } from 'react';
import { Branch } from '@/lib/api/types';
import {
  GitBranch,
  Eye,
  MoreVertical,
  Link2,
  Unlink,
  Trash2,
  Calendar,
  GitCommit,
  Database,
} from 'lucide-react';
import Link from 'next/link';

interface BranchCardProps {
  branch: Branch;
  workspaceId: string;
  repoName: string;
  onUpdate: () => void;
}

export function BranchCard({ branch, workspaceId, repoName, onUpdate }: BranchCardProps) {
  const [showMenu, setShowMenu] = useState(false);

  // Calculate commit count from commits array if available, otherwise use commit_count field
  const commitCount = branch.commits?.length ?? branch.commit_count ?? 0;

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

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <GitBranch className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-foreground">
              {branch.branch_name}
            </h3>
            {branch.parent_branch_name && (
              <p className="text-sm text-muted-foreground">
                From: {branch.parent_branch_name}
              </p>
            )}
          </div>
        </div>
        
        <div className="relative">
          <button
            onClick={() => setShowMenu(!showMenu)}
            className="p-1 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground"
          >
            <MoreVertical className="h-5 w-5" />
          </button>
          
          {showMenu && (
            <>
              <div
                className="fixed inset-0 z-10"
                onClick={() => setShowMenu(false)}
              />
              <div className="absolute right-0 mt-2 w-48 bg-popover border border-border rounded-md shadow-lg z-20 py-1">
                <Link
                  href={`/workspaces/${workspaceId}/repositories/${repoName}/branches/${branch.branch_name}`}
                  className="flex items-center px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                  onClick={() => setShowMenu(false)}
                >
                  <Eye className="h-4 w-4 mr-2" />
                  View Commits
                </Link>
                {!branch.attached_database_name ? (
                  <button
                    className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                    onClick={() => {
                      setShowMenu(false);
                      // TODO: Implement attach
                    }}
                  >
                    <Link2 className="h-4 w-4 mr-2" />
                    Attach Database
                  </button>
                ) : (
                  <button
                    className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                    onClick={() => {
                      setShowMenu(false);
                      // TODO: Implement detach
                    }}
                  >
                    <Unlink className="h-4 w-4 mr-2" />
                    Detach Database
                  </button>
                )}
                <div className="border-t border-border my-1" />
                <button
                  className="flex items-center w-full px-4 py-2 text-sm text-destructive hover:bg-accent"
                  onClick={() => {
                    setShowMenu(false);
                    // TODO: Implement delete
                  }}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  Delete
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Description */}
      {branch.branch_description && (
        <p className="text-sm text-muted-foreground mb-4">
          {branch.branch_description}
        </p>
      )}

      {/* Info */}
      <div className="space-y-2 mb-4">
        {branch.attached_database_name && (
          <div className="flex items-center text-sm">
            <Database className="h-4 w-4 mr-2 text-muted-foreground" />
            <span className="text-foreground">
              Attached to: <span className="font-mono">{branch.attached_database_name}</span>
            </span>
          </div>
        )}
        <div className="flex items-center text-sm">
          <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-muted-foreground">
            Created {formatDate(branch.created)}
          </span>
        </div>
      </div>

      {/* Commit Count */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center text-sm text-muted-foreground">
          <GitCommit className="h-4 w-4 mr-2" />
          <span>{commitCount} {commitCount === 1 ? 'commit' : 'commits'}</span>
        </div>
        <Link
          href={`/workspaces/${workspaceId}/repositories/${repoName}/branches/${branch.branch_name}`}
          className="text-sm text-primary hover:text-primary/80 font-medium"
        >
          View Commits →
        </Link>
      </div>
    </div>
  );
}

