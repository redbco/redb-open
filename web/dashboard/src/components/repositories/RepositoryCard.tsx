'use client';

import { useState } from 'react';
import { Repository } from '@/lib/api/types';
import {
  GitBranch,
  Eye,
  MoreVertical,
  Settings,
  Trash2,
  Calendar,
  GitCommit,
} from 'lucide-react';
import Link from 'next/link';

interface RepositoryCardProps {
  repository: Repository;
  workspaceId: string;
  onUpdate: () => void;
}

export function RepositoryCard({ repository, workspaceId, onUpdate }: RepositoryCardProps) {
  const [showMenu, setShowMenu] = useState(false);

  // Calculate branch count from branches array if available, otherwise use branch_count field
  const branchCount = repository.branches?.length ?? repository.branch_count ?? 0;

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
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200 hover:shadow-lg">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <GitBranch className="h-6 w-6 text-primary" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-foreground">
              {repository.repo_name}
            </h3>
            {repository.repo_type && (
              <p className="text-sm text-muted-foreground">{repository.repo_type}</p>
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
                  href={`/workspaces/${workspaceId}/repositories/${repository.repo_name}`}
                  className="flex items-center px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                  onClick={() => setShowMenu(false)}
                >
                  <Eye className="h-4 w-4 mr-2" />
                  View Details
                </Link>
                <button
                  className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                  onClick={() => {
                    setShowMenu(false);
                    // TODO: Implement modify
                  }}
                >
                  <Settings className="h-4 w-4 mr-2" />
                  Modify
                </button>
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
      {repository.repo_description && (
        <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
          {repository.repo_description}
        </p>
      )}

      {/* Info */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-muted-foreground">
            Created {formatDate(repository.created)}
          </span>
        </div>
      </div>

      {/* Branch Count */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center text-sm text-muted-foreground">
          <GitCommit className="h-4 w-4 mr-2" />
          <span>{branchCount} {branchCount === 1 ? 'branch' : 'branches'}</span>
        </div>
        <Link
          href={`/workspaces/${workspaceId}/repositories/${repository.repo_name}`}
          className="text-sm text-primary hover:text-primary/80 font-medium"
        >
          View Details →
        </Link>
      </div>
    </div>
  );
}

