'use client';

import { useState } from 'react';
import { GitBranch, ChevronRight, ChevronDown, Database, History } from 'lucide-react';
import Link from 'next/link';
import { CommitTreeNode } from './CommitTreeNode';
import type { Branch } from '@/lib/api/types';

interface BranchTreeNodeProps {
  branch: Branch;
  workspaceId: string;
  repoName: string;
  level?: number;
  allBranches: Branch[];
}

export function BranchTreeNode({
  branch,
  workspaceId,
  repoName,
  level = 0,
  allBranches,
}: BranchTreeNodeProps) {
  const [showCommits, setShowCommits] = useState(false);
  const [showChildren, setShowChildren] = useState(true);

  // Find child branches
  const childBranches = allBranches.filter(
    (b) => b.parent_branch_id === branch.branch_id
  );

  // Sort commits by created date (latest first)
  const sortedCommits = [...(branch.commits || [])].sort((a, b) => {
    const dateA = new Date(a.created || 0).getTime();
    const dateB = new Date(b.created || 0).getTime();
    return dateB - dateA;
  });

  // HEAD commit is the one with is_head flag or the first in sorted list
  const headCommit = sortedCommits.find((c) => c.is_head) || sortedCommits[0];
  const previousCommits = sortedCommits.filter((c) => c.commit_id !== headCommit?.commit_id);

  // Determine if HEAD commit is deployed (assume HEAD of attached branch is deployed)
  const isHeadDeployed = !!branch.attached_database_id;

  const branchUrl = `/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branch.branch_name)}`;

  const indentClass = level === 0 ? '' : `ml-${Math.min(level * 8, 24)}`;

  return (
    <div className={`${indentClass}`}>
      <div className="border border-border rounded-lg overflow-hidden mb-3 bg-card">
        {/* Branch Header */}
        <div className="p-4 bg-muted/30 border-b border-border">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 flex-1">
              {childBranches.length > 0 && (
                <button
                  onClick={() => setShowChildren(!showChildren)}
                  className="p-1 hover:bg-accent rounded transition-colors"
                  aria-label={showChildren ? 'Collapse child branches' : 'Expand child branches'}
                >
                  {showChildren ? (
                    <ChevronDown className="h-4 w-4 text-muted-foreground" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-muted-foreground" />
                  )}
                </button>
              )}
              
              <div className="p-2 bg-primary/10 rounded-lg">
                <GitBranch className="h-5 w-5 text-primary" />
              </div>
              
              <div className="flex-1">
                <Link
                  href={branchUrl}
                  className="text-lg font-semibold text-foreground hover:text-primary transition-colors"
                >
                  {branch.branch_name}
                </Link>
                
                <div className="flex items-center gap-3 mt-1">
                  {branch.attached_database_name && (
                    <div className="inline-flex items-center gap-1 text-sm text-muted-foreground">
                      <Database className="h-3.5 w-3.5" />
                      <span>Attached to: </span>
                      <span className="font-mono text-foreground">{branch.attached_database_name}</span>
                    </div>
                  )}
                  
                  {branch.parent_branch_name && (
                    <span className="text-sm text-muted-foreground">
                      From: {branch.parent_branch_name}
                    </span>
                  )}
                </div>
              </div>
            </div>
            
            <Link
              href={branchUrl}
              className="inline-flex items-center gap-2 px-3 py-1.5 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              <History className="h-3.5 w-3.5" />
              View History
            </Link>
          </div>
        </div>

        {/* Commits Section */}
        <div className="p-4">
          {/* HEAD Commit (always visible) */}
          {headCommit && (
            <div className="mb-3">
              <div className="text-xs font-medium text-muted-foreground mb-2 uppercase tracking-wide">
                Latest Commit
              </div>
              <CommitTreeNode
                commit={headCommit}
                workspaceId={workspaceId}
                repoName={repoName}
                branchName={branch.branch_name}
                isHead={true}
                isDeployed={isHeadDeployed}
                isCompact={false}
              />
            </div>
          )}

          {/* Previous Commits (expandable) */}
          {previousCommits.length > 0 && (
            <div>
              <button
                onClick={() => setShowCommits(!showCommits)}
                className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors mb-2"
              >
                {showCommits ? (
                  <ChevronDown className="h-4 w-4" />
                ) : (
                  <ChevronRight className="h-4 w-4" />
                )}
                <span>{previousCommits.length} previous {previousCommits.length === 1 ? 'commit' : 'commits'}</span>
              </button>

              {showCommits && (
                <div className="space-y-2 pl-6 border-l-2 border-border ml-2">
                  {previousCommits.map((commit) => (
                    <CommitTreeNode
                      key={commit.commit_id}
                      commit={commit}
                      workspaceId={workspaceId}
                      repoName={repoName}
                      branchName={branch.branch_name}
                      isHead={false}
                      isDeployed={false}
                      isCompact={true}
                    />
                  ))}
                </div>
              )}
            </div>
          )}

          {!headCommit && sortedCommits.length === 0 && (
            <div className="text-sm text-muted-foreground text-center py-4">
              No commits in this branch yet
            </div>
          )}
        </div>
      </div>

      {/* Child Branches (recursive) */}
      {showChildren && childBranches.length > 0 && (
        <div className="ml-8 space-y-3 border-l-2 border-border pl-4">
          {childBranches.map((childBranch) => (
            <BranchTreeNode
              key={childBranch.branch_id}
              branch={childBranch}
              workspaceId={workspaceId}
              repoName={repoName}
              level={level + 1}
              allBranches={allBranches}
            />
          ))}
        </div>
      )}
    </div>
  );
}

