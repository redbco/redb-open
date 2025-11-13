'use client';

import { GitBranch } from 'lucide-react';
import { BranchTreeNode } from './BranchTreeNode';
import type { Branch } from '@/lib/api/types';

interface RepositoryTreeProps {
  branches: Branch[];
  workspaceId: string;
  repoName: string;
}

export function RepositoryTree({ branches, workspaceId, repoName }: RepositoryTreeProps) {
  // Find the main branch (root branch with no parent)
  const mainBranch = branches.find(
    (b) => b.branch_name === 'main' || !b.parent_branch_id
  );

  if (!mainBranch && branches.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-12 text-center">
        <GitBranch className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-xl font-semibold text-foreground mb-2">No Branches</h3>
        <p className="text-muted-foreground">
          Create your first branch to start managing schema versions
        </p>
      </div>
    );
  }

  if (!mainBranch) {
    // Fallback: show all root branches if no main branch found
    const rootBranches = branches.filter((b) => !b.parent_branch_id);
    
    return (
      <div className="space-y-4">
        {rootBranches.map((branch) => (
          <BranchTreeNode
            key={branch.branch_id}
            branch={branch}
            workspaceId={workspaceId}
            repoName={repoName}
            level={0}
            allBranches={branches}
          />
        ))}
      </div>
    );
  }

  return (
    <div>
      <BranchTreeNode
        branch={mainBranch}
        workspaceId={workspaceId}
        repoName={repoName}
        level={0}
        allBranches={branches}
      />
    </div>
  );
}

