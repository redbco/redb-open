'use client';

import { useState, useEffect } from 'react';
import { useBranch } from '@/lib/hooks/useBranch';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { GitCommit, ArrowLeft } from 'lucide-react';
import Link from 'next/link';
import { CommitCard } from '@/components/repositories/CommitCard';

interface BranchDetailPageProps {
  params: Promise<{
    workspaceId: string;
    repoName: string;
    branchName: string;
  }>;
}

export default function BranchDetailPage({ params }: BranchDetailPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [repoName, setRepoName] = useState<string>('');
  const [branchName, setBranchName] = useState<string>('');

  useEffect(() => {
    params.then(({ workspaceId, repoName, branchName }) => {
      setWorkspaceId(workspaceId);
      setRepoName(decodeURIComponent(repoName));
      setBranchName(decodeURIComponent(branchName));
    });
  }, [params]);

  const { branch, isLoading, error } = useBranch(workspaceId, repoName, branchName);

  if (!workspaceId || !repoName || !branchName) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  const commits = branch?.commits || [];

  // Sort commits by created date (latest first)
  const sortedCommits = [...commits].sort((a, b) => {
    const dateA = new Date(a.created || 0).getTime();
    const dateB = new Date(b.created || 0).getTime();
    return dateB - dateA;
  });

  // Determine HEAD commit and if it's deployed
  const headCommit = sortedCommits.find((c) => (c as any).is_head || (c as any).isHead);
  const isHeadDeployed = !!branch?.attached_database_id;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}`}
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground mb-4"
        >
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back to {repoName}
        </Link>
        
        <div>
          <h2 className="text-3xl font-bold text-foreground">{branchName}</h2>
          {branch?.branch_description && (
            <p className="text-muted-foreground mt-2">
              {branch.branch_description}
            </p>
          )}
        </div>
      </div>

      {/* Branch Info */}
      {branch && (
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-sm text-muted-foreground mb-1">Repository</p>
              <p className="text-lg font-semibold text-foreground font-mono">
                {branch.repo_name}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Parent Branch</p>
              <p className="text-lg font-semibold text-foreground">
                {branch.parent_branch_name || 'None'}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Attached Database</p>
              <p className="text-lg font-semibold text-foreground">
                {branch.attached_database_name || 'Not attached'}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Commits</p>
              <p className="text-lg font-semibold text-foreground">
                {commits.length}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Commits */}
      <div>
        <h3 className="text-xl font-semibold text-foreground mb-4">Commits</h3>
        
        {isLoading ? (
          <div className="space-y-4">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
                <div className="h-5 bg-muted rounded w-1/4 mb-3"></div>
                <div className="h-4 bg-muted rounded w-3/4 mb-2"></div>
                <div className="h-4 bg-muted rounded w-1/2"></div>
              </div>
            ))}
          </div>
        ) : commits.length === 0 ? (
          <div className="bg-card border border-border rounded-lg p-12 text-center">
            <GitCommit className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-xl font-semibold text-foreground mb-2">No Commits</h3>
            <p className="text-muted-foreground">
              No commits have been made to this branch yet
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {sortedCommits.map((commit) => {
              const isHead = headCommit?.commit_id === commit.commit_id;
              const isDeployed = isHead && isHeadDeployed;
              
              return (
                <CommitCard 
                  key={commit.commit_id} 
                  commit={commit}
                  workspaceId={workspaceId}
                  repoName={repoName}
                  branchName={branchName}
                  isHead={isHead}
                  isDeployed={isDeployed}
                />
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

