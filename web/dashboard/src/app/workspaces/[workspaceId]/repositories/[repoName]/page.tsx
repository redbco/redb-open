'use client';

import { useState, useEffect } from 'react';
import { useRepository } from '@/lib/hooks/useRepositories';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { GitBranch, ArrowLeft, Plus } from 'lucide-react';
import Link from 'next/link';
import { BranchCard } from '@/components/repositories/BranchCard';

interface RepositoryDetailPageProps {
  params: Promise<{
    workspaceId: string;
    repoName: string;
  }>;
}

export default function RepositoryDetailPage({ params }: RepositoryDetailPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [repoName, setRepoName] = useState<string>('');

  useEffect(() => {
    params.then(({ workspaceId, repoName }) => {
      setWorkspaceId(workspaceId);
      setRepoName(decodeURIComponent(repoName));
    });
  }, [params]);

  const { repository, isLoading, refetch } = useRepository(workspaceId, repoName);

  if (!workspaceId || !repoName) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  const branches = repository?.branches || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Link
          href={`/workspaces/${workspaceId}/repositories`}
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground mb-4"
        >
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back to Repositories
        </Link>
        
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold text-foreground">{repoName}</h2>
            {repository?.repo_description && (
              <p className="text-muted-foreground mt-2">
                {repository.repo_description}
              </p>
            )}
          </div>
          <button
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Create Branch
          </button>
        </div>
      </div>

      {/* Repository Info */}
      {repository && (
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-sm text-muted-foreground mb-1">Type</p>
              <p className="text-lg font-semibold text-foreground">
                {repository.repo_type || 'N/A'}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Branches</p>
              <p className="text-lg font-semibold text-foreground">
                {branches.length}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Created</p>
              <p className="text-lg font-semibold text-foreground">
                {repository.created
                  ? new Date(repository.created).toLocaleDateString()
                  : 'N/A'}
              </p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground mb-1">Status</p>
              <p className="text-lg font-semibold text-foreground">
                {repository.status || 'Active'}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Branches */}
      <div>
        <h3 className="text-xl font-semibold text-foreground mb-4">Branches</h3>
        
        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
                <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
                <div className="h-4 bg-muted rounded w-full mb-2"></div>
              </div>
            ))}
          </div>
        ) : branches.length === 0 ? (
          <div className="bg-card border border-border rounded-lg p-12 text-center">
            <GitBranch className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-xl font-semibold text-foreground mb-2">No Branches</h3>
            <p className="text-muted-foreground mb-6">
              Create your first branch to start managing schema versions
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {branches.map((branch) => (
              <BranchCard
                key={branch.branch_id}
                branch={branch}
                workspaceId={workspaceId}
                repoName={repoName}
                onUpdate={refetch}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

