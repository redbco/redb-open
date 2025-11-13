'use client';

import { useState, useEffect } from 'react';
import { useRepository } from '@/lib/hooks/useRepositories';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ArrowLeft, Plus } from 'lucide-react';
import Link from 'next/link';
import { RepositoryTree } from '@/components/repositories/RepositoryTree';

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

  const { repository, isLoading } = useRepository(workspaceId, repoName);

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

      {/* Branch Tree */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-xl font-semibold text-foreground">Branch Tree</h3>
          <button
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Create Branch
          </button>
        </div>
        
        {isLoading ? (
          <div className="space-y-4">
            {[...Array(2)].map((_, i) => (
              <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
                <div className="h-6 bg-muted rounded w-1/4 mb-4"></div>
                <div className="h-20 bg-muted rounded w-full"></div>
              </div>
            ))}
          </div>
        ) : (
          <RepositoryTree
            branches={branches}
            workspaceId={workspaceId}
            repoName={repoName}
          />
        )}
      </div>
    </div>
  );
}

