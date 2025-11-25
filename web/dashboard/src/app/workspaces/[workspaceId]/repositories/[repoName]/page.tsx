'use client';

import { useState, useEffect } from 'react';
import { useRepository } from '@/lib/hooks/useRepositories';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ArrowLeft, GitBranch, GitCommit, RefreshCw, Calendar } from 'lucide-react';
import Link from 'next/link';
import { RepositoryTree } from '@/components/repositories/RepositoryTree';
import { formatDatabaseType } from '@/lib/formatters';

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
  
  // Find the HEAD commit from all branches to get the repository type
  let repoType = 'Unknown';
  for (const branch of branches) {
    const headCommit = branch.commits?.find(commit => commit.is_head);
    if (headCommit?.schema_type) {
      repoType = formatDatabaseType(headCommit.schema_type);
      break;
    }
  }
  
  // Get the latest commit date from the first commit (most recent) in the main branch
  const mainBranch = repository?.branches?.find(b => b.branch_name === 'main') || repository?.branches?.[0];
  const latestCommit = mainBranch?.commits?.[0];
  const latestCommitDate = latestCommit?.commit_date;
  
  // Get total commit count across all branches
  const totalCommits = branches.reduce((sum, branch) => sum + (branch.commits?.length || 0), 0);

  const handleRefresh = () => {
    refetch();
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between bg-gradient-to-r from-indigo-50/50 to-transparent dark:from-indigo-900/10 p-6 -mx-6 rounded-lg">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/repositories`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-900/20 dark:to-indigo-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm">
              <GitBranch className="h-7 w-7 text-indigo-600 dark:text-indigo-400" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">{repoName}</h2>
              <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                {repository?.repo_description && (
                  <span className="font-medium">{repository.repo_description}</span>
                )}
              </div>
            </div>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Repository Info Stats */}
      {repository && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Repository Type</p>
                <p className="text-3xl font-bold text-foreground mt-1">{repoType}</p>
                <p className="text-xs text-muted-foreground mt-1">Schema type</p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-900/20 dark:to-purple-800/20 flex items-center justify-center border border-border shadow-sm">
                <GitBranch className="h-6 w-6 text-purple-600 dark:text-purple-400" />
              </div>
            </div>
          </div>

          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Total Branches</p>
                <p className="text-3xl font-bold text-foreground mt-1">{branches.length}</p>
                <p className="text-xs text-muted-foreground mt-1">Active branches</p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-900/20 dark:to-indigo-800/20 flex items-center justify-center border border-border shadow-sm">
                <GitBranch className="h-6 w-6 text-indigo-600 dark:text-indigo-400" />
              </div>
            </div>
          </div>

          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Total Commits</p>
                <p className="text-3xl font-bold text-foreground mt-1">{totalCommits}</p>
                <p className="text-xs text-muted-foreground mt-1">All branches</p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-900/20 dark:to-blue-800/20 flex items-center justify-center border border-border shadow-sm">
                <GitCommit className="h-6 w-6 text-blue-600 dark:text-blue-400" />
              </div>
            </div>
          </div>

          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Latest Commit</p>
                <p className="text-2xl font-bold text-foreground mt-1">
                  {latestCommitDate
                    ? new Date(latestCommitDate).toLocaleDateString('en-US', {
                        month: 'short',
                        day: 'numeric',
                        year: 'numeric'
                      })
                    : 'No commits'}
                </p>
                <p className="text-xs text-muted-foreground mt-1">Most recent change</p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-green-50 to-green-100 dark:from-green-900/20 dark:to-green-800/20 flex items-center justify-center border border-border shadow-sm">
                <Calendar className="h-6 w-6 text-green-600 dark:text-green-400" />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Branch Tree */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-xl font-semibold text-foreground">Branch Tree</h3>
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
            databaseConnections={repository?.database_connections || []}
          />
        )}
      </div>
    </div>
  );
}

