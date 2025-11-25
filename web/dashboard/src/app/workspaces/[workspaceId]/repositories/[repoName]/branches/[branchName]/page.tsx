'use client';

import { useState, useEffect } from 'react';
import { useBranch } from '@/lib/hooks/useBranch';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { GitCommit, ArrowLeft, GitBranch, Database, RefreshCw } from 'lucide-react';
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

  const { branch, isLoading, error, refetch } = useBranch(workspaceId, repoName, branchName);

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
    const dateA = new Date(a.commit_date || 0).getTime();
    const dateB = new Date(b.commit_date || 0).getTime();
    return dateB - dateA;
  });

  // Determine HEAD commit and if it's deployed
  const headCommit = sortedCommits.find((c) => (c as any).is_head || (c as any).isHead);
  const isHeadDeployed = !!branch?.connected_to_database;
  
  // Get the latest commit date
  const latestCommit = sortedCommits[0];
  const latestCommitDate = latestCommit?.commit_date;

  const handleRefresh = () => {
    refetch();
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between bg-gradient-to-r from-indigo-50/50 to-transparent dark:from-indigo-900/10 p-6 -mx-6 rounded-lg">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-900/20 dark:to-indigo-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm">
              <GitBranch className="h-7 w-7 text-indigo-600 dark:text-indigo-400" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">
                <Link
                  href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}`}
                  className="hover:text-primary transition-colors"
                >
                  {repoName}
                </Link>
                <span className="text-muted-foreground"> / </span>
                {branchName}
              </h2>
              <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                {branch?.parent_branch_name && (
                  <span>Branched from {branch.parent_branch_name}</span>
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

      {/* Branch Info Stats */}
      {branch && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Repository</p>
                <p className="text-2xl font-bold text-foreground mt-1 font-mono">{repoName}</p>
                <p className="text-xs text-muted-foreground mt-1">Source repository</p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-900/20 dark:to-indigo-800/20 flex items-center justify-center border border-border shadow-sm">
                <GitBranch className="h-6 w-6 text-indigo-600 dark:text-indigo-400" />
              </div>
            </div>
          </div>

          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Parent Branch</p>
                <p className="text-2xl font-bold text-foreground mt-1">
                  {branch.parent_branch_name || 'None'}
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  {branch.parent_branch_name ? 'Branched from' : 'Root branch'}
                </p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-900/20 dark:to-purple-800/20 flex items-center justify-center border border-border shadow-sm">
                <GitBranch className="h-6 w-6 text-purple-600 dark:text-purple-400" />
              </div>
            </div>
          </div>

          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Attached Database</p>
                <p className="text-2xl font-bold text-foreground mt-1">
                  {branch.connected_to_database && branch.database_name ? (
                    <Link
                      href={`/workspaces/${workspaceId}/databases/${branch.database_name}/schema`}
                      className="hover:text-primary hover:underline transition-colors"
                    >
                      {branch.database_name}
                    </Link>
                  ) : (
                    'Not attached'
                  )}
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  {branch.connected_to_database ? 'Connected' : 'No connection'}
                </p>
              </div>
              <div className={`w-12 h-12 rounded-xl bg-gradient-to-br flex items-center justify-center border border-border shadow-sm ${
                branch.connected_to_database
                  ? 'from-blue-50 to-blue-100 dark:from-blue-900/20 dark:to-blue-800/20'
                  : 'from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900'
              }`}>
                <Database className={`h-6 w-6 ${
                  branch.connected_to_database
                    ? 'text-blue-600 dark:text-blue-400'
                    : 'text-muted-foreground'
                }`} />
              </div>
            </div>
          </div>

          <div className="bg-card border border-border rounded-xl p-5 hover:shadow-lg transition-all duration-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-muted-foreground">Total Commits</p>
                <p className="text-3xl font-bold text-foreground mt-1">{commits.length}</p>
                <p className="text-xs text-muted-foreground mt-1">
                  {latestCommitDate
                    ? `Latest: ${new Date(latestCommitDate).toLocaleDateString('en-US', {
                        month: 'short',
                        day: 'numeric'
                      })}`
                    : 'No commits yet'}
                </p>
              </div>
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-green-50 to-green-100 dark:from-green-900/20 dark:to-green-800/20 flex items-center justify-center border border-border shadow-sm">
                <GitCommit className="h-6 w-6 text-green-600 dark:text-green-400" />
              </div>
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

