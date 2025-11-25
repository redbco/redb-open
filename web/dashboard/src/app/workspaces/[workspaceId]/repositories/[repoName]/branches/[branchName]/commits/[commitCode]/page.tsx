'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useCommit } from '@/lib/hooks/useCommit';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { GitCommit, ArrowLeft, AlertCircle, RefreshCw, Copy, Check, Eye, EyeOff } from 'lucide-react';
import Link from 'next/link';
import { CommitSchemaOverview } from '@/components/repositories/CommitSchemaOverview';
import { CommitChangeSummary } from '@/components/repositories/CommitChangeSummary';
import { ContainerDiffCard } from '@/components/repositories/ContainerDiffCard';
import type { CommitSchemaStructure, CommitContainer, CommitItem } from '@/lib/api/types';

interface CommitSchemaPageProps {
  params: Promise<{
    workspaceId: string;
    repoName: string;
    branchName: string;
    commitCode: string;
  }>;
}

export default function CommitSchemaPage({ params }: CommitSchemaPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [repoName, setRepoName] = useState<string>('');
  const [branchName, setBranchName] = useState<string>('');
  const [commitCode, setCommitCode] = useState<string>('');
  const [isCopied, setIsCopied] = useState(false);
  const [showUnchanged, setShowUnchanged] = useState(true);
  const router = useRouter();
  const { showToast } = useToast();

  useEffect(() => {
    params.then(({ workspaceId, repoName, branchName, commitCode }) => {
      setWorkspaceId(workspaceId);
      setRepoName(decodeURIComponent(repoName));
      setBranchName(decodeURIComponent(branchName));
      setCommitCode(decodeURIComponent(commitCode));
    });
  }, [params]);

  const { commit, isLoading, error, refetch } = useCommit(workspaceId, repoName, branchName, commitCode);

  if (!workspaceId || !repoName || !branchName || !commitCode) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // Handle back navigation
  const handleViewHistory = () => {
    router.push(`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`);
  };

  const handleDeploySchema = () => {
    // TODO: Implement deploy schema functionality
    showToast({ type: 'info', title: 'Coming Soon', message: 'Deploy schema functionality coming soon' });
  };

  const handleRefresh = () => {
    refetch();
  };

  const handleCopyCommitPath = async () => {
    const commitPath = `${repoName}/${branchName}/${commitCode}`;
    try {
      await navigator.clipboard.writeText(commitPath);
      setIsCopied(true);
      showToast({
        type: 'success',
        title: 'Copied!',
        message: `Commit path copied: ${commitPath}`,
      });
      setTimeout(() => setIsCopied(false), 2000);
    } catch {
      showToast({
        type: 'error',
        title: 'Copy Failed',
        message: 'Failed to copy commit path to clipboard',
      });
    }
  };

  // Parse schema structure from commit
  let schemaStructure: CommitSchemaStructure = { containers: [], items: [] };
  if (commit?.schema_structure) {
    try {
      if (typeof commit.schema_structure === 'string') {
        schemaStructure = JSON.parse(commit.schema_structure);
      } else {
        schemaStructure = commit.schema_structure;
      }
    } catch (err) {
      console.error('[CommitSchemaPage] Error parsing schema structure:', err);
      console.error('[CommitSchemaPage] Schema structure:', commit.schema_structure);
    }
  }

  // Group containers by change status
  const containersByStatus = schemaStructure.containers.reduce(
    (acc, container) => {
      const status = container.change_status;
      if (status === 'STATUS_CREATED') acc.created.push(container);
      else if (status === 'STATUS_UPDATED') acc.updated.push(container);
      else if (status === 'STATUS_DELETED') acc.deleted.push(container);
      else acc.unchanged.push(container);
      return acc;
    },
    { created: [] as CommitContainer[], updated: [] as CommitContainer[], deleted: [] as CommitContainer[], unchanged: [] as CommitContainer[] }
  );

  // Create a map of container URI to items
  const itemsByContainer = schemaStructure.items.reduce((acc, item) => {
    if (!acc[item.container_uri]) {
      acc[item.container_uri] = [];
    }
    acc[item.container_uri].push(item);
    return acc;
  }, {} as Record<string, CommitItem[]>);

  // Get items for a container
  const getItemsForContainer = (container: CommitContainer): CommitItem[] => {
    return itemsByContainer[container.resource_uri] || [];
  };

  // Calculate counts
  const changedContainersCount = containersByStatus.created.length + containersByStatus.updated.length + containersByStatus.deleted.length;
  const unchangedContainersCount = containersByStatus.unchanged.length;

  // Check if this commit is HEAD (is_head flag or inferred from commit data)
  // Note: The backend should provide is_head flag, but we need to check the actual field name
  const isHead = commit?.is_head || false;
  
  // Check if this commit is deployed
  // Assumption: If the branch has an attached database, the HEAD commit is deployed
  // This is a simplified check - in a real implementation, we'd need backend support
  const isDeployed = isHead; // Simplified for now

  if (error) {
    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between bg-gradient-to-r from-indigo-50/50 to-transparent dark:from-indigo-900/10 p-6 -mx-6 rounded-lg">
          <div className="flex items-center gap-4">
            <Link
              href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
              className="p-2 hover:bg-accent rounded-md transition-colors"
            >
              <ArrowLeft className="h-5 w-5" />
            </Link>
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900 rounded-xl flex items-center justify-center border border-border shadow-sm">
                <GitCommit className="h-7 w-7 text-foreground" />
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
                  <Link
                    href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
                    className="hover:text-primary transition-colors"
                  >
                    {branchName}
                  </Link>
                  <span className="text-muted-foreground"> / </span>
                  {commitCode}
                </h2>
                <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                  <span className="font-medium">Commit Schema</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-6">
          <div className="flex items-start gap-3">
            <AlertCircle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-destructive font-semibold">Error Loading Commit</p>
              <p className="text-destructive/80 text-sm mt-1">{error.message}</p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (isLoading || !commit) {
    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between bg-gradient-to-r from-indigo-50/50 to-transparent dark:from-indigo-900/10 p-6 -mx-6 rounded-lg">
          <div className="flex items-center gap-4">
            <Link
              href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
              className="p-2 hover:bg-accent rounded-md transition-colors"
            >
              <ArrowLeft className="h-5 w-5" />
            </Link>
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900 rounded-xl flex items-center justify-center border border-border shadow-sm">
                <GitCommit className="h-7 w-7 text-foreground" />
              </div>
              <div>
                <h2 className="text-3xl font-bold text-foreground">Loading...</h2>
                <p className="text-xs text-muted-foreground mt-0.5">Fetching commit details</p>
              </div>
            </div>
          </div>
        </div>

        <div className="flex items-center justify-center min-h-[400px]">
          <LoadingSpinner size="lg" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between bg-gradient-to-r from-indigo-50/50 to-transparent dark:from-indigo-900/10 p-6 -mx-6 rounded-lg">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-900/20 dark:to-indigo-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm">
              <GitCommit className="h-7 w-7 text-indigo-600 dark:text-indigo-400" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground group/title cursor-pointer" onClick={handleCopyCommitPath}>
                <Link
                  href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}`}
                  className="hover:text-primary transition-colors"
                  onClick={(e) => e.stopPropagation()}
                >
                  {repoName}
                </Link>
                <span className="text-muted-foreground"> / </span>
                <Link
                  href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
                  className="hover:text-primary transition-colors"
                  onClick={(e) => e.stopPropagation()}
                >
                  {branchName}
                </Link>
                <span className="text-muted-foreground"> / </span>
                {commitCode}
                {isCopied ? (
                  <Check className="inline-block ml-2 h-5 w-5 text-green-600 dark:text-green-400" />
                ) : (
                  <Copy className="inline-block ml-2 h-5 w-5 text-muted-foreground opacity-0 group-hover/title:opacity-100 transition-opacity" />
                )}
              </h2>
              <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                <span className="font-medium">Commit Schema</span>
                {commit.commit_message && (
                  <>
                    <span>•</span>
                    <span className="truncate max-w-md">{commit.commit_message.split('\n')[0]}</span>
                  </>
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

      {/* Schema Overview */}
      <CommitSchemaOverview
        schemaStructure={schemaStructure}
        commitCode={commitCode}
        commitMessage={commit.commit_message}
        commitDate={commit.commit_date}
        branchName={branchName}
        repoName={repoName}
        isHead={isHead}
        isDeployed={isDeployed}
        onDeploySchema={handleDeploySchema}
        onViewHistory={handleViewHistory}
      />

      {/* Change Summary */}
      {schemaStructure.containers.length > 0 && (
        <CommitChangeSummary schemaStructure={schemaStructure} />
      )}

      {/* Containers Display with Toggle */}
      {schemaStructure.containers.length > 0 ? (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-xl font-semibold text-foreground">
              Containers
              {changedContainersCount > 0 && unchangedContainersCount > 0 && (
                <span className="ml-2 text-sm text-muted-foreground font-normal">
                  ({showUnchanged ? `${schemaStructure.containers.length} total` : `${changedContainersCount} changed, ${unchangedContainersCount} hidden`})
                </span>
              )}
            </h3>
            {unchangedContainersCount > 0 && (
              <button
                onClick={() => setShowUnchanged(!showUnchanged)}
                className="inline-flex items-center gap-2 px-3 py-2 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              >
                {showUnchanged ? (
                  <>
                    <EyeOff className="h-4 w-4" />
                    Hide Unchanged
                  </>
                ) : (
                  <>
                    <Eye className="h-4 w-4" />
                    Show Unchanged ({unchangedContainersCount})
                  </>
                )}
              </button>
            )}
          </div>

          {/* Created Containers */}
          {containersByStatus.created.length > 0 && (
            <>
              {containersByStatus.created.map((container, index) => (
                <ContainerDiffCard
                  key={`created-${container.object_name}-${index}`}
                  container={container}
                  items={getItemsForContainer(container)}
                  defaultExpanded={true}
                />
              ))}
            </>
          )}

          {/* Updated Containers */}
          {containersByStatus.updated.length > 0 && (
            <>
              {containersByStatus.updated.map((container, index) => (
                <ContainerDiffCard
                  key={`updated-${container.object_name}-${index}`}
                  container={container}
                  items={getItemsForContainer(container)}
                  defaultExpanded={true}
                />
              ))}
            </>
          )}

          {/* Deleted Containers */}
          {containersByStatus.deleted.length > 0 && (
            <>
              {containersByStatus.deleted.map((container, index) => (
                <ContainerDiffCard
                  key={`deleted-${container.object_name}-${index}`}
                  container={container}
                  items={getItemsForContainer(container)}
                  defaultExpanded={true}
                />
              ))}
            </>
          )}

          {/* Unchanged Containers */}
          {showUnchanged && containersByStatus.unchanged.length > 0 && (
            <>
              {containersByStatus.unchanged.map((container, index) => (
                <ContainerDiffCard
                  key={`unchanged-${container.object_name}-${index}`}
                  container={container}
                  items={getItemsForContainer(container)}
                  defaultExpanded={false}
                />
              ))}
            </>
          )}
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <AlertCircle className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-xl font-semibold text-foreground mb-2">No Schema Data</h3>
          <p className="text-muted-foreground">
            This commit does not contain any container definitions
          </p>
          {commit?.schema_structure && (
            <p className="text-xs text-muted-foreground mt-2">
              Schema structure type: {typeof commit.schema_structure}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

