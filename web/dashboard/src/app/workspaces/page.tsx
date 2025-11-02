'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/lib/auth/auth-context';
import { useWorkspaces } from '@/lib/hooks/useWorkspace';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { Folder, Plus, ArrowRight } from 'lucide-react';

export default function WorkspacesPage() {
  const router = useRouter();
  const { isAuthenticated, isLoading: authLoading } = useAuth();
  const { workspaces, isLoading: workspacesLoading, error } = useWorkspaces();

  useEffect(() => {
    if (!authLoading && !isAuthenticated) {
      router.push('/auth/login');
    }
  }, [isAuthenticated, authLoading, router]);

  // Auto-redirect if there's only one workspace (disabled to allow manual workspace selection)
  // useEffect(() => {
  //   if (!workspacesLoading && workspaces.length === 1) {
  //     router.push(`/workspaces/${workspaces[0].workspace_name}`);
  //   }
  // }, [workspaces, workspacesLoading, router]);

  if (authLoading || workspacesLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center p-4">
        <div className="bg-card border border-border rounded-lg p-8 max-w-md text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h2 className="text-xl font-semibold text-foreground mb-2">Failed to Load Workspaces</h2>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background p-8">
      <div className="max-w-6xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-foreground mb-2">Your Workspaces</h1>
          <p className="text-muted-foreground">
            Select a workspace to manage databases, instances, and data relationships.
          </p>
        </div>

        {workspaces.length === 0 ? (
          <div className="bg-card border border-border rounded-lg p-12 text-center">
            <Folder className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
            <h2 className="text-2xl font-semibold text-foreground mb-2">No Workspaces Found</h2>
            <p className="text-muted-foreground mb-6">
              You don't have access to any workspaces yet. Contact your administrator to get started.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {workspaces.map((workspace) => (
              <button
                key={workspace.workspace_id}
                onClick={() => router.push(`/workspaces/${workspace.workspace_name}`)}
                className="bg-card border border-border rounded-lg p-6 hover:border-primary hover:shadow-lg transition-all text-left group"
              >
                <div className="flex items-start justify-between mb-4">
                  <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center group-hover:bg-primary/20 transition-colors">
                    <Folder className="h-6 w-6 text-primary" />
                  </div>
                  <ArrowRight className="h-5 w-5 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all" />
                </div>
                <h3 className="text-xl font-semibold text-foreground mb-2">
                  {workspace.workspace_name}
                </h3>
                {workspace.workspace_description && (
                  <p className="text-sm text-muted-foreground line-clamp-2">
                    {workspace.workspace_description}
                  </p>
                )}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

