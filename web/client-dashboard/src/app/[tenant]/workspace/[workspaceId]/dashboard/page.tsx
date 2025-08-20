import { Suspense } from 'react';
import { WorkspaceOverview } from '@/components/workspace/WorkspaceOverview';
import { LoadingSpinner } from '@/components/auth/LoadingSpinner';

interface DashboardPageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function DashboardPage({ params }: DashboardPageProps) {
  const { tenant, workspaceId } = await params;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-foreground">Workspace Operations</h2>
        <p className="text-muted-foreground mt-2">
          Real-time operational overview and health monitoring for {workspaceId} workspace.
        </p>
      </div>

      <Suspense fallback={<LoadingSpinner />}>
        <WorkspaceOverview tenantId={tenant} workspaceId={workspaceId} />
      </Suspense>
    </div>
  );
}

export async function generateMetadata({ params }: DashboardPageProps) {
  const { tenant, workspaceId } = await params;
  
  return {
    title: `Operations | ${workspaceId} | ${tenant} | reDB`,
    description: `Operational overview for ${workspaceId} workspace`,
  };
}
