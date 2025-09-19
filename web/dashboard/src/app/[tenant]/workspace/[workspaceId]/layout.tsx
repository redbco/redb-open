import { WorkspaceDashboardLayout } from '@/components/layout/WorkspaceDashboardLayout';

interface WorkspaceLayoutWrapperProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function WorkspaceLayoutWrapper({ 
  children, 
  params 
}: WorkspaceLayoutWrapperProps) {
  const { tenant, workspaceId } = await params;

  return (
    <WorkspaceDashboardLayout tenantId={tenant} workspaceId={workspaceId}>
      {children}
    </WorkspaceDashboardLayout>
  );
}
