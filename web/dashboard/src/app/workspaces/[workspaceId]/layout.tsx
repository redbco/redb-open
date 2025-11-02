import { WorkspaceDashboardLayout } from '@/components/layout/WorkspaceDashboardLayout';

interface WorkspaceLayoutWrapperProps {
  children: React.ReactNode;
  params: Promise<{
    workspaceId: string;
  }>;
}

export default async function WorkspaceLayoutWrapper({ 
  children, 
  params 
}: WorkspaceLayoutWrapperProps) {
  const { workspaceId } = await params;

  return (
    <WorkspaceDashboardLayout workspaceId={workspaceId}>
      {children}
    </WorkspaceDashboardLayout>
  );
}

