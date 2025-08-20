import { TenantDashboardLayout } from '@/components/layout/TenantDashboardLayout';
import { WorkspaceListMenu } from '@/components/workspace/WorkspaceListMenu';

interface WorkspacesLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function WorkspacesLayout({ children, params }: WorkspacesLayoutProps) {
  const { tenant } = await params;

  return (
    <TenantDashboardLayout 
      tenantId={tenant}
      asideContent={<WorkspaceListMenu tenantId={tenant} />}
    >
      {children}
    </TenantDashboardLayout>
  );
}
