import { TenantDashboardLayout } from '@/components/layout/TenantDashboardLayout';
import { MeshMenu } from '@/components/mesh/MeshMenu';

interface MeshLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function MeshLayout({ children, params }: MeshLayoutProps) {
  const { tenant } = await params;

  return (
    <TenantDashboardLayout 
      tenantId={tenant}
      asideContent={<MeshMenu tenantId={tenant} />}
    >
      {children}
    </TenantDashboardLayout>
  );
}
