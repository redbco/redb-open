import { TenantDashboardLayout } from '@/components/layout/TenantDashboardLayout';
import { MeshMenu } from '@/components/mesh/MeshMenu';

interface AnchorsLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function AnchorsLayout({ children, params }: AnchorsLayoutProps) {
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
