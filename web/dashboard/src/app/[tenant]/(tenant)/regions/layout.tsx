import { TenantDashboardLayout } from '@/components/layout/TenantDashboardLayout';
import { MeshMenu } from '@/components/mesh/MeshMenu';

interface RegionsLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function RegionsLayout({ children, params }: RegionsLayoutProps) {
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
