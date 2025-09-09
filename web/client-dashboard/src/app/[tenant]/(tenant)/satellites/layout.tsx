import { TenantDashboardLayout } from '@/components/layout/TenantDashboardLayout';
import { MeshMenu } from '@/components/mesh/MeshMenu';

interface SatellitesLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function SatellitesLayout({ children, params }: SatellitesLayoutProps) {
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
