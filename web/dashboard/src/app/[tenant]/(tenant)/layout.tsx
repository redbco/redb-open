import { TenantDashboardLayout } from '@/components/layout/TenantDashboardLayout';

interface TenantLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function TenantLayout({ children, params }: TenantLayoutProps) {
  const { tenant } = await params;

  return (
    <TenantDashboardLayout tenantId={tenant}>
      {children}
    </TenantDashboardLayout>
  );
}
