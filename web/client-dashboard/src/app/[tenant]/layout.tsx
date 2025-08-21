import { Metadata } from 'next';

interface TenantLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default async function TenantLayout({ children }: TenantLayoutProps) {
  // This layout is now just a pass-through for all tenant routes
  // Auth routes will render directly, dashboard routes will have their own layout
  return <>{children}</>;
}

export async function generateMetadata({ params }: TenantLayoutProps): Promise<Metadata> {
  const { tenant } = await params;
  
  return {
    title: `${tenant} Dashboard | reDB`,
    description: `${tenant} workspace dashboard on reDB platform`,
  };
}
