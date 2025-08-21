import { redirect } from 'next/navigation';

interface TenantPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function TenantPage({ params }: TenantPageProps) {
  const { tenant } = await params;
  
  // Redirect to tenant overview by default
  redirect(`/${tenant}/overview`);
}
