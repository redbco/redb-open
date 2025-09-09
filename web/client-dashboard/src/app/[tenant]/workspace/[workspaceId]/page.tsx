import { redirect } from 'next/navigation';

interface WorkspacePageProps {
  params: Promise<{
    tenant: string;
    workspaceId: string;
  }>;
}

export default async function WorkspacePage({ params }: WorkspacePageProps) {
  const { tenant, workspaceId } = await params;
  
  // Redirect to the dashboard page
  redirect(`/${tenant}/workspace/${workspaceId}/dashboard`);
}
