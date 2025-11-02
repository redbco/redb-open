'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

interface WorkspacePageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function WorkspacePage({ params }: WorkspacePageProps) {
  const router = useRouter();

  useEffect(() => {
    params.then(({ workspaceId }) => {
      router.push(`/workspaces/${workspaceId}/dashboard`);
    });
  }, [params, router]);

  return null;
}

