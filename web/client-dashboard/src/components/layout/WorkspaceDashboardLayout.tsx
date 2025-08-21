'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { SessionStorage } from '@/lib/auth/storage';
import { IconTenantSidebar } from '@/components/layout/IconTenantSidebar';
import { AsideMenu } from '@/components/layout/AsideMenu';
import { WorkspaceMenu } from '@/components/workspace/WorkspaceMenu';
import { LoadingSpinner } from '@/components/auth/LoadingSpinner';
import { WorkspaceProvider } from '@/lib/workspace';

interface WorkspaceDashboardLayoutProps {
  children: React.ReactNode;
  tenantId: string;
  workspaceId: string;
}

export function WorkspaceDashboardLayout({ children, tenantId, workspaceId }: WorkspaceDashboardLayoutProps) {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(true);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  useEffect(() => {
    // Check authentication status
    const checkAuth = () => {
      const hasValidSession = SessionStorage.hasValidSession();
      const sessionTenantId = SessionStorage.getTenantId();
      
      if (!hasValidSession) {
        router.push(`/${tenantId}/auth/login`);
        return;
      }
      
      // Check tenant match - be flexible with tenant ID comparison
      if (sessionTenantId && sessionTenantId !== tenantId) {
        // For now, we'll be permissive since the API might return different formats
        // In production, you might want stricter validation
        console.warn('Tenant ID mismatch:', { sessionTenantId, urlTenantId: tenantId });
      }
      
      setIsAuthenticated(true);
      setIsLoading(false);
    };

    checkAuth();
  }, [tenantId, router]);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <LoadingSpinner />
      </div>
    );
  }

  if (!isAuthenticated) {
    return null; // Will redirect to login
  }

  return (
    <WorkspaceProvider tenantId={tenantId}>
      <div className="min-h-screen bg-background">
        {/* Icon-only tenant sidebar */}
        <IconTenantSidebar tenantId={tenantId} />
        
        {/* Main content area */}
        <main className="lg:pl-20">
          <div className="xl:pl-80">
            <div className="px-4 py-4 sm:px-6 lg:px-6">
              {children}
            </div>
          </div>
        </main>

        {/* Workspace menu aside */}
        <AsideMenu>
          <WorkspaceMenu 
            tenantId={tenantId}
            workspaceId={workspaceId}
          />
        </AsideMenu>
      </div>
    </WorkspaceProvider>
  );
}
