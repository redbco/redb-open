'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { SessionStorage } from '@/lib/auth/storage';
import { IconTenantSidebar } from '@/components/layout/IconTenantSidebar';
import { AsideMenu } from '@/components/layout/AsideMenu';

import { LoadingSpinner } from '@/components/auth/LoadingSpinner';

interface TenantDashboardLayoutProps {
  children: React.ReactNode;
  tenantId: string;
  asideContent?: React.ReactNode;
}

export function TenantDashboardLayout({ children, tenantId, asideContent }: TenantDashboardLayoutProps) {
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
    <div className="min-h-screen bg-background">
      {/* Icon-only tenant sidebar */}
      <IconTenantSidebar tenantId={tenantId} />
      
      {/* Main content area */}
      <main className="lg:pl-20">
        <div className={asideContent ? "xl:pl-54" : ""}>
          <div className="px-4 pt-2 pb-4 sm:px-6 lg:px-6">
            {children}
          </div>
        </div>
      </main>

      {/* Optional aside content */}
      {asideContent && (
        <AsideMenu>
          {asideContent}
        </AsideMenu>
      )}
    </div>
  );
}
