'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/lib/auth/auth-context';
import { WorkspaceSidebar } from '@/components/layout/WorkspaceSidebar';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ThemeToggle } from '@/components/theme/ThemeToggle';

interface WorkspaceDashboardLayoutProps {
  children: React.ReactNode;
  workspaceId: string;
}

export function WorkspaceDashboardLayout({ children, workspaceId }: WorkspaceDashboardLayoutProps) {
  const router = useRouter();
  const { isAuthenticated, isLoading } = useAuth();
  const [sidebarOpen, setSidebarOpen] = useState(true);

  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      router.push('/auth/login');
    }
  }, [isAuthenticated, isLoading, router]);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (!isAuthenticated) {
    return null; // Will redirect to login
  }

  return (
      <div className="min-h-screen bg-background">
      {/* Sidebar */}
      <WorkspaceSidebar 
        workspaceId={workspaceId}
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen(!sidebarOpen)}
      />
        
        {/* Main content area */}
      <main className={`transition-all duration-300 ${sidebarOpen ? 'lg:ml-64' : 'lg:ml-16'}`}>
            <div className="px-4 py-4 sm:px-6 lg:px-6">
          {/* Top bar */}
          <div className="flex items-center justify-end mb-6">
            <ThemeToggle />
          </div>
          
          {/* Page content */}
              {children}
          </div>
        </main>
      </div>
  );
}
