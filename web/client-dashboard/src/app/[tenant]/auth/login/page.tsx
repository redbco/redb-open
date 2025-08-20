import { Suspense } from 'react';
import { LoginForm } from '@/components/auth/LoginForm';
import { TenantBranding } from '@/components/auth/TenantBranding';
import { LoadingSpinner } from '@/components/auth/LoadingSpinner';

interface LoginPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function LoginPage({ params }: LoginPageProps) {
  const { tenant } = await params;

  return (
    <div className="min-h-screen bg-gradient-to-br from-background to-muted flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        <div className="bg-card rounded-lg shadow-lg border p-8 pb-16">
          {/* Tenant Branding Section */}
          <Suspense fallback={<LoadingSpinner />}>
            <TenantBranding tenantId={tenant} />
          </Suspense>

          {/* Welcome Message */}
          <div className="text-center mb-8">
            <h1 className="text-2xl font-bold text-foreground mb-2">
              Welcome back
            </h1>
            <p className="text-muted-foreground">
              Sign in for {tenant}
            </p>
          </div>

          {/* Login Form */}
          <Suspense fallback={<LoadingSpinner />}>
            <LoginForm tenantId={tenant} />
          </Suspense>
        </div>

        {/* Additional Info */}
        <div className="mt-6 text-center">
          <p className="text-xs text-muted-foreground">
            Powered by{' '}
            <span className="font-semibold text-primary">reDB</span>
          </p>
        </div>
      </div>
    </div>
  );
}

// Generate metadata for the page
export async function generateMetadata({ params }: LoginPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Sign in to ${tenant} | reDB`,
    description: `Access your ${tenant} workspace on reDB platform`,
  };
}
