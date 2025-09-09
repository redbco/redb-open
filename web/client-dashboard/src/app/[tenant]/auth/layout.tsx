import { Metadata } from 'next';
import { ThemeToggle } from '@/components/theme';

interface AuthLayoutProps {
  children: React.ReactNode;
  params: Promise<{
    tenant: string;
  }>;
}

export default function AuthLayout({ children }: AuthLayoutProps) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-background to-muted">
      {/* Theme toggle in top-right corner */}
      <div className="absolute top-4 right-4 z-10">
        <ThemeToggle />
      </div>
      {children}
    </div>
  );
}

export async function generateMetadata({ params }: AuthLayoutProps): Promise<Metadata> {
  const { tenant } = await params;
  
  return {
    title: `Authentication | ${tenant} | reDB`,
    description: `Secure authentication for ${tenant} workspace`,
  };
}
