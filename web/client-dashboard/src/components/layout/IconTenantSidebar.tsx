'use client';

import Link from 'next/link';
import Image from 'next/image';
import { useRouter, usePathname } from 'next/navigation';
import { LogOut, User } from 'lucide-react';
import { SessionStorage } from '@/lib/auth/storage';
import { authAPI } from '@/lib/auth/api';
import { ThemeToggle } from '../theme/ThemeToggle';
import { 
  LayoutDashboard, 
  Network,
  Shield,
  Plug,
  Briefcase
} from 'lucide-react';

interface IconTenantSidebarProps {
  tenantId: string;
}

interface NavItem {
  name: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  description: string;
}

const tenantNavigationItems: NavItem[] = [
  {
    name: 'Overview',
    href: '/overview',
    icon: LayoutDashboard,
    description: 'Tenant overview and mesh status'
  },
  {
    name: 'Mesh',
    href: '/mesh',
    icon: Network,
    description: 'Mesh topology and node status (read-only)'
  },
  {
    name: 'Workspaces',
    href: '/workspaces',
    icon: Briefcase,
    description: 'Manage and access workspaces'
  },
  {
    name: 'Access Management',
    href: '/access',
    icon: Shield,
    description: 'Users, roles, permissions, and API tokens'
  },

  {
    name: 'Integrations',
    href: '/integrations',
    icon: Plug,
    description: 'External integrations and connections'
  }
];

export function IconTenantSidebar({ tenantId }: IconTenantSidebarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const user = SessionStorage.getUser();

  const handleLogout = async () => {
    try {
      const refreshToken = SessionStorage.getRefreshToken();
      if (refreshToken) {
        await authAPI.logout(refreshToken, tenantId);
      }
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      SessionStorage.clearSession();
      router.push(`/${tenantId}/auth/login`);
    }
  };

  const isActiveRoute = (href: string) => {
    const fullPath = `/${tenantId}${href}`;
    return pathname === fullPath || pathname.startsWith(fullPath + '/');
  };

  return (
    <div className="hidden lg:fixed lg:inset-y-0 lg:left-0 lg:z-50 lg:block lg:w-20 lg:overflow-y-auto lg:bg-card lg:border-r lg:border-border">
      {/* Tenant Logo/Avatar */}
      <div className="relative flex h-16 shrink-0 items-center justify-center">
        <div className="w-8 h-8 flex items-center justify-center">
          <Image src="/logo.svg" alt="reDB" width={32} height={32} />
        </div>
      </div>

      {/* Navigation Icons */}
      <nav className="relative mt-8">
        <ul role="list" className="flex flex-col items-center space-y-1 px-2">
          {tenantNavigationItems.map((item) => {
              const Icon = item.icon;
              const active = isActiveRoute(item.href);
              
              return (
                <li key={item.name}>
                  <Link
                    href={`/${tenantId}${item.href}`}
                    className={`group flex rounded-md p-3 text-sm font-semibold transition-colors ${
                      active
                        ? 'bg-primary text-primary-foreground'
                        : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
                    }`}
                    title={item.name}
                  >
                    <Icon className="h-6 w-6 shrink-0" />
                    <span className="sr-only">{item.name}</span>
                  </Link>
                </li>
              );
            })}
        </ul>
      </nav>

      {/* User Section at Bottom */}
      <div className="absolute bottom-0 left-0 right-0 p-2">
        <div className="flex flex-col items-center space-y-2">
          {/* Theme Toggle */}
          <ThemeToggle />

          {/* User Avatar */}
          <Link
            href={`/${tenantId}/profile`}
            className={`w-8 h-8 rounded-full flex items-center justify-center transition-colors ${
              isActiveRoute('/profile')
                ? 'bg-primary text-primary-foreground'
                : 'bg-muted text-muted-foreground hover:bg-accent hover:text-accent-foreground'
            }`}
            title={user?.email || 'User Profile'}
          >
            <User className="h-4 w-4" />
          </Link>
          
          {/* Logout Button */}
          <button
            onClick={handleLogout}
            className="p-2 rounded-md text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors"
            title="Sign out"
          >
            <LogOut className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
