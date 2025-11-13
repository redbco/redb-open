'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { 
  LayoutDashboard, 
  Folder,
  MapPin,
  Network,
  Users as UsersIcon,
  User,
  ChevronLeft,
  ChevronRight,
  LogOut,
  Building2
} from 'lucide-react';
import { useAuth } from '@/lib/auth/auth-context';
import { useWorkspaces } from '@/lib/hooks/useWorkspace';
import { useState } from 'react';

interface SystemSidebarProps {
  isOpen: boolean;
  onToggle: () => void;
}

interface NavItem {
  name: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  description: string;
}

const systemNavigationItems: NavItem[] = [
  {
    name: 'Overview',
    href: '/overview',
    icon: LayoutDashboard,
    description: 'System overview and statistics'
  },
  {
    name: 'Workspaces',
    href: '/workspaces',
    icon: Folder,
    description: 'Manage workspaces'
  },
  {
    name: 'Regions',
    href: '/regions',
    icon: MapPin,
    description: 'Geographic regions'
  },
  {
    name: 'Mesh',
    href: '/mesh',
    icon: Network,
    description: 'Mesh network topology'
  },
  {
    name: 'Users',
    href: '/users',
    icon: UsersIcon,
    description: 'User management'
  },
  {
    name: 'Profile',
    href: '/profile',
    icon: User,
    description: 'Your profile settings'
  }
];

export function SystemSidebar({ isOpen, onToggle }: SystemSidebarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const { profile, logout } = useAuth();
  const { workspaces } = useWorkspaces();
  const [showWorkspaceMenu, setShowWorkspaceMenu] = useState(false);

  const handleLogout = async () => {
    await logout();
  };

  const isActiveRoute = (href: string) => {
    return pathname === href || pathname?.startsWith(href + '/');
  };

  return (
    <div className={`fixed left-0 top-0 h-full bg-card border-r border-border transition-all duration-300 z-50 ${
      isOpen ? 'w-64' : 'w-16'
    }`}>
      {/* Header */}
      <div className={`flex items-center border-b border-border p-4 ${
        isOpen ? 'justify-between' : 'justify-center'
      }`}>
        {isOpen && (
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 bg-primary rounded-lg flex items-center justify-center">
              <Building2 className="h-5 w-5 text-primary-foreground" />
            </div>
            <div>
              <h2 className="font-semibold text-foreground text-sm">
                reDB System
              </h2>
              <p className="text-xs text-muted-foreground">Administration</p>
            </div>
          </div>
        )}
        <button
          onClick={onToggle}
          className="p-1 rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
        >
          {isOpen ? (
            <ChevronLeft className="h-4 w-4" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
        </button>
      </div>

      {/* Quick Workspace Access */}
      {isOpen && workspaces && workspaces.length > 0 && (
        <div className="px-4 py-2 border-b border-border">
          <div className="relative">
            <button
              onClick={() => setShowWorkspaceMenu(!showWorkspaceMenu)}
              className="w-full flex items-center justify-between px-3 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-accent rounded-md transition-colors"
            >
              <span className="flex items-center space-x-2">
                <Folder className="h-4 w-4" />
                <span>Go to Workspace</span>
              </span>
              <ChevronRight className={`h-4 w-4 transition-transform ${showWorkspaceMenu ? 'rotate-90' : ''}`} />
            </button>
            
            {showWorkspaceMenu && (
              <div className="absolute left-0 right-0 mt-1 bg-card border border-border rounded-md shadow-lg max-h-64 overflow-y-auto z-10">
                {workspaces.map((workspace) => (
                  <Link
                    key={workspace.workspace_id}
                    href={`/workspaces/${workspace.workspace_name}/dashboard`}
                    className="block px-3 py-2 text-sm text-foreground hover:bg-accent transition-colors"
                    onClick={() => setShowWorkspaceMenu(false)}
                  >
                    {workspace.workspace_name}
                  </Link>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto p-4">
        <ul className="space-y-1">
          {systemNavigationItems.map((item) => {
            const Icon = item.icon;
            const active = isActiveRoute(item.href);
            
            return (
              <li key={item.name}>
                <Link
                  href={item.href}
                  className={`flex items-center rounded-md transition-colors group ${
                    isOpen ? 'space-x-3 px-3' : 'justify-center px-0'
                  } py-2 ${
                    active
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:text-foreground hover:bg-accent'
                  }`}
                  title={!isOpen ? item.description : undefined}
                >
                  <Icon className={`h-5 w-5 flex-shrink-0 ${active ? 'text-primary-foreground' : ''}`} />
                  {isOpen && (
                    <div className="flex-1 min-w-0">
                      <span className="text-sm font-medium">{item.name}</span>
                      {!active && (
                        <p className="text-xs truncate text-muted-foreground">
                          {item.description}
                        </p>
                      )}
                    </div>
                  )}
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>

      {/* User Section */}
      <div className="border-t border-border p-4">
        {isOpen && profile && (
          <div className="mb-4">
            <div className="flex items-center space-x-3 px-3 py-2">
              <div className="w-8 h-8 bg-muted rounded-full flex items-center justify-center">
                <User className="h-4 w-4 text-muted-foreground" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-foreground truncate">
                  {profile.first_name && profile.last_name 
                    ? `${profile.first_name} ${profile.last_name}`
                    : profile.username}
                </p>
                <p className="text-xs text-muted-foreground truncate">
                  {profile.email}
                </p>
              </div>
            </div>
          </div>
        )}
        
        <button
          onClick={handleLogout}
          className={`flex items-center space-x-3 px-3 py-2 rounded-md transition-colors text-muted-foreground hover:text-foreground hover:bg-accent w-full ${
            !isOpen ? 'justify-center' : ''
          }`}
          title={!isOpen ? 'Sign out' : undefined}
        >
          <LogOut className="h-5 w-5 flex-shrink-0" />
          {isOpen && <span className="text-sm">Sign out</span>}
        </button>
      </div>
    </div>
  );
}

