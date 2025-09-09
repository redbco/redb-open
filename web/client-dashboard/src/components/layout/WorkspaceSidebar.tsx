'use client';

import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { 
  LayoutDashboard, 
  Database,
  GitBranch,
  Settings, 
  ChevronLeft,
  ChevronRight,
  LogOut,
  User,
  Server,
  Layers,
  ArrowLeft,
  Zap,
  ArrowRightLeft,
  Link as LucideLink
} from 'lucide-react';
import { SessionStorage } from '@/lib/auth/storage';
import { authAPI } from '@/lib/auth/api';
import { useWorkspace } from '@/lib/workspace';

interface WorkspaceSidebarProps {
  tenantId: string;
  workspaceId: string;
  isOpen: boolean;
  onToggle: () => void;
  currentPath: string;
}

interface NavItem {
  name: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  description: string;
}

export const workspaceNavigationItems: NavItem[] = [
  {
    name: 'Operations',
    href: '/dashboard',
    icon: LayoutDashboard,
    description: 'Workspace overview and health'
  },
  {
    name: 'Instances',
    href: '/instances',
    icon: Server,
    description: 'Database instances and connections'
  },
  {
    name: 'Databases',
    href: '/databases',
    icon: Database,
    description: 'Logical databases across instances'
  },
  {
    name: 'Repositories',
    href: '/repositories',
    icon: GitBranch,
    description: 'Schema version control with git-like features'
  },
  {
    name: 'Mappings',
    href: '/mappings',
    icon: ArrowRightLeft,
    description: 'Schema and table column mappings'
  },
  {
    name: 'Relationships',
    href: '/relationships',
    icon: LucideLink,
    description: 'Active data replication and migration'
  },
  {
    name: 'Jobs & Tasks',
    href: '/jobs',
    icon: Zap,
    description: 'Migrations, RAG processes, and tasks'
  },
  {
    name: 'Environments',
    href: '/environments',
    icon: Layers,
    description: 'Database environment classification'
  },
  {
    name: 'MCP Servers',
    href: '/mcp-servers',
    icon: Server,
    description: 'Model Context Protocol servers'
  },
  {
    name: 'Settings',
    href: '/settings',
    icon: Settings,
    description: 'Workspace settings and configuration'
  }
];

export function WorkspaceSidebar({ tenantId, workspaceId, isOpen, onToggle, currentPath }: WorkspaceSidebarProps) {
  const router = useRouter();
  const user = SessionStorage.getUser();
  const { currentWorkspace } = useWorkspace();

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
    const fullPath = `/${tenantId}/workspace/${workspaceId}${href}`;
    return currentPath === fullPath || currentPath.startsWith(fullPath + '/');
  };

  return (
    <div className={`fixed left-0 top-0 h-full bg-card border-r border-border transition-all duration-300 z-50 ${
      isOpen ? 'w-64' : 'w-16'
    }`}>
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-border">
        {isOpen && (
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 bg-primary rounded-lg flex items-center justify-center">
              <span className="text-primary-foreground font-bold text-sm">
                {(currentWorkspace?.name || workspaceId).charAt(0).toUpperCase()}
              </span>
            </div>
            <div>
              <h2 className="font-semibold text-foreground">{currentWorkspace?.name || workspaceId}</h2>
              <p className="text-xs text-muted-foreground">Workspace</p>
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

      {/* Back to Tenant */}
      {isOpen && (
        <div className="px-4 py-2 border-b border-border">
          <Link
            href={`/${tenantId}/overview`}
            className="flex items-center space-x-2 px-3 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            <span>Back to {tenantId}</span>
          </Link>
        </div>
      )}

      {/* Navigation */}
      <nav className="flex-1 p-4">
        <ul className="space-y-2">
          {workspaceNavigationItems.map((item) => {
            const Icon = item.icon;
            const active = isActiveRoute(item.href);
            
            return (
              <li key={item.name}>
                <Link
                  href={`/${tenantId}/workspace/${workspaceId}${item.href}`}
                  className={`flex items-center space-x-3 px-3 py-2 rounded-md transition-colors group ${
                    active
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:text-foreground hover:bg-accent'
                  }`}
                  title={!isOpen ? item.description : undefined}
                >
                  <Icon className={`h-5 w-5 flex-shrink-0 ${active ? 'text-primary-foreground' : ''}`} />
                  {isOpen && (
                    <div className="flex-1 min-w-0">
                      <span className="font-medium">{item.name}</span>
                      <p className={`text-xs truncate ${
                        active ? 'text-primary-foreground/80' : 'text-muted-foreground'
                      }`}>
                        {item.description}
                      </p>
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
        {isOpen && user && (
          <div className="mb-4">
            <div className="flex items-center space-x-3 px-3 py-2">
              <div className="w-8 h-8 bg-muted rounded-full flex items-center justify-center">
                <User className="h-4 w-4 text-muted-foreground" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-foreground truncate">
                  {user.name || user.email}
                </p>
                <p className="text-xs text-muted-foreground truncate">
                  {user.email}
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
          {isOpen && <span>Sign out</span>}
        </button>
      </div>
    </div>
  );
}
