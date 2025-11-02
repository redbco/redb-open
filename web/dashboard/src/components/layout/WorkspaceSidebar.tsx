'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { 
  LayoutDashboard, 
  Database,
  Settings, 
  ChevronLeft,
  ChevronRight,
  LogOut,
  User,
  Server,
  ArrowLeft,
  ArrowRightLeft,
  Link as LucideLink,
  GitBranch,
  Layers
} from 'lucide-react';
import { useAuth } from '@/lib/auth/auth-context';
import { useWorkspace } from '@/lib/hooks/useWorkspace';

interface WorkspaceSidebarProps {
  workspaceId: string;
  isOpen: boolean;
  onToggle: () => void;
}

interface NavItem {
  name: string;
  href: string;
  icon: React.ComponentType<{ className?: string }>;
  description: string;
}

const workspaceNavigationItems: NavItem[] = [
  {
    name: 'Dashboard',
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
    description: 'Schema repositories and version control'
  },
  {
    name: 'Environments',
    href: '/environments',
    icon: Layers,
    description: 'Deployment environments'
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

export function WorkspaceSidebar({ workspaceId, isOpen, onToggle }: WorkspaceSidebarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const { profile, logout } = useAuth();
  const { workspace } = useWorkspace(workspaceId);

  const handleLogout = async () => {
    await logout();
  };

  const isActiveRoute = (href: string) => {
    const fullPath = `/workspaces/${workspaceId}${href}`;
    return pathname === fullPath || pathname?.startsWith(fullPath + '/');
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
                {(workspace?.workspace_name || workspaceId).charAt(0).toUpperCase()}
              </span>
            </div>
            <div>
              <h2 className="font-semibold text-foreground text-sm">
                {workspace?.workspace_name || workspaceId}
              </h2>
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

      {/* Back to Workspaces */}
      {isOpen && (
        <div className="px-4 py-2 border-b border-border">
          <Link
            href="/workspaces"
            className="flex items-center space-x-2 px-3 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            <span>All Workspaces</span>
          </Link>
        </div>
      )}

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto p-4">
        <ul className="space-y-1">
          {workspaceNavigationItems.map((item) => {
            const Icon = item.icon;
            const active = isActiveRoute(item.href);
            
            return (
              <li key={item.name}>
                <Link
                  href={`/workspaces/${workspaceId}${item.href}`}
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
            <Link
              href="/profile"
              className="flex items-center space-x-3 px-3 py-2 rounded-md hover:bg-accent transition-colors"
            >
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
            </Link>
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
