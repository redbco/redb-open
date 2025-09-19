'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { ArrowLeft, Briefcase, Database, GitBranch, Settings } from 'lucide-react';
import { WorkspaceSelector } from '@/components/workspace/WorkspaceSelector';
import { workspaceNavigationItems } from '@/components/layout/WorkspaceSidebar';
import { useWorkspace } from '@/lib/workspace';

interface WorkspaceMenuProps {
  tenantId: string;
  workspaceId: string;
}

export function WorkspaceMenu({ tenantId, workspaceId }: WorkspaceMenuProps) {
  const pathname = usePathname();
  const { currentWorkspace } = useWorkspace();

  const isActiveRoute = (href: string) => {
    const fullPath = `/${tenantId}/workspace/${workspaceId}${href}`;
    return pathname === fullPath || pathname.startsWith(fullPath + '/');
  };

  return (
    <div className="flex flex-col h-full">
      {/* Workspace Header */}
      <div className="mb-6">
        <div className="flex items-center space-x-3 mb-4">
          <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
            <Briefcase className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h2 className="font-semibold text-foreground">{currentWorkspace?.name || workspaceId}</h2>
            <p className="text-xs text-muted-foreground">Workspace Environment</p>
          </div>
        </div>

        {/* Back to Workspaces */}
        <Link
          href={`/${tenantId}/workspaces`}
          className="flex items-center space-x-2 px-3 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-accent rounded-md transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          <span>Back to workspaces</span>
        </Link>

        {/* Workspace Selector */}
        <div className="mt-4">
          <WorkspaceSelector 
            tenantId={tenantId} 
            currentWorkspaceId={workspaceId}
            compact={true}
          />
        </div>
      </div>

      {/* Navigation Menu */}
      <div className="flex-1">
        <h3 className="font-medium text-foreground mb-3">Navigation</h3>
        <div className="space-y-1">
          {workspaceNavigationItems.map((item) => {
            const Icon = item.icon;
            const active = isActiveRoute(item.href);
            
            return (
              <Link
                key={item.name}
                href={`/${tenantId}/workspace/${workspaceId}${item.href}`}
                className={`flex items-center space-x-3 px-3 py-2 rounded-md transition-colors ${
                  active
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:text-foreground hover:bg-accent'
                }`}
              >
                <Icon className={`h-4 w-4 flex-shrink-0 ${active ? 'text-primary-foreground' : ''}`} />
                <span className="text-sm font-medium">{item.name}</span>
              </Link>
            );
          })}
        </div>
      </div>

      {/* Quick Actions */}
      <div className="mt-6 pt-4 border-t border-border">
        <div className="space-y-2">
          <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-foreground hover:bg-accent rounded-md transition-colors">
            <Database className="h-4 w-4" />
            <span>Connect Database</span>
          </button>
          <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-foreground hover:bg-accent rounded-md transition-colors">
            <GitBranch className="h-4 w-4" />
            <span>Create Repository</span>
          </button>
          <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-foreground hover:bg-accent rounded-md transition-colors">
            <Settings className="h-4 w-4" />
            <span>Workspace Settings</span>
          </button>
        </div>
      </div>
    </div>
  );
}