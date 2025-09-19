'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Network, Satellite, Anchor, Globe, Activity, Server, Settings } from 'lucide-react';

interface MeshMenuProps {
  tenantId: string;
}

// Mesh navigation items
const meshNavigationItems = [
  {
    name: 'Mesh Overview',
    href: '/mesh',
    icon: Network,
    description: 'Mesh topology and node status'
  },
  {
    name: 'Satellites',
    href: '/satellites',
    icon: Satellite,
    description: 'API-access and MCP server nodes'
  },
  {
    name: 'Anchors',
    href: '/anchors',
    icon: Anchor,
    description: 'Database-only access nodes'
  },
  {
    name: 'Regions',
    href: '/regions',
    icon: Globe,
    description: 'Physical locations and data centers'
  }
];

export function MeshMenu({ tenantId }: MeshMenuProps) {
  const pathname = usePathname();

  const isActiveRoute = (href: string) => {
    const fullPath = `/${tenantId}${href}`;
    return pathname === fullPath || pathname.startsWith(fullPath + '/');
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center space-x-3 mb-4">
          <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
            <Network className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h2 className="font-semibold text-foreground">Mesh Network</h2>
            <p className="text-xs text-muted-foreground">Network infrastructure management</p>
          </div>
        </div>
      </div>

      {/* Navigation Menu */}
      <div className="flex-1">
        <h3 className="font-medium text-foreground mb-3">Navigation</h3>
        <div className="space-y-1">
          {meshNavigationItems.map((item) => {
            const Icon = item.icon;
            const active = isActiveRoute(item.href);
            
            return (
              <Link
                key={item.name}
                href={`/${tenantId}${item.href}`}
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

      {/* Mesh Statistics */}
      <div className="grid grid-cols-2 gap-3 mt-6">
        <div className="bg-card border border-border rounded-lg p-4 text-center">
          <Server className="h-6 w-6 text-primary mx-auto mb-2" />
          <p className="text-lg font-semibold text-foreground">42</p>
          <p className="text-xs text-muted-foreground">Total Nodes</p>
        </div>
        <div className="bg-card border border-border rounded-lg p-4 text-center">
          <Activity className="h-6 w-6 text-green-600 dark:text-green-400 mx-auto mb-2" />
          <p className="text-lg font-semibold text-foreground">5</p>
          <p className="text-xs text-muted-foreground">Regions</p>
        </div>
      </div>

      {/* Node Types */}
      <div className="mt-6">
        <h3 className="font-medium text-foreground mb-3">Node Distribution</h3>
        <div className="space-y-2">
          {[
            { type: 'Satellites', count: 18, icon: Satellite, status: 'healthy' },
            { type: 'Anchors', count: 24, icon: Anchor, status: 'healthy' },
            { type: 'Total Regions', count: 5, icon: Globe, status: 'active' }
          ].map((nodeType, index) => (
            <div key={index} className="flex items-center justify-between p-2 border border-border rounded-md">
              <div className="flex items-center space-x-2">
                <nodeType.icon className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium text-foreground">{nodeType.type}</span>
              </div>
              <div className="flex items-center space-x-2">
                <span className="text-sm text-muted-foreground">{nodeType.count}</span>
                <div className={`w-2 h-2 rounded-full ${
                  nodeType.status === 'healthy' ? 'bg-green-500' : 'bg-yellow-500'
                }`}></div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Quick Actions */}
      <div className="mt-6 pt-4 border-t border-border">
        <div className="space-y-2">
          <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-foreground hover:bg-accent rounded-md transition-colors">
            <Activity className="h-4 w-4" />
            <span>Health Check</span>
          </button>
          <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-foreground hover:bg-accent rounded-md transition-colors">
            <Network className="h-4 w-4" />
            <span>Topology View</span>
          </button>
          <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-foreground hover:bg-accent rounded-md transition-colors">
            <Settings className="h-4 w-4" />
            <span>Mesh Settings</span>
          </button>
        </div>
      </div>
    </div>
  );
}