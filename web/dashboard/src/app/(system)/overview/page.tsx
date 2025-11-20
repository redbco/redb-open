'use client';

import { useRouter } from 'next/navigation';
import { useWorkspaces } from '@/lib/hooks/useWorkspace';
import { useRegions } from '@/lib/hooks/useRegions';
import { useMesh, useNodes } from '@/lib/hooks/useMesh';
import { useUsers } from '@/lib/hooks/useUsers';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { 
  Folder, 
  MapPin, 
  Network, 
  Users as UsersIcon, 
  ArrowRight,
  Activity,
  Server,
  Database,
  TrendingUp
} from 'lucide-react';

export default function OverviewPage() {
  const router = useRouter();
  const { workspaces, isLoading: workspacesLoading } = useWorkspaces();
  const { regions, isLoading: regionsLoading } = useRegions();
  const { mesh, isLoading: meshLoading } = useMesh();
  const { nodes, isLoading: nodesLoading } = useNodes();
  const { users, isLoading: usersLoading } = useUsers();

  const isLoading = workspacesLoading || regionsLoading || meshLoading || nodesLoading || usersLoading;

  const stats = [
    {
      name: 'Workspaces',
      value: workspaces?.length || 0,
      icon: Folder,
      color: 'text-blue-600 dark:text-blue-400',
      bgColor: 'bg-blue-100 dark:bg-blue-900/20',
      href: '/workspaces',
      description: 'Active workspaces'
    },
    {
      name: 'Regions',
      value: regions?.length || 0,
      icon: MapPin,
      color: 'text-green-600 dark:text-green-400',
      bgColor: 'bg-green-100 dark:bg-green-900/20',
      href: '/regions',
      description: 'Geographic regions'
    },
    {
      name: 'Mesh Nodes',
      value: nodes?.length || 0,
      icon: Network,
      color: 'text-purple-600 dark:text-purple-400',
      bgColor: 'bg-purple-100 dark:bg-purple-900/20',
      href: '/mesh',
      description: 'Active nodes'
    },
    {
      name: 'Users',
      value: users?.length || 0,
      icon: UsersIcon,
      color: 'text-orange-600 dark:text-orange-400',
      bgColor: 'bg-orange-100 dark:bg-orange-900/20',
      href: '/users',
      description: 'Total users'
    }
  ];

  const quickLinks = [
    {
      name: 'Manage Regions',
      description: 'Configure geographic regions for your infrastructure',
      icon: MapPin,
      href: '/regions',
      color: 'text-green-600 dark:text-green-400'
    },
    {
      name: 'View Mesh Network',
      description: 'Monitor your distributed mesh topology and node status',
      icon: Network,
      href: '/mesh',
      color: 'text-purple-600 dark:text-purple-400'
    },
    {
      name: 'Manage Users',
      description: 'Add, remove, and configure user accounts and permissions',
      icon: UsersIcon,
      href: '/users',
      color: 'text-orange-600 dark:text-orange-400'
    }
  ];

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-4xl font-bold text-foreground mb-2">System Overview</h1>
        <p className="text-muted-foreground">
          Monitor your infrastructure and manage system-wide resources
        </p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stats.map((stat) => {
          const Icon = stat.icon;
          return (
            <button
              key={stat.name}
              onClick={() => router.push(stat.href)}
              className="bg-card border border-border rounded-lg p-6 hover:border-primary hover:shadow-lg transition-all text-left group"
            >
              <div className="flex items-center justify-between mb-4">
                <div className={`w-12 h-12 ${stat.bgColor} rounded-lg flex items-center justify-center`}>
                  <Icon className={`h-6 w-6 ${stat.color}`} />
                </div>
                <ArrowRight className="h-5 w-5 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all" />
              </div>
              <div className="text-3xl font-bold text-foreground mb-1">
                {stat.value}
              </div>
              <div className="text-sm font-medium text-foreground mb-1">
                {stat.name}
              </div>
              <div className="text-xs text-muted-foreground">
                {stat.description}
              </div>
            </button>
          );
        })}
      </div>

      {/* System Health Overview */}
      {mesh && (
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center space-x-3">
              <Activity className="h-6 w-6 text-primary" />
              <h2 className="text-xl font-semibold text-foreground">System Health</h2>
            </div>
            <span className="px-3 py-1 bg-green-100 dark:bg-green-900/20 text-green-600 dark:text-green-400 text-sm font-medium rounded-full">
              Operational
            </span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="flex items-center space-x-3">
              <Server className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-sm text-muted-foreground">Mesh Network</p>
                <p className="text-lg font-semibold text-foreground">
                  {mesh.status || 'Active'}
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-3">
              <Network className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-sm text-muted-foreground">Connected Nodes</p>
                <p className="text-lg font-semibold text-foreground">
                  {nodes?.length || 0} / {mesh.node_count || nodes?.length || 0}
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-3">
              <TrendingUp className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="text-sm text-muted-foreground">System Status</p>
                <p className="text-lg font-semibold text-foreground">Healthy</p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Workspaces */}
      <div>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-2xl font-semibold text-foreground">Your Workspaces</h2>
          <button
            onClick={() => router.push('/workspaces')}
            className="text-sm text-primary hover:underline"
          >
            View all
          </button>
        </div>

        {workspaces && workspaces.length === 0 ? (
          <div className="bg-card border border-border rounded-lg p-12 text-center">
            <Folder className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-xl font-semibold text-foreground mb-2">No Workspaces Found</h3>
            <p className="text-muted-foreground">
              You don&apos;t have access to any workspaces yet. Contact your administrator to get started.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {workspaces?.slice(0, 6).map((workspace) => (
              <button
                key={workspace.workspace_id}
                onClick={() => router.push(`/workspaces/${workspace.workspace_name}/dashboard`)}
                className="bg-card border border-border rounded-lg p-6 hover:border-primary hover:shadow-lg transition-all text-left group"
              >
                <div className="flex items-start justify-between mb-4">
                  <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center group-hover:bg-primary/20 transition-colors">
                    <Folder className="h-6 w-6 text-primary" />
                  </div>
                  <ArrowRight className="h-5 w-5 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all" />
                </div>
                <h3 className="text-lg font-semibold text-foreground mb-2">
                  {workspace.workspace_name}
                </h3>
                {workspace.workspace_description && (
                  <p className="text-sm text-muted-foreground line-clamp-2">
                    {workspace.workspace_description}
                  </p>
                )}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Quick Links */}
      <div>
        <h2 className="text-2xl font-semibold text-foreground mb-4">Quick Access</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {quickLinks.map((link) => {
            const Icon = link.icon;
            return (
              <button
                key={link.name}
                onClick={() => router.push(link.href)}
                className="bg-card border border-border rounded-lg p-6 hover:border-primary hover:shadow-lg transition-all text-left group"
              >
                <Icon className={`h-8 w-8 ${link.color} mb-4`} />
                <h3 className="text-lg font-semibold text-foreground mb-2 group-hover:text-primary transition-colors">
                  {link.name}
                </h3>
                <p className="text-sm text-muted-foreground">
                  {link.description}
                </p>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

