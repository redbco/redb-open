'use client';

import Link from 'next/link';
import { Plus, Briefcase, Search } from 'lucide-react';
import { useState } from 'react';

interface Workspace {
  id: string;
  name: string;
  description: string;
  environment: 'production' | 'staging' | 'development';
  lastAccessed: string;
}

interface WorkspaceListMenuProps {
  tenantId: string;
}

// Mock workspace data - in real app, this would come from API
const mockWorkspaces: Workspace[] = [
  {
    id: 'production',
    name: 'Production',
    description: 'Live production environment',
    environment: 'production',
    lastAccessed: '2 hours ago'
  },
  {
    id: 'staging',
    name: 'Staging',
    description: 'Pre-production testing',
    environment: 'staging',
    lastAccessed: '1 day ago'
  },
  {
    id: 'development',
    name: 'Development',
    description: 'Development and testing',
    environment: 'development',
    lastAccessed: '3 days ago'
  },
  {
    id: 'analytics',
    name: 'Analytics',
    description: 'Data analytics workspace',
    environment: 'production',
    lastAccessed: '1 week ago'
  }
];

export function WorkspaceListMenu({ tenantId }: WorkspaceListMenuProps) {
  const [searchQuery, setSearchQuery] = useState('');

  const filteredWorkspaces = mockWorkspaces.filter(workspace =>
    workspace.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    workspace.description.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const getEnvironmentColor = (environment: string) => {
    switch (environment) {
      case 'production':
        return 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400';
      case 'staging':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400';
      case 'development':
        return 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400';
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center space-x-3 mb-4">
          <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
            <Briefcase className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h2 className="font-semibold text-foreground">Workspaces</h2>
            <p className="text-xs text-muted-foreground">Manage and access workspaces</p>
          </div>
        </div>

        {/* Search */}
        <div className="relative mb-4">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search workspaces..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-3 py-2 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
          />
        </div>

        {/* Create Workspace */}
        <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-primary hover:bg-primary/10 rounded-md transition-colors border border-primary/20">
          <Plus className="h-4 w-4" />
          <span>Create New Workspace</span>
        </button>
      </div>

      {/* Workspace List */}
      <div className="flex-1 overflow-y-auto">
        <div className="space-y-2">
          {filteredWorkspaces.map((workspace) => (
            <Link
              key={workspace.id}
              href={`/${tenantId}/workspace/${workspace.id}`}
              className="block p-4 border border-border rounded-lg hover:bg-accent transition-colors"
            >
              <div className="flex items-start justify-between mb-2">
                <div className="flex items-center space-x-3 flex-1 min-w-0">
                  <div className="w-8 h-8 bg-primary/10 rounded-lg flex items-center justify-center flex-shrink-0">
                    <Briefcase className="h-4 w-4 text-primary" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <h3 className="font-medium text-foreground truncate">{workspace.name}</h3>
                    <p className="text-sm text-muted-foreground truncate">{workspace.description}</p>
                  </div>
                </div>
                <span className={`px-2 py-1 rounded-full text-xs font-medium flex-shrink-0 ${getEnvironmentColor(workspace.environment)}`}>
                  {workspace.environment}
                </span>
              </div>
              <div className="flex items-center justify-between text-xs text-muted-foreground">
                <span>Last accessed: {workspace.lastAccessed}</span>
                <span className="text-primary hover:text-primary/80 font-medium">
                  →
                </span>
              </div>
            </Link>
          ))}
        </div>

        {filteredWorkspaces.length === 0 && (
          <div className="text-center py-8">
            <Briefcase className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
            <p className="text-muted-foreground">No workspaces found</p>
            <p className="text-sm text-muted-foreground">Try adjusting your search or create a new workspace</p>
          </div>
        )}
      </div>

      {/* Statistics */}
      <div className="mt-6 pt-4 border-t border-border">
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <p className="text-lg font-semibold text-foreground">{mockWorkspaces.length}</p>
            <p className="text-xs text-muted-foreground">Total</p>
          </div>
          <div>
            <p className="text-lg font-semibold text-foreground">
              {mockWorkspaces.filter(w => w.environment === 'production').length}
            </p>
            <p className="text-xs text-muted-foreground">Production</p>
          </div>
          <div>
            <p className="text-lg font-semibold text-foreground">
              {mockWorkspaces.filter(w => w.environment === 'development').length}
            </p>
            <p className="text-xs text-muted-foreground">Development</p>
          </div>
        </div>
      </div>
    </div>
  );
}
