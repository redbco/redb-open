'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { ChevronDown, Search, Plus, Briefcase, Check } from 'lucide-react';

interface Workspace {
  id: string;
  name: string;
  description: string;
  environment: 'production' | 'staging' | 'development';
  lastAccessed: string;
}

interface WorkspaceSelectorProps {
  tenantId: string;
  currentWorkspaceId?: string;
  onWorkspaceSelect?: (workspaceId: string) => void;
  compact?: boolean;
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

export function WorkspaceSelector({ 
  tenantId, 
  currentWorkspaceId, 
  onWorkspaceSelect,
  compact = false 
}: WorkspaceSelectorProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const router = useRouter();

  const currentWorkspace = mockWorkspaces.find(w => w.id === currentWorkspaceId);
  
  const filteredWorkspaces = mockWorkspaces.filter(workspace =>
    workspace.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    workspace.description.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const handleWorkspaceSelect = (workspaceId: string) => {
    setIsOpen(false);
    setSearchQuery('');
    
    if (onWorkspaceSelect) {
      onWorkspaceSelect(workspaceId);
    } else {
      // Navigate to workspace
      router.push(`/${tenantId}/workspace/${workspaceId}`);
    }
  };

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

  if (compact) {
    return (
      <div className="relative">
        <button
          onClick={() => setIsOpen(!isOpen)}
          className="flex items-center space-x-2 px-3 py-2 text-sm bg-muted hover:bg-accent rounded-md transition-colors"
        >
          <Briefcase className="h-4 w-4" />
          <span className="truncate max-w-24">
            {currentWorkspace?.name || 'Select Workspace'}
          </span>
          <ChevronDown className="h-3 w-3" />
        </button>

        {isOpen && (
          <>
            <div 
              className="fixed inset-0 z-40" 
              onClick={() => setIsOpen(false)}
            />
            <div className="absolute top-full left-0 mt-1 w-72 bg-popover border border-border rounded-md shadow-lg z-50">
              <div className="p-3 border-b border-border">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <input
                    type="text"
                    placeholder="Search workspaces..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="w-full pl-9 pr-3 py-2 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                  />
                </div>
              </div>
              
              <div className="max-h-64 overflow-y-auto">
                {filteredWorkspaces.map((workspace) => (
                  <button
                    key={workspace.id}
                    onClick={() => handleWorkspaceSelect(workspace.id)}
                    className="w-full flex items-center justify-between px-3 py-3 hover:bg-accent transition-colors text-left"
                  >
                    <div className="flex items-center space-x-3 flex-1 min-w-0">
                      <div className="w-8 h-8 bg-primary/10 rounded-lg flex items-center justify-center flex-shrink-0">
                        <Briefcase className="h-4 w-4 text-primary" />
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="font-medium text-foreground truncate">{workspace.name}</p>
                        <p className="text-xs text-muted-foreground truncate">{workspace.description}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2 flex-shrink-0">
                      <span className={`px-2 py-1 rounded-full text-xs font-medium ${getEnvironmentColor(workspace.environment)}`}>
                        {workspace.environment}
                      </span>
                      {currentWorkspaceId === workspace.id && (
                        <Check className="h-4 w-4 text-primary" />
                      )}
                    </div>
                  </button>
                ))}
              </div>
              
              <div className="p-3 border-t border-border">
                <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-accent rounded-md transition-colors">
                  <Plus className="h-4 w-4" />
                  <span>Create New Workspace</span>
                </button>
              </div>
            </div>
          </>
        )}
      </div>
    );
  }

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between px-4 py-3 bg-card border border-border rounded-lg hover:bg-accent transition-colors"
      >
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
            <Briefcase className="h-5 w-5 text-primary" />
          </div>
          <div className="text-left">
            <p className="font-medium text-foreground">
              {currentWorkspace?.name || 'Select Workspace'}
            </p>
            <p className="text-sm text-muted-foreground">
              {currentWorkspace?.description || 'Choose a workspace to continue'}
            </p>
          </div>
        </div>
        <ChevronDown className="h-5 w-5 text-muted-foreground" />
      </button>

      {isOpen && (
        <>
          <div 
            className="fixed inset-0 z-40" 
            onClick={() => setIsOpen(false)}
          />
          <div className="absolute top-full left-0 right-0 mt-2 bg-popover border border-border rounded-lg shadow-lg z-50">
            <div className="p-4 border-b border-border">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Search workspaces..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-9 pr-3 py-2 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
            </div>
            
            <div className="max-h-80 overflow-y-auto">
              {filteredWorkspaces.map((workspace) => (
                <button
                  key={workspace.id}
                  onClick={() => handleWorkspaceSelect(workspace.id)}
                  className="w-full flex items-center justify-between px-4 py-4 hover:bg-accent transition-colors text-left border-b border-border last:border-b-0"
                >
                  <div className="flex items-center space-x-3 flex-1 min-w-0">
                    <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center flex-shrink-0">
                      <Briefcase className="h-5 w-5 text-primary" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="font-medium text-foreground">{workspace.name}</p>
                      <p className="text-sm text-muted-foreground">{workspace.description}</p>
                      <p className="text-xs text-muted-foreground mt-1">Last accessed: {workspace.lastAccessed}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-3 flex-shrink-0">
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${getEnvironmentColor(workspace.environment)}`}>
                      {workspace.environment}
                    </span>
                    {currentWorkspaceId === workspace.id && (
                      <Check className="h-4 w-4 text-primary" />
                    )}
                  </div>
                </button>
              ))}
            </div>
            
            <div className="p-4 border-t border-border">
              <button className="w-full flex items-center space-x-2 px-3 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-accent rounded-md transition-colors">
                <Plus className="h-4 w-4" />
                <span>Create New Workspace</span>
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}