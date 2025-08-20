'use client';

import React, { createContext, useContext, useState, useEffect } from 'react';

export interface Workspace {
  id: string;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
}

interface WorkspaceContextType {
  currentWorkspace: Workspace | null;
  workspaces: Workspace[];
  setCurrentWorkspace: (workspace: Workspace) => void;
  isLoading: boolean;
  error: string | null;
  refreshWorkspaces: () => Promise<void>;
}

const WorkspaceContext = createContext<WorkspaceContextType | undefined>(undefined);

export function useWorkspace() {
  const context = useContext(WorkspaceContext);
  if (context === undefined) {
    throw new Error('useWorkspace must be used within a WorkspaceProvider');
  }
  return context;
}

interface WorkspaceProviderProps {
  children: React.ReactNode;
  tenantId: string;
}

export function WorkspaceProvider({ children, tenantId }: WorkspaceProviderProps) {
  const [currentWorkspace, setCurrentWorkspace] = useState<Workspace | null>(null);
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Load workspaces for the tenant
  const loadWorkspaces = async () => {
    try {
      setIsLoading(true);
      setError(null);
      
      // TODO: Replace with actual API call to fetch workspaces
      // For now, using mock data
      const mockWorkspaces: Workspace[] = [
        {
          id: 'ws-production',
          name: 'Production',
          description: 'Production environment workspace',
          createdAt: '2024-01-15T10:00:00Z',
          updatedAt: '2024-01-15T10:00:00Z',
        },
        {
          id: 'ws-staging',
          name: 'Staging',
          description: 'Staging environment for testing',
          createdAt: '2024-01-15T10:00:00Z',
          updatedAt: '2024-01-15T10:00:00Z',
        },
        {
          id: 'ws-development',
          name: 'Development',
          description: 'Development workspace for new features',
          createdAt: '2024-01-15T10:00:00Z',
          updatedAt: '2024-01-15T10:00:00Z',
        },
      ];

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setWorkspaces(mockWorkspaces);
      
      // Set default workspace if none is selected
      if (!currentWorkspace && mockWorkspaces.length > 0) {
        // Try to load from localStorage first
        const savedWorkspaceId = localStorage.getItem(`redb-workspace-${tenantId}`);
        const savedWorkspace = mockWorkspaces.find(ws => ws.id === savedWorkspaceId);
        
        setCurrentWorkspace(savedWorkspace || mockWorkspaces[0]);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load workspaces');
    } finally {
      setIsLoading(false);
    }
  };

  // Handle workspace selection
  const handleSetCurrentWorkspace = (workspace: Workspace) => {
    setCurrentWorkspace(workspace);
    // Save to localStorage for persistence
    localStorage.setItem(`redb-workspace-${tenantId}`, workspace.id);
  };

  // Load workspaces on mount or tenant change
  useEffect(() => {
    loadWorkspaces();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tenantId]);

  const value = {
    currentWorkspace,
    workspaces,
    setCurrentWorkspace: handleSetCurrentWorkspace,
    isLoading,
    error,
    refreshWorkspaces: loadWorkspaces,
  };

  return (
    <WorkspaceContext.Provider value={value}>
      {children}
    </WorkspaceContext.Provider>
  );
}
