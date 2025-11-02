'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useMCPTools } from '@/lib/hooks/useMCPTools';
import { useMCPResources } from '@/lib/hooks/useMCPResources';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { ArrowLeft, RefreshCw, Wrench, FileText, Server, Info } from 'lucide-react';
import { MCPToolsList } from '@/components/mcp/MCPToolsList';
import { MCPResourcesList } from '@/components/mcp/MCPResourcesList';
import { api } from '@/lib/api/endpoints';
import type { MCPServer } from '@/lib/api/types';

interface MCPServerDetailPageProps {
  params: Promise<{
    workspaceId: string;
    serverName: string;
  }>;
}

type Tab = 'overview' | 'tools' | 'resources';

export default function MCPServerDetailPage({ params }: MCPServerDetailPageProps) {
  const router = useRouter();
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [serverName, setServerName] = useState<string>('');
  const [server, setServer] = useState<MCPServer | null>(null);
  const [serverLoading, setServerLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<Tab>('overview');
  const { showToast } = useToast();
  
  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wsId, serverName: sName }) => {
      setWorkspaceId(wsId);
      setServerName(decodeURIComponent(sName));
    });
  }, [params]);

  // Fetch server details
  useEffect(() => {
    const fetchServer = async () => {
      if (!workspaceId || !serverName) return;
      
      try {
        setServerLoading(true);
        const response = await api.mcpServers.show(workspaceId, serverName);
        setServer(response.mcp_server);
      } catch (error) {
        console.error('Error fetching server:', error);
        showToast({
          type: 'error',
          title: 'Failed to load server details',
        });
      } finally {
        setServerLoading(false);
      }
    };

    fetchServer();
  }, [workspaceId, serverName]);

  const { mcpTools, isLoading: toolsLoading, refetch: refetchTools } = useMCPTools(workspaceId);
  const { mcpResources, isLoading: resourcesLoading, refetch: refetchResources } = useMCPResources(workspaceId);

  // Filter tools and resources for this server
  const serverTools = mcpTools.filter(t => t.mcp_server_ids?.includes(server?.mcp_server_id || ''));
  const serverResources = mcpResources.filter(r => r.mcp_server_ids?.includes(server?.mcp_server_id || ''));

  const handleRefresh = () => {
    if (activeTab === 'tools') {
      refetchTools();
    } else if (activeTab === 'resources') {
      refetchResources();
    }
    showToast({
      type: 'info',
      title: 'Refreshing...',
    });
  };

  if (!workspaceId || !serverName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (serverLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (!server) {
    return (
      <div className="space-y-6">
        <button
          onClick={() => router.back()}
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back to MCP Servers
        </button>
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <h3 className="text-xl font-semibold text-foreground mb-2">Server Not Found</h3>
          <p className="text-muted-foreground">
            The requested MCP server could not be found
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <button
          onClick={() => router.back()}
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground mb-4"
        >
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back to MCP Servers
        </button>
        
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="p-3 bg-primary/10 rounded-lg">
              <Server className="h-8 w-8 text-primary" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">{server.mcp_server_name}</h2>
              {server.mcp_server_description && (
                <p className="text-muted-foreground mt-1">
                  {server.mcp_server_description}
                </p>
              )}
            </div>
          </div>
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-4 py-2 bg-background border border-border text-foreground rounded-md hover:bg-accent transition-colors"
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-border">
        <div className="flex space-x-8">
          <button
            onClick={() => setActiveTab('overview')}
            className={`pb-4 px-1 border-b-2 font-medium text-sm transition-colors ${
              activeTab === 'overview'
                ? 'border-primary text-primary'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-border'
            }`}
          >
            <div className="flex items-center space-x-2">
              <Info className="h-4 w-4" />
              <span>Overview</span>
            </div>
          </button>
          <button
            onClick={() => setActiveTab('tools')}
            className={`pb-4 px-1 border-b-2 font-medium text-sm transition-colors ${
              activeTab === 'tools'
                ? 'border-primary text-primary'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-border'
            }`}
          >
            <div className="flex items-center space-x-2">
              <Wrench className="h-4 w-4" />
              <span>Tools</span>
              <span className="px-2 py-0.5 rounded-full text-xs bg-muted">
                {serverTools.length}
              </span>
            </div>
          </button>
          <button
            onClick={() => setActiveTab('resources')}
            className={`pb-4 px-1 border-b-2 font-medium text-sm transition-colors ${
              activeTab === 'resources'
                ? 'border-primary text-primary'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-border'
            }`}
          >
            <div className="flex items-center space-x-2">
              <FileText className="h-4 w-4" />
              <span>Resources</span>
              <span className="px-2 py-0.5 rounded-full text-xs bg-muted">
                {serverResources.length}
              </span>
            </div>
          </button>
        </div>
      </div>

      {/* Overview Tab */}
      {activeTab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-card border border-border rounded-lg p-6">
              <h3 className="text-lg font-semibold text-foreground mb-4">Server Information</h3>
              <dl className="space-y-3">
                <div>
                  <dt className="text-sm font-medium text-muted-foreground">Port</dt>
                  <dd className="text-base text-foreground font-mono">{server.mcp_server_port}</dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-muted-foreground">Status</dt>
                  <dd className="text-base text-foreground capitalize">{server.status || 'Unknown'}</dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-muted-foreground">Enabled</dt>
                  <dd className="text-base text-foreground">
                    {server.mcp_server_enabled ? 'Yes' : 'No'}
                  </dd>
                </div>
                {server.mcp_server_host_ids && server.mcp_server_host_ids.length > 0 && (
                  <div>
                    <dt className="text-sm font-medium text-muted-foreground">Host Nodes</dt>
                    <dd className="text-base text-foreground">
                      {server.mcp_server_host_ids.length} node{server.mcp_server_host_ids.length !== 1 ? 's' : ''}
                    </dd>
                  </div>
                )}
              </dl>
            </div>

            <div className="bg-card border border-border rounded-lg p-6">
              <h3 className="text-lg font-semibold text-foreground mb-4">Resources</h3>
              <dl className="space-y-3">
                <div>
                  <dt className="text-sm font-medium text-muted-foreground">Attached Tools</dt>
                  <dd className="text-2xl font-bold text-foreground">{serverTools.length}</dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-muted-foreground">Attached Resources</dt>
                  <dd className="text-2xl font-bold text-foreground">{serverResources.length}</dd>
                </div>
              </dl>
            </div>
          </div>
        </div>
      )}

      {/* Tools Tab */}
      {activeTab === 'tools' && (
        <div>
          <div className="mb-4">
            <h3 className="text-lg font-semibold text-foreground">Attached Tools</h3>
            <p className="text-sm text-muted-foreground">
              Tools expose operations and actions through this MCP server
            </p>
          </div>
          <MCPToolsList
            tools={serverTools}
            isLoading={toolsLoading}
          />
        </div>
      )}

      {/* Resources Tab */}
      {activeTab === 'resources' && (
        <div>
          <div className="mb-4">
            <h3 className="text-lg font-semibold text-foreground">Attached Resources</h3>
            <p className="text-sm text-muted-foreground">
              Resources provide read access to data through this MCP server
            </p>
          </div>
          <MCPResourcesList
            resources={serverResources}
            isLoading={resourcesLoading}
          />
        </div>
      )}
    </div>
  );
}

