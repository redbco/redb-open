'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Server, Plus, RefreshCw, Activity, CheckCircle, Power } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import type { MCPServer } from '@/lib/api/types';

interface MCPServersPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function MCPServersPage({ params }: MCPServersPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [mcpServers, setMcpServers] = useState<MCPServer[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const { showToast } = useToast();
  
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const fetchServers = async () => {
    if (!workspaceId) return;
    
    try {
      setIsLoading(true);
      const response = await api.mcpServers.list(workspaceId);
      setMcpServers(response.mcp_servers || []);
      setError(null);
    } catch (err) {
      console.error('[MCPServers] Error fetching servers:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch MCP servers'));
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    if (workspaceId) {
      fetchServers();
    }
  }, [workspaceId]);

  if (!workspaceId) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold text-foreground">MCP Servers</h2>
            <p className="text-muted-foreground mt-2">
              Model Context Protocol servers
            </p>
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load MCP Servers</h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
        </div>
      </div>
    );
  }

  const enabledServers = mcpServers.filter(s => s.mcp_server_enabled).length;
  const healthyServers = mcpServers.filter(s => s.status === 'healthy').length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">MCP Servers</h2>
          <p className="text-muted-foreground mt-2">
            Manage Model Context Protocol servers for resource access
          </p>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={() => {
              fetchServers();
              showToast({ type: 'info', title: 'Refreshing MCP servers...' });
            }}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => showToast({ type: 'info', title: 'Coming Soon', message: 'Add MCP server dialog will be available soon' })}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add Server
          </button>
        </div>
      </div>

      {/* Overview Metrics */}
      {!isLoading && mcpServers.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          {[
            {
              title: 'Total Servers',
              value: mcpServers.length.toString(),
              change: 'MCP instances',
              icon: Server,
              color: 'text-blue-600 dark:text-blue-400'
            },
            {
              title: 'Enabled',
              value: enabledServers.toString(),
              change: 'Active servers',
              icon: Power,
              color: 'text-green-600 dark:text-green-400'
            },
            {
              title: 'Healthy',
              value: healthyServers.toString(),
              change: 'Running smoothly',
              icon: CheckCircle,
              color: 'text-green-600 dark:text-green-400'
            },
            {
              title: 'Resources',
              value: '-',
              change: 'Available resources',
              icon: Activity,
              color: 'text-purple-600 dark:text-purple-400'
            }
          ].map((metric, index) => (
            <div key={index} className="bg-card border border-border rounded-lg p-6">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-muted-foreground">{metric.title}</p>
                  <p className="text-2xl font-bold text-foreground mt-1">{metric.value}</p>
                  <p className="text-sm text-muted-foreground mt-1">{metric.change}</p>
                </div>
                <div className={`w-12 h-12 rounded-lg bg-muted/50 flex items-center justify-center ${metric.color}`}>
                  <metric.icon className="h-6 w-6" />
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Server List */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {[...Array(2)].map((_, i) => (
            <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
              <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
              <div className="h-4 bg-muted rounded w-full mb-2"></div>
              <div className="h-4 bg-muted rounded w-2/3"></div>
            </div>
          ))}
        </div>
      ) : mcpServers.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Server className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No MCP Servers</h3>
          <p className="text-muted-foreground mb-6">
            Add your first MCP server to start exposing resources via Model Context Protocol
          </p>
          <button
            onClick={() => showToast({ type: 'info', title: 'Coming Soon', message: 'Add MCP server dialog will be available soon' })}
            className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-5 w-5 mr-2" />
            Add Server
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {mcpServers.map((server) => (
            <Link
              key={server.mcp_server_id}
              href={`/workspaces/${workspaceId}/mcp-servers/${encodeURIComponent(server.mcp_server_name)}`}
              className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all cursor-pointer"
            >
              <div className="flex items-start justify-between mb-4">
                <div>
                  <div className="flex items-center space-x-3 mb-2">
                    <Server className="h-5 w-5 text-blue-600 dark:text-blue-400" />
                    <h3 className="text-lg font-semibold text-foreground">{server.mcp_server_name}</h3>
                  </div>
                  {server.mcp_server_description && (
                    <p className="text-sm text-muted-foreground mb-3">{server.mcp_server_description}</p>
                  )}
                </div>
                <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                  server.mcp_server_enabled
                    ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                    : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
                }`}>
                  {server.mcp_server_enabled ? 'Enabled' : 'Disabled'}
                </span>
              </div>
              
              <div className="space-y-2 text-sm">
                <div className="flex items-center justify-between">
                  <span className="text-muted-foreground">Port:</span>
                  <span className="text-foreground font-mono">{server.mcp_server_port}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-muted-foreground">Status:</span>
                  <span className="text-foreground capitalize">{server.status}</span>
                </div>
                {server.mcp_server_host_ids && server.mcp_server_host_ids.length > 0 && (
                  <div className="flex items-center justify-between">
                    <span className="text-muted-foreground">Nodes:</span>
                    <span className="text-foreground">{server.mcp_server_host_ids.length}</span>
                  </div>
                )}
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

