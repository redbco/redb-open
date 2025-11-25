'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Server, Plus, RefreshCw, Activity, CheckCircle, Power, AlertCircle } from 'lucide-react';
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
  const disabledServers = mcpServers.length - enabledServers;
  const healthyServers = mcpServers.filter(s => s.status === 'healthy').length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between bg-gradient-to-r from-violet-50/50 to-transparent dark:from-violet-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div>
          <div className="flex items-center gap-3">
            <Server className="w-8 h-8 text-violet-600 dark:text-violet-400" />
            <h2 className="text-3xl font-bold text-foreground">MCP Servers</h2>
          </div>
          <div className="flex items-center gap-2 mt-2">
            <p className="text-muted-foreground">
              Manage Model Context Protocol servers for resource access
            </p>
            {!isLoading && mcpServers.length > 0 && (
              <>
                <span className="text-muted-foreground/30">•</span>
                {disabledServers > 0 ? (
                  <div className="flex items-center gap-1.5 text-rose-600 dark:text-rose-400 text-sm font-medium animate-in fade-in duration-300">
                    <AlertCircle className="w-4 h-4" />
                    {disabledServers} disabled
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5 text-emerald-600 dark:text-emerald-400 text-sm font-medium animate-in fade-in duration-300">
                    <CheckCircle className="w-4 h-4" />
                    All servers enabled
                  </div>
                )}
              </>
            )}
          </div>
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
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {mcpServers.map((server) => (
            <Link
              key={server.mcp_server_id}
              href={`/workspaces/${workspaceId}/mcp-servers/${encodeURIComponent(server.mcp_server_name)}`}
              className="group bg-card border border-border rounded-xl p-5 hover:shadow-lg hover:border-primary/20 transition-all duration-200 flex flex-col h-full cursor-pointer"
            >
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-center space-x-4">
                  <div className="w-12 h-12 bg-gradient-to-br from-violet-50 to-violet-100 dark:from-violet-900/20 dark:to-violet-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
                    <Server className="h-6 w-6 text-violet-600 dark:text-violet-400" />
                  </div>
                  <div>
                    <h3 className="font-bold text-lg text-foreground group-hover:text-primary transition-colors">{server.mcp_server_name}</h3>
                    <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                      <span className="font-medium">Port {server.mcp_server_port}</span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  <div className="relative flex h-2.5 w-2.5">
                    <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${
                      server.mcp_server_enabled ? 'bg-emerald-500' : 'bg-slate-500'
                    }`}></span>
                    <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${
                      server.mcp_server_enabled ? 'bg-emerald-500' : 'bg-slate-500'
                    }`}></span>
                  </div>
                  <span className="text-xs font-medium text-muted-foreground">
                    {server.mcp_server_enabled ? 'Enabled' : 'Disabled'}
                  </span>
                </div>
              </div>

              <div className="flex-grow">
                <p className="text-sm text-muted-foreground mb-4 line-clamp-2 min-h-[2.5rem]">
                  {server.mcp_server_description || <span className="text-muted-foreground/60 italic">No description provided</span>}
                </p>

                <div className="grid grid-cols-2 gap-y-3 gap-x-4">
                  <div className="flex flex-col gap-1">
                    <span className="text-xs text-muted-foreground flex items-center gap-1.5">
                      <Activity className="h-3.5 w-3.5" />
                      Status
                    </span>
                    <span className="text-sm font-medium text-foreground capitalize">
                      {server.status}
                    </span>
                  </div>

                  {server.mcp_server_host_ids && server.mcp_server_host_ids.length > 0 && (
                    <div className="flex flex-col gap-1">
                      <span className="text-xs text-muted-foreground flex items-center gap-1.5">
                        <Server className="h-3.5 w-3.5" />
                        Nodes
                      </span>
                      <span className="text-sm font-medium text-foreground">
                        {server.mcp_server_host_ids.length}
                      </span>
                    </div>
                  )}
                </div>
              </div>

              <div className="mt-auto pt-4 border-t border-border/50 flex items-center justify-between">
                <span className="text-xs text-muted-foreground">
                  Click to view details
                </span>
                <div className="flex items-center gap-1">
                  {server.mcp_server_enabled && (
                    <Power className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                  )}
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

