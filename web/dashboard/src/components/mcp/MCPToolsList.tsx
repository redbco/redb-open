'use client';

import { MCPTool } from '@/lib/api/types';
import { Wrench, Link2, Trash2 } from 'lucide-react';

interface MCPToolsListProps {
  tools: MCPTool[];
  isLoading: boolean;
  onDetach?: (toolId: string, serverName: string) => void;
}

export function MCPToolsList({ tools, isLoading, onDetach }: MCPToolsListProps) {
  if (isLoading) {
    return (
      <div className="space-y-3">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="bg-card border border-border rounded-lg p-4 animate-pulse">
            <div className="h-5 bg-muted rounded w-1/3 mb-2"></div>
            <div className="h-4 bg-muted rounded w-2/3"></div>
          </div>
        ))}
      </div>
    );
  }

  if (tools.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-12 text-center">
        <Wrench className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-xl font-semibold text-foreground mb-2">No Tools</h3>
        <p className="text-muted-foreground">
          No tools are attached to this MCP server
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {tools.map((tool) => (
        <div
          key={tool.mcp_tool_id}
          className="bg-card border border-border rounded-lg p-4 hover:border-primary/50 transition-all"
        >
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <div className="flex items-center space-x-2 mb-2">
                <Wrench className="h-4 w-4 text-primary" />
                <h4 className="text-base font-semibold text-foreground">
                  {tool.mcp_tool_name}
                </h4>
                {tool.mcp_tool_mapping_name && (
                  <span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400">
                    {tool.mcp_tool_mapping_name}
                  </span>
                )}
              </div>
              
              {tool.mcp_tool_description && (
                <p className="text-sm text-muted-foreground mb-3">
                  {tool.mcp_tool_description}
                </p>
              )}

              <div className="flex items-center space-x-4 text-xs text-muted-foreground">
                {tool.mcp_server_ids && tool.mcp_server_ids.length > 0 && (
                  <div className="flex items-center">
                    <Link2 className="h-3 w-3 mr-1" />
                    <span>{tool.mcp_server_ids.length} server{tool.mcp_server_ids.length !== 1 ? 's' : ''}</span>
                  </div>
                )}
                {tool.policy_ids && tool.policy_ids.length > 0 && (
                  <span>{tool.policy_ids.length} polic{tool.policy_ids.length !== 1 ? 'ies' : 'y'}</span>
                )}
              </div>
            </div>

            {onDetach && (
              <button
                onClick={() => {
                  if (confirm('Are you sure you want to detach this tool?')) {
                    // We'd need the server name here, but this is a placeholder
                    onDetach(tool.mcp_tool_id, 'server_name');
                  }
                }}
                className="p-2 text-muted-foreground hover:text-destructive hover:bg-destructive/10 rounded-md transition-colors"
                title="Detach tool"
              >
                <Trash2 className="h-4 w-4" />
              </button>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

