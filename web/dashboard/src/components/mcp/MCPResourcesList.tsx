'use client';

import { MCPResource } from '@/lib/api/types';
import { FileText, Link2, Trash2 } from 'lucide-react';

interface MCPResourcesListProps {
  resources: MCPResource[];
  isLoading: boolean;
  onDetach?: (resourceId: string, serverName: string) => void;
}

export function MCPResourcesList({ resources, isLoading, onDetach }: MCPResourcesListProps) {
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

  if (resources.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-12 text-center">
        <FileText className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-xl font-semibold text-foreground mb-2">No Resources</h3>
        <p className="text-muted-foreground">
          No resources are attached to this MCP server
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {resources.map((resource) => (
        <div
          key={resource.mcp_resource_id}
          className="bg-card border border-border rounded-lg p-4 hover:border-primary/50 transition-all"
        >
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <div className="flex items-center space-x-2 mb-2">
                <FileText className="h-4 w-4 text-primary" />
                <h4 className="text-base font-semibold text-foreground">
                  {resource.mcp_resource_name}
                </h4>
                {resource.mcp_resource_mapping_name && (
                  <span className="px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400">
                    {resource.mcp_resource_mapping_name}
                  </span>
                )}
              </div>
              
              {resource.mcp_resource_description && (
                <p className="text-sm text-muted-foreground mb-3">
                  {resource.mcp_resource_description}
                </p>
              )}

              <div className="flex items-center space-x-4 text-xs text-muted-foreground">
                {resource.mcp_server_ids && resource.mcp_server_ids.length > 0 && (
                  <div className="flex items-center">
                    <Link2 className="h-3 w-3 mr-1" />
                    <span>{resource.mcp_server_ids.length} server{resource.mcp_server_ids.length !== 1 ? 's' : ''}</span>
                  </div>
                )}
                {resource.policy_ids && resource.policy_ids.length > 0 && (
                  <span>{resource.policy_ids.length} polic{resource.policy_ids.length !== 1 ? 'ies' : 'y'}</span>
                )}
              </div>
            </div>

            {onDetach && (
              <button
                onClick={() => {
                  if (confirm('Are you sure you want to detach this resource?')) {
                    // We'd need the server name here, but this is a placeholder
                    onDetach(resource.mcp_resource_id, 'server_name');
                  }
                }}
                className="p-2 text-muted-foreground hover:text-destructive hover:bg-destructive/10 rounded-md transition-colors"
                title="Detach resource"
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

