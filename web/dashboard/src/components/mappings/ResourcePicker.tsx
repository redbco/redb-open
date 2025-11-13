'use client';

import { useState, useEffect } from 'react';
import { ChevronRight } from 'lucide-react';
import { ResourceType, ResourceSelection } from '@/lib/api/types';
import { ResourceTypeSelector } from './ResourceTypeSelector';
import { useDatabases, useMCPResources, useMCPTools, useWebhooks, useStreams } from '@/lib/hooks/useResources';
import { useDatabaseSchemaInfo } from '@/lib/hooks/useDatabaseSchemaInfo';
import { buildDatabaseURI, buildTableURI, buildMCPResourceURI, buildMCPToolURI, buildWebhookURI, buildStreamURI } from '@/lib/utils/uri-builder';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';

interface ResourcePickerProps {
  workspaceId: string;
  label: string;
  value: ResourceSelection | null;
  onChange: (selection: ResourceSelection | null) => void;
  placeholder?: string;
  allowedTypes?: ResourceType[];
  disabled?: boolean;
}

export function ResourcePicker({
  workspaceId,
  label,
  value,
  onChange,
  placeholder = 'Select a resource',
  allowedTypes,
  disabled = false,
}: ResourcePickerProps) {
  const [selectedType, setSelectedType] = useState<ResourceType | null>(value?.type || null);
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);

  // Fetch resources based on type
  const { databases, isLoading: loadingDatabases } = useDatabases(workspaceId);
  const { mcpResources, isLoading: loadingMCPResources } = useMCPResources(workspaceId);
  const { mcpTools, isLoading: loadingMCPTools } = useMCPTools(workspaceId);
  const { webhooks, isLoading: loadingWebhooks } = useWebhooks(workspaceId);
  const { streams, isLoading: loadingStreams } = useStreams(workspaceId);

  // Fetch schema for selected database (for table selection)
  const { schema, isLoading: loadingSchema } = useDatabaseSchemaInfo(
    workspaceId,
    selectedDatabase || ''
  );

  // Update selectedDatabase when value changes externally
  useEffect(() => {
    if (value?.type === 'database' || value?.type === 'table') {
      setSelectedDatabase(value.databaseName || null);
      if (value.type === 'table') {
        setSelectedTable(value.tableName || null);
      }
    }
  }, [value]);

  const handleTypeSelect = (type: ResourceType) => {
    setSelectedType(type);
    setSelectedDatabase(null);
    setSelectedTable(null);
    onChange(null);
  };

  const handleDatabaseSelect = (databaseId: string, databaseName: string) => {
    setSelectedDatabase(databaseName);
    setSelectedTable(null);

    if (selectedType === 'database') {
      onChange({
        type: 'database',
        resourceId: databaseId,
        resourceName: databaseName,
        databaseId,
        databaseName,
        uri: buildDatabaseURI(databaseId),
      });
    }
  };

  const handleTableSelect = (tableName: string) => {
    setSelectedTable(tableName);

    const database = databases.find((db) => db.database_name === selectedDatabase);
    if (!database) return;

    onChange({
      type: 'table',
      resourceId: `${database.database_id}/${tableName}`,
      resourceName: tableName,
      databaseId: database.database_id,
      databaseName: database.database_name,
      tableName,
      uri: buildTableURI(database.database_id, tableName),
    });
  };

  const handleMCPResourceSelect = (resourceId: string, resourceName: string) => {
    onChange({
      type: 'mcp-resource',
      resourceId,
      resourceName,
      uri: buildMCPResourceURI(resourceId),
    });
  };

  const handleMCPToolSelect = (toolId: string, toolName: string) => {
    onChange({
      type: 'mcp-tool',
      resourceId: toolId,
      resourceName: toolName,
      uri: buildMCPToolURI(toolId),
    });
  };

  const handleWebhookSelect = (webhookId: string, webhookName: string) => {
    onChange({
      type: 'webhook',
      resourceId: webhookId,
      resourceName: webhookName,
      uri: buildWebhookURI(webhookId),
    });
  };

  const handleStreamSelect = (streamId: string, streamName: string) => {
    onChange({
      type: 'stream',
      resourceId: streamId,
      resourceName: streamName,
      uri: buildStreamURI(streamId),
    });
  };

  const renderResourceList = () => {
    if (!selectedType) return null;

    // Database type
    if (selectedType === 'database') {
      if (loadingDatabases) {
        return <LoadingSpinner size="sm" />;
      }

      if (databases.length === 0) {
        return (
          <div className="text-sm text-muted-foreground p-4 text-center border border-border rounded-md">
            No databases available
          </div>
        );
      }

      return (
        <div className="space-y-2">
          <label className="block text-xs font-medium text-foreground">Select Database</label>
          <select
            className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            value={selectedDatabase || ''}
            onChange={(e) => {
              const db = databases.find((d) => d.database_name === e.target.value);
              if (db) handleDatabaseSelect(db.database_id, db.database_name);
            }}
            disabled={disabled}
          >
            <option value="">Select a database...</option>
            {databases.map((db) => (
              <option key={db.database_id} value={db.database_name}>
                {db.database_name}
              </option>
            ))}
          </select>
        </div>
      );
    }

    // Table type
    if (selectedType === 'table') {
      return (
        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium text-foreground mb-1.5">
              1. Select Database
            </label>
            {loadingDatabases ? (
              <LoadingSpinner size="sm" />
            ) : (
              <select
                className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                value={selectedDatabase || ''}
                onChange={(e) => {
                  const db = databases.find((d) => d.database_name === e.target.value);
                  if (db) handleDatabaseSelect(db.database_id, db.database_name);
                }}
                disabled={disabled}
              >
                <option value="">Select a database...</option>
                {databases.map((db) => (
                  <option key={db.database_id} value={db.database_name}>
                    {db.database_name}
                  </option>
                ))}
              </select>
            )}
          </div>

          {selectedDatabase && (
            <div>
              <label className="block text-xs font-medium text-foreground mb-1.5">
                2. Select Table
              </label>
              {loadingSchema ? (
                <LoadingSpinner size="sm" />
              ) : schema?.tables && schema.tables.length > 0 ? (
                <select
                  className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                  value={selectedTable || ''}
                  onChange={(e) => handleTableSelect(e.target.value)}
                  disabled={disabled}
                >
                  <option value="">Select a table...</option>
                  {schema.tables.map((table) => (
                    <option key={table.name} value={table.name}>
                      {table.name} ({table.columns?.length || 0} columns)
                    </option>
                  ))}
                </select>
              ) : (
                <div className="text-xs text-muted-foreground p-3 text-center border border-border rounded-md">
                  No tables available in this database
                </div>
              )}
            </div>
          )}
        </div>
      );
    }

    // MCP Resource type
    if (selectedType === 'mcp-resource') {
      if (loadingMCPResources) {
        return <LoadingSpinner size="sm" />;
      }

      if (mcpResources.length === 0) {
        return (
          <div className="text-sm text-muted-foreground p-4 text-center border border-border rounded-md">
            No MCP resources available
          </div>
        );
      }

      return (
        <div className="space-y-2">
          <label className="block text-xs font-medium text-foreground">Select MCP Resource</label>
          <select
            className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            value={value?.resourceId || ''}
            onChange={(e) => {
              const resource = mcpResources.find((r) => r.mcp_resource_id === e.target.value);
              if (resource) {
                handleMCPResourceSelect(resource.mcp_resource_id, resource.mcp_resource_name);
              }
            }}
            disabled={disabled}
          >
            <option value="">Select an MCP resource...</option>
            {mcpResources.map((resource) => (
              <option key={resource.mcp_resource_id} value={resource.mcp_resource_id}>
                {resource.mcp_resource_name}
              </option>
            ))}
          </select>
        </div>
      );
    }

    // MCP Tool type
    if (selectedType === 'mcp-tool') {
      if (loadingMCPTools) {
        return <LoadingSpinner size="sm" />;
      }

      if (mcpTools.length === 0) {
        return (
          <div className="text-sm text-muted-foreground p-4 text-center border border-border rounded-md">
            No MCP tools available
          </div>
        );
      }

      return (
        <div className="space-y-2">
          <label className="block text-xs font-medium text-foreground">Select MCP Tool</label>
          <select
            className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            value={value?.resourceId || ''}
            onChange={(e) => {
              const tool = mcpTools.find((t) => t.mcp_tool_id === e.target.value);
              if (tool) {
                handleMCPToolSelect(tool.mcp_tool_id, tool.mcp_tool_name);
              }
            }}
            disabled={disabled}
          >
            <option value="">Select an MCP tool...</option>
            {mcpTools.map((tool) => (
              <option key={tool.mcp_tool_id} value={tool.mcp_tool_id}>
                {tool.mcp_tool_name}
              </option>
            ))}
          </select>
        </div>
      );
    }

    // Webhook type
    if (selectedType === 'webhook') {
      if (loadingWebhooks) {
        return <LoadingSpinner size="sm" />;
      }

      return (
        <div className="text-sm text-muted-foreground p-4 text-center border border-border rounded-md">
          Webhooks are not yet implemented
        </div>
      );
    }

    // Stream type
    if (selectedType === 'stream') {
      if (loadingStreams) {
        return <LoadingSpinner size="sm" />;
      }

      return (
        <div className="text-sm text-muted-foreground p-4 text-center border border-border rounded-md">
          Streams are not yet implemented
        </div>
      );
    }

    return null;
  };

  return (
    <div className="space-y-3">
      <div className="p-3 border border-border rounded-lg bg-card space-y-3">
        <div>
          <label className="block text-xs font-medium text-muted-foreground mb-2">
            {label}
          </label>
          <ResourceTypeSelector
            onSelect={handleTypeSelect}
            selected={selectedType}
            disabled={disabled}
            allowedTypes={allowedTypes}
          />
        </div>

        {selectedType && (
          <div className="pt-3 border-t border-border">
            {renderResourceList()}
          </div>
        )}
      </div>

      {value && (
        <div className="text-xs">
          <span className="text-muted-foreground">Selected:</span>{' '}
          <span className="font-medium text-foreground">{value.resourceName}</span>
          {value.databaseName && value.tableName && (
            <span className="text-muted-foreground">
              {' '}
              ({value.databaseName}.{value.tableName})
            </span>
          )}
        </div>
      )}
    </div>
  );
}

