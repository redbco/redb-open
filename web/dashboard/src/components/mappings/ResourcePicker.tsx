/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import { useState, useEffect } from 'react';
import { ResourceType, ResourceSelection } from '@/lib/api/types';
import { ResourceTypeSelector } from './ResourceTypeSelector';
import { useDatabases, useMCPResources, useMCPTools, useWebhooks, useStreams } from '@/lib/hooks/useResources';
import { useDatabaseSchemaInfo } from '@/lib/hooks/useDatabaseSchemaInfo';
import { useDatabaseCapabilities } from '@/lib/hooks/useDatabaseCapabilities';
import { detectContainerType } from '@/lib/utils/container-type-detector';
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
  isTargetSelector?: boolean; // Indicates this is selecting a target (allows MCP resources/tools)
  enableStatsFiltering?: boolean; // Show stats and disable types with no containers
}

export function ResourcePicker({
  workspaceId,
  label,
  value,
  onChange,
  placeholder = 'Select a resource',
  allowedTypes,
  disabled = false,
  isTargetSelector = false,
  enableStatsFiltering = false,
}: ResourcePickerProps) {
  const [selectedType, setSelectedType] = useState<ResourceType | null>(value?.type || null);
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [selectedDatabaseType, setSelectedDatabaseType] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [selectedIntegration, setSelectedIntegration] = useState<string | null>(null);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);

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

  // Fetch database capabilities for paradigm detection
  const { capabilities } = useDatabaseCapabilities(selectedDatabaseType || undefined);

  // Update selectedDatabase when value changes externally
  useEffect(() => {
    if (value?.type === 'database' || value?.type === 'table' || value?.containerType) {
      setSelectedDatabase(value.databaseName || null);
      setSelectedDatabaseType(value.databaseType || null);
      if (value.type === 'table' || value.containerType) {
        setSelectedTable(value.tableName || value.containerName || null);
      }
    } else if (value?.type === 'stream') {
      // Extract integration and topic from stream resource
      const streamData = value as any;
      setSelectedIntegration(streamData.integrationName || null);
      setSelectedTopic(streamData.topicName || null);
    }
  }, [value]);

  const handleTypeSelect = (type: ResourceType) => {
    setSelectedType(type);
    setSelectedDatabase(null);
    setSelectedTable(null);
    setSelectedIntegration(null);
    setSelectedTopic(null);
    onChange(null);
  };

  const handleDatabaseSelect = (databaseId: string, databaseName: string, dbType?: string) => {
    setSelectedDatabase(databaseName);
    setSelectedDatabaseType(dbType || null);
    setSelectedTable(null);

    if (selectedType === 'database') {
      onChange({
        type: 'database',
        resourceId: databaseId,
        resourceName: databaseName,
        databaseId,
        databaseName,
        databaseType: dbType,
        uri: buildDatabaseURI(databaseId),
      });
    }
  };

  const handleTableSelect = (tableName: string) => {
    setSelectedTable(tableName);

    const database = databases.find((db) => db.database_name === selectedDatabase);
    if (!database) return;

    // Detect container type based on database paradigm
    const dbType = database.database_type || selectedDatabaseType;
    const paradigm = capabilities?.paradigms?.[0];
    const containerType = detectContainerType(dbType || undefined, paradigm);

    // For backward compatibility, still set type to 'table', but also include containerType
    onChange({
      type: containerType as ResourceType, // Use the detected container type
      resourceId: `${database.database_id}/${tableName}`,
      resourceName: tableName,
      databaseId: database.database_id,
      databaseName: database.database_name,
      databaseType: dbType || undefined,
      databaseParadigm: paradigm,
      tableName,
      containerName: tableName,
      containerType,
      uri: buildTableURI(database.database_id, tableName),
    });
  };

  const handleMCPResourceSelect = (resourceId: string, resourceName: string) => {
    onChange({
      type: 'mcp-resource',
      resourceId,
      resourceName,
      uri: buildMCPResourceURI(resourceId),
      isTargetOnly: true, // MCP resources can only be targets
    });
  };

  const handleMCPToolSelect = (toolId: string, toolName: string) => {
    onChange({
      type: 'mcp-tool',
      resourceId: toolId,
      resourceName: toolName,
      uri: buildMCPToolURI(toolId),
      isTargetOnly: true, // MCP tools can only be targets
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

  const handleStreamSelect = (integrationName: string, topicName: string) => {
    setSelectedIntegration(integrationName);
    setSelectedTopic(topicName);
    
    // Find the stream object - the API returns stream_platform instead of integration_name
    const stream = streams.find(
      (s) => (s.stream_platform || s.integration_name) === integrationName && 
             (s.stream_name || s.topic_name) === topicName
    );
    
    if (!stream) return;

    // Use stream_id if available, otherwise construct from platform/name
    const streamId = stream.stream_id || `${integrationName}/${topicName}`;

    onChange({
      type: 'stream',
      resourceId: streamId,
      resourceName: `${integrationName}/${topicName}`,
      uri: buildStreamURI(streamId),
      // Include additional stream-specific fields
      integrationName,
      topicName,
    } as any);
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
              if (db) handleDatabaseSelect(db.database_id, db.database_name, db.database_type);
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
                  if (db) handleDatabaseSelect(db.database_id, db.database_name, db.database_type);
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

      if (streams.length === 0) {
        return (
          <div className="text-sm text-muted-foreground p-4 text-center border border-border rounded-md">
            No streams available
          </div>
        );
      }

      // Group streams by integration/platform
      // The API returns stream_platform and stream_name, not integration_name and topic_name
      const streamsByIntegration: Record<string, any[]> = {};
      streams.forEach((stream) => {
        const integration = stream.stream_platform || stream.integration_name;
        if (integration) {
          if (!streamsByIntegration[integration]) {
            streamsByIntegration[integration] = [];
          }
          streamsByIntegration[integration].push(stream);
        }
      });

      return (
        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium text-foreground mb-1.5">
              1. Select Integration
            </label>
            <select
              className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              value={selectedIntegration || ''}
              onChange={(e) => {
                setSelectedIntegration(e.target.value);
                setSelectedTopic(null);
              }}
              disabled={disabled}
            >
              <option value="">Select an integration...</option>
              {Object.keys(streamsByIntegration).map((integration) => (
                <option key={integration} value={integration}>
                  {integration}
                </option>
              ))}
            </select>
          </div>

          {selectedIntegration && (
            <div>
              <label className="block text-xs font-medium text-foreground mb-1.5">
                2. Select Topic
              </label>
              {streamsByIntegration[selectedIntegration]?.length > 0 ? (
                <select
                  className="w-full px-3 py-1.5 text-sm bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                  value={selectedTopic || ''}
                  onChange={(e) => handleStreamSelect(selectedIntegration, e.target.value)}
                  disabled={disabled}
                >
                  <option value="">Select a topic...</option>
                  {streamsByIntegration[selectedIntegration].map((stream) => {
                    const topicName = stream.stream_name || stream.topic_name;
                    return (
                      <option key={topicName} value={topicName}>
                        {topicName}
                      </option>
                    );
                  })}
                </select>
              ) : (
                <div className="text-xs text-muted-foreground p-3 text-center border border-border rounded-md">
                  No topics available for this integration
                </div>
              )}
            </div>
          )}
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
            isTargetSelector={isTargetSelector}
            enableStatsFiltering={enableStatsFiltering}
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
          {value.type === 'stream' && (value as any).integrationName && (value as any).topicName && (
            <span className="text-muted-foreground">
              {' '}
              ({(value as any).integrationName}/{(value as any).topicName})
            </span>
          )}
        </div>
      )}
    </div>
  );
}

