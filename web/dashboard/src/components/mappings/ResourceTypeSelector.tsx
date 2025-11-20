'use client';

import { 
  Table, 
  FileJson, 
  Key, 
  Network, 
  GitBranch,
  Search, 
  Layers, 
  Clock, 
  Archive,
  Activity,
  Webhook,
  Box,
  Wrench,
} from 'lucide-react';
import { ResourceType } from '@/lib/api/types';
import { useContainerStats } from '@/lib/hooks/useContainerStats';
import { useParams } from 'next/navigation';

interface ResourceTypeSelectorProps {
  onSelect: (type: ResourceType) => void;
  selected: ResourceType | null;
  disabled?: boolean;
  allowedTypes?: ResourceType[];
  isTargetSelector?: boolean; // If true, show MCP resources/tools
  enableStatsFiltering?: boolean; // If true, disable icons with no containers
}

interface ResourceTypeOption {
  type: ResourceType;
  icon: typeof Table;
  label: string;
  description: string;
  category: 'container' | 'endpoint' | 'target-only';
}

const CONTAINER_TYPES: ResourceTypeOption[] = [
  {
    type: 'tabular-record-set',
    icon: Table,
    label: 'Tabular Record Set',
    description: 'SQL tables, CSV files',
    category: 'container',
  },
  {
    type: 'document',
    icon: FileJson,
    label: 'Document',
    description: 'JSON documents, MongoDB',
    category: 'container',
  },
  {
    type: 'keyvalue-item',
    icon: Key,
    label: 'Key-Value Item',
    description: 'Redis, DynamoDB items',
    category: 'container',
  },
  {
    type: 'graph-node',
    icon: Network,
    label: 'Graph Node',
    description: 'Neo4j nodes, vertices',
    category: 'container',
  },
  {
    type: 'graph-relationship',
    icon: GitBranch,
    label: 'Graph Relationship',
    description: 'Neo4j edges, relationships',
    category: 'container',
  },
  {
    type: 'search-document',
    icon: Search,
    label: 'Search Document',
    description: 'Elasticsearch, Solr',
    category: 'container',
  },
  {
    type: 'vector',
    icon: Layers,
    label: 'Vector',
    description: 'Embeddings, Milvus',
    category: 'container',
  },
  {
    type: 'timeseries-point',
    icon: Clock,
    label: 'Time-Series Point',
    description: 'InfluxDB, TimescaleDB',
    category: 'container',
  },
  {
    type: 'blob-object',
    icon: Archive,
    label: 'Blob/Object',
    description: 'S3, Azure Blob, files',
    category: 'container',
  },
];

const ENDPOINT_TYPES: ResourceTypeOption[] = [
  {
    type: 'stream',
    icon: Activity,
    label: 'Stream',
    description: 'Kafka, event streams',
    category: 'endpoint',
  },
  {
    type: 'webhook',
    icon: Webhook,
    label: 'Webhook',
    description: 'HTTP endpoints',
    category: 'endpoint',
  },
];

const TARGET_ONLY_TYPES: ResourceTypeOption[] = [
  {
    type: 'mcp-resource',
    icon: Box,
    label: 'MCP Resource',
    description: 'MCP resource endpoint',
    category: 'target-only',
  },
  {
    type: 'mcp-tool',
    icon: Wrench,
    label: 'MCP Tool',
    description: 'MCP tool function',
    category: 'target-only',
  },
];

export function ResourceTypeSelector({
  onSelect,
  selected,
  disabled = false,
  allowedTypes,
  isTargetSelector = false,
  enableStatsFiltering = false,
}: ResourceTypeSelectorProps) {
  const params = useParams();
  const workspaceName = params?.workspaceId as string || '';

  // Fetch container stats if filtering is enabled
  const { stats, loading: statsLoading } = useContainerStats(
    enableStatsFiltering ? workspaceName : ''
  );

  // Build the list of types to show
  let availableTypes = [...CONTAINER_TYPES, ...ENDPOINT_TYPES];
  
  // Add target-only types if this is a target selector
  if (isTargetSelector) {
    availableTypes = [...availableTypes, ...TARGET_ONLY_TYPES];
  }

  // Filter types if allowedTypes is provided
  const filteredTypes = allowedTypes
    ? availableTypes.filter((t) => allowedTypes.includes(t.type))
    : availableTypes;

  // Group types by category
  const containerTypes = filteredTypes.filter(t => t.category === 'container');
  const endpointTypes = filteredTypes.filter(t => t.category === 'endpoint');
  const targetOnlyTypes = filteredTypes.filter(t => t.category === 'target-only');

  const renderTypeButton = (resourceType: ResourceTypeOption) => {
    const Icon = resourceType.icon;
    const isSelected = selected === resourceType.type;
    
    // Check if this type has containers when stats filtering is enabled
    const hasContainers = !enableStatsFiltering || (stats[resourceType.type] !== undefined && stats[resourceType.type] > 0);
    const containerCount = stats[resourceType.type] || 0;
    const isDisabled = disabled || (enableStatsFiltering && !hasContainers && !statsLoading);

    return (
      <button
        key={resourceType.type}
        type="button"
        onClick={() => !isDisabled && onSelect(resourceType.type)}
        disabled={isDisabled}
        className={`
          relative p-3 rounded-lg border-2 transition-all
          ${
            isSelected
              ? 'border-primary bg-primary/5 shadow-sm'
              : 'border-border bg-background hover:border-primary/50'
          }
          ${isDisabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
          disabled:cursor-not-allowed
        `}
      >
        <div className="flex flex-col items-center text-center space-y-1.5">
          <div
            className={`
              p-2 rounded-full transition-colors
              ${isSelected ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}
            `}
          >
            <Icon className="h-5 w-5" />
          </div>
          <div className={`font-medium text-xs leading-tight ${isSelected ? 'text-primary' : 'text-foreground'}`}>
            {resourceType.label}
          </div>
          <div className="text-[10px] text-muted-foreground leading-tight">
            {resourceType.description}
          </div>
          {/* Show container count if available and not loading */}
          {enableStatsFiltering && !statsLoading && hasContainers && (
            <div className="text-[10px] font-semibold text-primary">
              {containerCount.toLocaleString()} items
            </div>
          )}
          {/* Show loading state */}
          {enableStatsFiltering && statsLoading && (
            <div className="text-[10px] text-muted-foreground animate-pulse">
              Loading...
            </div>
          )}
        </div>
        {isSelected && (
          <div className="absolute top-1.5 right-1.5">
            <div className="h-2 w-2 rounded-full bg-primary"></div>
          </div>
        )}
        {/* Show badge if container has items */}
        {enableStatsFiltering && !statsLoading && hasContainers && (
          <div className="absolute top-1.5 left-1.5">
            <div className="px-1.5 py-0.5 rounded-full bg-primary/10 text-primary text-[10px] font-semibold">
              {containerCount}
            </div>
          </div>
        )}
      </button>
    );
  };

  return (
    <div className="space-y-4">
      {/* Container Types */}
      {containerTypes.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-2">
            Data Containers
          </h4>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {containerTypes.map(renderTypeButton)}
          </div>
        </div>
      )}

      {/* Endpoint Types */}
      {endpointTypes.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-2">
            Endpoints
          </h4>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {endpointTypes.map(renderTypeButton)}
          </div>
        </div>
      )}

      {/* Target-Only Types */}
      {targetOnlyTypes.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-2 flex items-center gap-2">
            AI (Model Context Protocol)
            <span className="text-[10px] font-normal normal-case bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 px-1.5 py-0.5 rounded">
              Target Only
            </span>
          </h4>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {targetOnlyTypes.map(renderTypeButton)}
          </div>
        </div>
      )}
    </div>
  );
}
