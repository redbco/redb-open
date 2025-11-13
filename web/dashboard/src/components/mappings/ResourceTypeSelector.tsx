'use client';

import { Database, Table, Box, Wrench, Webhook, Activity } from 'lucide-react';
import { ResourceType } from '@/lib/api/types';
import { getResourceTypeName } from '@/lib/utils/mapping-validator';

interface ResourceTypeSelectorProps {
  onSelect: (type: ResourceType) => void;
  selected: ResourceType | null;
  disabled?: boolean;
  allowedTypes?: ResourceType[];
}

interface ResourceTypeOption {
  type: ResourceType;
  icon: typeof Database;
  label: string;
  description: string;
}

const RESOURCE_TYPES: ResourceTypeOption[] = [
  {
    type: 'database',
    icon: Database,
    label: 'Database',
    description: 'Entire database mapping',
  },
  {
    type: 'table',
    icon: Table,
    label: 'Table',
    description: 'Single table mapping',
  },
  {
    type: 'mcp-resource',
    icon: Box,
    label: 'MCP Resource',
    description: 'MCP resource endpoint',
  },
  {
    type: 'mcp-tool',
    icon: Wrench,
    label: 'MCP Tool',
    description: 'MCP tool function',
  },
  {
    type: 'webhook',
    icon: Webhook,
    label: 'Webhook',
    description: 'Webhook endpoint',
  },
  {
    type: 'stream',
    icon: Activity,
    label: 'Stream',
    description: 'Data stream',
  },
];

export function ResourceTypeSelector({
  onSelect,
  selected,
  disabled = false,
  allowedTypes,
}: ResourceTypeSelectorProps) {
  // Filter types if allowedTypes is provided
  const filteredTypes = allowedTypes
    ? RESOURCE_TYPES.filter((t) => allowedTypes.includes(t.type))
    : RESOURCE_TYPES;

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
      {filteredTypes.map((resourceType) => {
        const Icon = resourceType.icon;
        const isSelected = selected === resourceType.type;
        const isDisabled = disabled;

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
              <div className={`font-medium text-xs ${isSelected ? 'text-primary' : 'text-foreground'}`}>
                {resourceType.label}
              </div>
            </div>
            {isSelected && (
              <div className="absolute top-1.5 right-1.5">
                <div className="h-2 w-2 rounded-full bg-primary"></div>
              </div>
            )}
          </button>
        );
      })}
    </div>
  );
}

