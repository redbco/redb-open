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
  Database,
} from 'lucide-react';
import { ResourceType, ContainerType } from '@/lib/api/types';
import { getContainerTypeName } from '@/lib/utils/container-type-detector';

interface ContainerTypeBadgeProps {
  type: ResourceType | ContainerType;
  size?: 'sm' | 'md' | 'lg';
  showIcon?: boolean;
  showLabel?: boolean;
  className?: string;
}

export function ContainerTypeBadge({
  type,
  size = 'md',
  showIcon = true,
  showLabel = true,
  className = '',
}: ContainerTypeBadgeProps) {
  const getIconComponent = () => {
    const icons: Record<string, typeof Table> = {
      database: Database,
      table: Table,
      'tabular-record-set': Table,
      document: FileJson,
      'keyvalue-item': Key,
      'graph-node': Network,
      'graph-relationship': GitBranch,
      'search-document': Search,
      vector: Layers,
      'timeseries-point': Clock,
      'blob-object': Archive,
      stream: Activity,
      webhook: Webhook,
      'mcp-resource': Box,
      'mcp-tool': Wrench,
    };

    return icons[type] || Table;
  };

  const getLabel = () => {
    const labels: Record<string, string> = {
      database: 'Database',
      table: 'Table',
      'tabular-record-set': 'Table',
      document: 'Document',
      'keyvalue-item': 'Key-Value',
      'graph-node': 'Node',
      'graph-relationship': 'Relationship',
      'search-document': 'Search Doc',
      vector: 'Vector',
      'timeseries-point': 'Time-Series',
      'blob-object': 'Blob',
      stream: 'Stream',
      webhook: 'Webhook',
      'mcp-resource': 'MCP Resource',
      'mcp-tool': 'MCP Tool',
    };

    return labels[type] || type;
  };

  const getColorClasses = () => {
    const colors: Record<string, { bg: string; text: string; border: string }> = {
      database: {
        bg: 'bg-blue-100 dark:bg-blue-900/30',
        text: 'text-blue-700 dark:text-blue-300',
        border: 'border-blue-200 dark:border-blue-800',
      },
      table: {
        bg: 'bg-blue-100 dark:bg-blue-900/30',
        text: 'text-blue-700 dark:text-blue-300',
        border: 'border-blue-200 dark:border-blue-800',
      },
      'tabular-record-set': {
        bg: 'bg-blue-100 dark:bg-blue-900/30',
        text: 'text-blue-700 dark:text-blue-300',
        border: 'border-blue-200 dark:border-blue-800',
      },
      document: {
        bg: 'bg-green-100 dark:bg-green-900/30',
        text: 'text-green-700 dark:text-green-300',
        border: 'border-green-200 dark:border-green-800',
      },
      'keyvalue-item': {
        bg: 'bg-yellow-100 dark:bg-yellow-900/30',
        text: 'text-yellow-700 dark:text-yellow-300',
        border: 'border-yellow-200 dark:border-yellow-800',
      },
      'graph-node': {
        bg: 'bg-purple-100 dark:bg-purple-900/30',
        text: 'text-purple-700 dark:text-purple-300',
        border: 'border-purple-200 dark:border-purple-800',
      },
      'graph-relationship': {
        bg: 'bg-purple-100 dark:bg-purple-900/30',
        text: 'text-purple-700 dark:text-purple-300',
        border: 'border-purple-200 dark:border-purple-800',
      },
      'search-document': {
        bg: 'bg-orange-100 dark:bg-orange-900/30',
        text: 'text-orange-700 dark:text-orange-300',
        border: 'border-orange-200 dark:border-orange-800',
      },
      vector: {
        bg: 'bg-pink-100 dark:bg-pink-900/30',
        text: 'text-pink-700 dark:text-pink-300',
        border: 'border-pink-200 dark:border-pink-800',
      },
      'timeseries-point': {
        bg: 'bg-cyan-100 dark:bg-cyan-900/30',
        text: 'text-cyan-700 dark:text-cyan-300',
        border: 'border-cyan-200 dark:border-cyan-800',
      },
      'blob-object': {
        bg: 'bg-gray-100 dark:bg-gray-900/30',
        text: 'text-gray-700 dark:text-gray-300',
        border: 'border-gray-200 dark:border-gray-800',
      },
      stream: {
        bg: 'bg-indigo-100 dark:bg-indigo-900/30',
        text: 'text-indigo-700 dark:text-indigo-300',
        border: 'border-indigo-200 dark:border-indigo-800',
      },
      webhook: {
        bg: 'bg-red-100 dark:bg-red-900/30',
        text: 'text-red-700 dark:text-red-300',
        border: 'border-red-200 dark:border-red-800',
      },
      'mcp-resource': {
        bg: 'bg-teal-100 dark:bg-teal-900/30',
        text: 'text-teal-700 dark:text-teal-300',
        border: 'border-teal-200 dark:border-teal-800',
      },
      'mcp-tool': {
        bg: 'bg-emerald-100 dark:bg-emerald-900/30',
        text: 'text-emerald-700 dark:text-emerald-300',
        border: 'border-emerald-200 dark:border-emerald-800',
      },
    };

    return colors[type] || colors.table;
  };

  const getSizeClasses = () => {
    const sizes = {
      sm: {
        badge: 'px-1.5 py-0.5 text-[10px]',
        icon: 'h-3 w-3',
        gap: 'gap-1',
      },
      md: {
        badge: 'px-2 py-1 text-xs',
        icon: 'h-3.5 w-3.5',
        gap: 'gap-1.5',
      },
      lg: {
        badge: 'px-3 py-1.5 text-sm',
        icon: 'h-4 w-4',
        gap: 'gap-2',
      },
    };

    return sizes[size];
  };

  const Icon = getIconComponent();
  const label = getLabel();
  const colors = getColorClasses();
  const sizes = getSizeClasses();

  return (
    <span
      className={`
        inline-flex items-center ${sizes.gap} ${sizes.badge}
        ${colors.bg} ${colors.text}
        border ${colors.border}
        rounded-md font-medium
        ${className}
      `}
    >
      {showIcon && <Icon className={sizes.icon} />}
      {showLabel && <span>{label}</span>}
    </span>
  );
}

