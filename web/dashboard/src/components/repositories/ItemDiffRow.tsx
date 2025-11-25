'use client';

import { Plus, Edit, Minus, Shield, Key, Link2 } from 'lucide-react';
import type { CommitItem, ChangeStatus } from '@/lib/api/types';

interface ItemDiffRowProps {
  item: CommitItem;
}

const getStatusStyles = (status: ChangeStatus) => {
  switch (status) {
    case 'STATUS_CREATED':
      return {
        bg: 'bg-green-50 dark:bg-green-900/10 hover:bg-green-100 dark:hover:bg-green-900/20',
        border: 'border-l-4 border-green-500',
        text: 'text-green-900 dark:text-green-100',
        icon: Plus,
        iconColor: 'text-green-600 dark:text-green-400',
        symbol: '+',
      };
    case 'STATUS_UPDATED':
      return {
        bg: 'bg-yellow-50 dark:bg-yellow-900/10 hover:bg-yellow-100 dark:hover:bg-yellow-900/20',
        border: 'border-l-4 border-yellow-500',
        text: 'text-yellow-900 dark:text-yellow-100',
        icon: Edit,
        iconColor: 'text-yellow-600 dark:text-yellow-400',
        symbol: '~',
      };
    case 'STATUS_DELETED':
      return {
        bg: 'bg-red-50 dark:bg-red-900/10 hover:bg-red-100 dark:hover:bg-red-900/20',
        border: 'border-l-4 border-red-500',
        text: 'text-red-900 dark:text-red-100',
        icon: Minus,
        iconColor: 'text-red-600 dark:text-red-400',
        symbol: '-',
      };
    default:
      return {
        bg: 'bg-background hover:bg-muted/50',
        border: 'border-l-4 border-transparent',
        text: 'text-foreground',
        icon: null,
        iconColor: '',
        symbol: '',
      };
  }
};

const formatDataType = (dataType: string, isArray: boolean) => {
  if (isArray) {
    return `${dataType}[]`;
  }
  return dataType;
};

export function ItemDiffRow({ item }: ItemDiffRowProps) {
  const styles = getStatusStyles(item.change_status);
  const Icon = styles.icon;
  const isDeleted = item.change_status === 'STATUS_DELETED';
  const isCreated = item.change_status === 'STATUS_CREATED';

  return (
    <div
      className={`${styles.bg} ${styles.border} transition-colors`}
    >
      <div className="grid grid-cols-1 md:grid-cols-12 gap-2 md:gap-4 py-3 px-4">
        {/* Column Name */}
        <div className="col-span-1 md:col-span-3">
          <div className="flex items-center gap-2">
            {Icon && (
              <Icon className={`h-4 w-4 flex-shrink-0 ${styles.iconColor}`} />
            )}
            <span className={`font-mono text-sm ${isDeleted ? 'line-through opacity-75' : ''} ${isCreated ? 'font-semibold' : ''} ${styles.text}`}>
              {item.item_name}
            </span>
            {item.is_primary_key && (
              <Key className="h-3.5 w-3.5 text-amber-500" title="Primary Key" />
            )}
          </div>
          <div className="md:hidden text-xs text-muted-foreground mt-1">
            {item.item_display_name}
          </div>
        </div>

        {/* Data Type */}
        <div className="col-span-1 md:col-span-2">
          <span className={`font-mono text-sm ${styles.text}`}>
            {formatDataType(item.data_type, item.is_array)}
          </span>
          <div className="md:hidden flex flex-wrap gap-1 mt-1">
            {item.is_nullable && (
              <span className="text-xs px-1.5 py-0.5 bg-gray-100 dark:bg-gray-800 rounded text-gray-600 dark:text-gray-400">
                nullable
              </span>
            )}
            {item.is_unique && (
              <span className="text-xs px-1.5 py-0.5 bg-blue-100 dark:bg-blue-900/30 rounded text-blue-600 dark:text-blue-400">
                unique
              </span>
            )}
            {item.is_indexed && (
              <span className="text-xs px-1.5 py-0.5 bg-purple-100 dark:bg-purple-900/30 rounded text-purple-600 dark:text-purple-400">
                indexed
              </span>
            )}
          </div>
        </div>

        {/* Constraints */}
        <div className="hidden md:block col-span-2">
          <div className="flex flex-wrap gap-1">
            {item.is_nullable && (
              <span className="text-xs px-1.5 py-0.5 bg-gray-100 dark:bg-gray-800 rounded text-gray-600 dark:text-gray-400">
                nullable
              </span>
            )}
            {item.is_unique && (
              <span className="text-xs px-1.5 py-0.5 bg-blue-100 dark:bg-blue-900/30 rounded text-blue-600 dark:text-blue-400">
                unique
              </span>
            )}
            {item.is_indexed && (
              <span className="text-xs px-1.5 py-0.5 bg-purple-100 dark:bg-purple-900/30 rounded text-purple-600 dark:text-purple-400">
                indexed
              </span>
            )}
            {item.default_value && (
              <span className="text-xs px-1.5 py-0.5 bg-indigo-100 dark:bg-indigo-900/30 rounded text-indigo-600 dark:text-indigo-400" title={`Default: ${item.default_value}`}>
                default
              </span>
            )}
          </div>
        </div>

        {/* Classification */}
        <div className="col-span-1 md:col-span-4">
          {item.is_privileged && item.detection_confidence && item.detection_confidence > 0 ? (
            <div className="flex items-center gap-2">
              <Shield className={`h-4 w-4 flex-shrink-0 ${
                item.detection_confidence > 0.7 
                  ? 'text-red-500' 
                  : item.detection_confidence >= 0.4 
                    ? 'text-yellow-500' 
                    : 'text-gray-500'
              }`} />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className={`text-sm font-medium ${
                    item.detection_confidence > 0.7 
                      ? 'text-red-700 dark:text-red-400' 
                      : item.detection_confidence >= 0.4 
                        ? 'text-yellow-700 dark:text-yellow-400' 
                        : 'text-gray-700 dark:text-gray-400'
                  }`}>
                    {item.privileged_classification || 'Privileged Data'}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    ({Math.round(item.detection_confidence * 100)}%)
                  </span>
                </div>
                {item.detection_method && (
                  <span className="text-xs text-muted-foreground">
                    {item.detection_method}
                  </span>
                )}
              </div>
            </div>
          ) : (
            <span className="text-sm text-muted-foreground">—</span>
          )}
        </div>

        {/* Position */}
        <div className="hidden md:block col-span-1 text-right">
          <span className="text-sm text-muted-foreground">
            #{item.ordinal_position}
          </span>
        </div>
      </div>

      {/* Mobile: Show default value if present */}
      {item.default_value && (
        <div className="md:hidden px-4 pb-3 text-xs text-muted-foreground">
          Default: <span className="font-mono">{item.default_value}</span>
        </div>
      )}
    </div>
  );
}

