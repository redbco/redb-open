'use client';

import { useState } from 'react';
import { Table, ChevronDown, ChevronRight, Plus, Edit, Minus, Shield, Box } from 'lucide-react';
import { ItemDiffRow } from './ItemDiffRow';
import { formatContainerCategory } from '@/lib/formatters';
import type { CommitContainer, CommitItem, ChangeStatus } from '@/lib/api/types';

interface ContainerDiffCardProps {
  container: CommitContainer;
  items: CommitItem[];
  defaultExpanded?: boolean;
}

const getStatusStyles = (status: ChangeStatus) => {
  switch (status) {
    case 'STATUS_CREATED':
      return {
        bg: 'bg-green-50 dark:bg-green-900/10',
        border: 'border-green-200 dark:border-green-800',
        headerBg: 'bg-green-100 dark:bg-green-900/20',
        icon: Plus,
        iconBg: 'from-green-50 to-green-100 dark:from-green-900/20 dark:to-green-800/20',
        iconColor: 'text-green-600 dark:text-green-400',
        textColor: 'text-green-900 dark:text-green-100',
        symbol: '+',
        badge: 'bg-green-500 text-white',
        badgeText: 'Added',
      };
    case 'STATUS_UPDATED':
      return {
        bg: 'bg-yellow-50 dark:bg-yellow-900/10',
        border: 'border-yellow-200 dark:border-yellow-800',
        headerBg: 'bg-yellow-100 dark:bg-yellow-900/20',
        icon: Edit,
        iconBg: 'from-yellow-50 to-yellow-100 dark:from-yellow-900/20 dark:to-yellow-800/20',
        iconColor: 'text-yellow-600 dark:text-yellow-400',
        textColor: 'text-yellow-900 dark:text-yellow-100',
        symbol: '~',
        badge: 'bg-yellow-500 text-white',
        badgeText: 'Modified',
      };
    case 'STATUS_DELETED':
      return {
        bg: 'bg-red-50 dark:bg-red-900/10',
        border: 'border-red-200 dark:border-red-800',
        headerBg: 'bg-red-100 dark:bg-red-900/20',
        icon: Minus,
        iconBg: 'from-red-50 to-red-100 dark:from-red-900/20 dark:to-red-800/20',
        iconColor: 'text-red-600 dark:text-red-400',
        textColor: 'text-red-900 dark:text-red-100',
        symbol: '-',
        badge: 'bg-red-500 text-white',
        badgeText: 'Deleted',
      };
    default:
      return {
        bg: 'bg-card',
        border: 'border-border',
        headerBg: 'bg-muted/30',
        icon: Box,
        iconBg: 'from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900',
        iconColor: 'text-foreground',
        textColor: 'text-foreground',
        symbol: '',
        badge: 'bg-gray-400 text-white',
        badgeText: 'Unchanged',
      };
  }
};

export function ContainerDiffCard({ container, items, defaultExpanded = false }: ContainerDiffCardProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded);
  const styles = getStatusStyles(container.change_status);
  const Icon = styles.icon;
  const isDeleted = container.change_status === 'STATUS_DELETED';
  const isCreated = container.change_status === 'STATUS_CREATED';

  // Count items by change status
  const itemStats = items.reduce(
    (acc, item) => {
      if (item.change_status === 'STATUS_CREATED') acc.created++;
      else if (item.change_status === 'STATUS_UPDATED') acc.updated++;
      else if (item.change_status === 'STATUS_DELETED') acc.deleted++;
      else acc.unchanged++;
      return acc;
    },
    { created: 0, updated: 0, deleted: 0, unchanged: 0 }
  );

  const hasItemChanges = itemStats.created > 0 || itemStats.updated > 0 || itemStats.deleted > 0;

  // Count privileged items (high confidence only > 0.7)
  const privilegedCount = items.filter(
    item => item.is_privileged && (item.detection_confidence || 0) > 0.7
  ).length;

  // Format classification source
  const formatClassificationSource = (source?: string) => {
    if (!source) return 'auto';
    return source.toLowerCase() === 'automatic' || source.toLowerCase() === 'auto' ? 'auto' : source;
  };

  // Sort items by ordinal position, but show changed items first if container is unchanged
  const sortedItems = [...items].sort((a, b) => {
    // If container is unchanged, prioritize changed items
    if (container.change_status === 'STATUS_UNCHANGED') {
      const aHasChange = a.change_status !== 'STATUS_UNCHANGED';
      const bHasChange = b.change_status !== 'STATUS_UNCHANGED';
      if (aHasChange && !bHasChange) return -1;
      if (!aHasChange && bHasChange) return 1;
    }
    
    // Then sort by ordinal position
    return a.ordinal_position - b.ordinal_position;
  });

  return (
    <div className={`group ${styles.bg} border ${styles.border} rounded-xl overflow-hidden hover:shadow-lg transition-all duration-200`}>
      {/* Container Header */}
      <div className={`p-5 border-b ${styles.border} ${styles.headerBg}`}>
        <div className="flex items-start justify-between mb-3">
          <div className="flex items-center gap-3 flex-1">
            <button
              onClick={() => setIsExpanded(!isExpanded)}
              className="p-1 hover:bg-accent/50 rounded transition-colors"
            >
              {isExpanded ? (
                <ChevronDown className={`h-5 w-5 ${styles.textColor}`} />
              ) : (
                <ChevronRight className={`h-5 w-5 ${styles.textColor}`} />
              )}
            </button>
            <div className={`w-10 h-10 bg-gradient-to-br rounded-xl flex items-center justify-center border ${styles.border} shadow-sm group-hover:scale-105 transition-transform duration-200 ${styles.iconBg}`}>
              {privilegedCount > 0 ? (
                <Shield className="h-5 w-5 text-red-600 dark:text-red-400" />
              ) : (
                <Table className={`h-5 w-5 ${styles.iconColor}`} />
              )}
            </div>
            <div className="flex-1">
              <div className="flex items-center gap-2">
                {Icon && (
                  <Icon className={`h-4 w-4 ${styles.iconColor}`} />
                )}
                <span className={`font-semibold text-lg ${isDeleted ? 'line-through opacity-75' : ''} ${isCreated ? 'font-bold' : ''} ${styles.textColor}`}>
                  {container.object_name}
                </span>
                <span className={`text-xs px-2 py-1 rounded-full ${styles.badge} font-semibold`}>
                  {styles.badgeText}
                </span>
              </div>
              <div className="flex items-center gap-2 mt-1 text-sm text-muted-foreground">
                <span className="font-medium">
                  {formatContainerCategory(container.container_classification)} (
                  {formatClassificationSource(container.container_classification_source)},{' '}
                  {(container.container_classification_confidence * 100).toFixed(0)}%)
                </span>
                <span>•</span>
                <span>
                  {container.item_count} {container.item_count === 1 ? 'item' : 'items'}
                </span>
                {privilegedCount > 0 && (
                  <>
                    <span>•</span>
                    <span className="flex items-center gap-1 text-red-600 dark:text-red-400">
                      <Shield className="h-3.5 w-3.5" />
                      {privilegedCount} privileged
                    </span>
                  </>
                )}
              </div>
              {hasItemChanges && container.change_status === 'STATUS_UNCHANGED' && (
                <div className="flex items-center gap-3 mt-2 text-xs">
                  {itemStats.created > 0 && (
                    <span className="flex items-center gap-1 text-green-700 dark:text-green-400">
                      <Plus className="h-3 w-3" />
                      {itemStats.created} added
                    </span>
                  )}
                  {itemStats.updated > 0 && (
                    <span className="flex items-center gap-1 text-yellow-700 dark:text-yellow-400">
                      <Edit className="h-3 w-3" />
                      {itemStats.updated} modified
                    </span>
                  )}
                  {itemStats.deleted > 0 && (
                    <span className="flex items-center gap-1 text-red-700 dark:text-red-400">
                      <Minus className="h-3 w-3" />
                      {itemStats.deleted} deleted
                    </span>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Items List */}
      {isExpanded && (
        <div className="divide-y divide-border">
          {items.length === 0 ? (
            <div className="p-8 text-center text-muted-foreground">
              <p>No items found in this container</p>
            </div>
          ) : (
            <>
              {/* Column Headers */}
              <div className="hidden md:grid grid-cols-12 gap-4 py-3 px-4 bg-muted/50 text-sm font-medium text-muted-foreground border-b border-border">
                <div className="col-span-3">Item Name</div>
                <div className="col-span-2">Data Type</div>
                <div className="col-span-2">Constraints</div>
                <div className="col-span-4">Classification</div>
                <div className="col-span-1 text-right">Pos.</div>
              </div>

              {/* Item Rows */}
              {sortedItems.map((item, index) => (
                <ItemDiffRow
                  key={`${container.object_name}-${item.item_name}-${index}`}
                  item={item}
                />
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}

