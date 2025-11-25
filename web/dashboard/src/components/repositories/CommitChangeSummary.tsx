'use client';

import { useState } from 'react';
import { Plus, Edit, Minus, ChevronDown, ChevronUp, Box } from 'lucide-react';
import type { CommitSchemaStructure, ChangeStatus } from '@/lib/api/types';

interface CommitChangeSummaryProps {
  schemaStructure: CommitSchemaStructure;
}

interface ChangeStats {
  created: { containers: string[]; items: number };
  updated: { containers: string[]; items: number };
  deleted: { containers: string[]; items: number };
  unchanged: { containers: string[]; items: number };
}

export function CommitChangeSummary({ schemaStructure }: CommitChangeSummaryProps) {
  const [expandedSection, setExpandedSection] = useState<ChangeStatus | null>(null);

  // Calculate statistics
  const stats: ChangeStats = {
    created: { containers: [], items: 0 },
    updated: { containers: [], items: 0 },
    deleted: { containers: [], items: 0 },
    unchanged: { containers: [], items: 0 },
  };

  // Count containers by change status
  schemaStructure.containers.forEach((container) => {
    const status = container.change_status;
    if (status === 'STATUS_CREATED') {
      stats.created.containers.push(container.object_name);
    } else if (status === 'STATUS_UPDATED') {
      stats.updated.containers.push(container.object_name);
    } else if (status === 'STATUS_DELETED') {
      stats.deleted.containers.push(container.object_name);
    } else if (status === 'STATUS_UNCHANGED') {
      stats.unchanged.containers.push(container.object_name);
    }
  });

  // Count items by change status
  schemaStructure.items.forEach((item) => {
    const status = item.change_status;
    if (status === 'STATUS_CREATED') {
      stats.created.items++;
    } else if (status === 'STATUS_UPDATED') {
      stats.updated.items++;
    } else if (status === 'STATUS_DELETED') {
      stats.deleted.items++;
    } else if (status === 'STATUS_UNCHANGED') {
      stats.unchanged.items++;
    }
  });

  const toggleSection = (status: ChangeStatus) => {
    setExpandedSection(expandedSection === status ? null : status);
  };

  const hasChanges = stats.created.containers.length > 0 || stats.updated.containers.length > 0 || stats.deleted.containers.length > 0;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-xl font-semibold text-foreground">Change Summary</h3>
        {!hasChanges && (
          <span className="text-sm text-muted-foreground italic">No changes in this commit</span>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {/* Created */}
        <div
          className={`bg-green-50 dark:bg-green-900/10 border-2 rounded-lg p-4 transition-all ${
            stats.created.containers.length > 0
              ? 'border-green-500 dark:border-green-700 cursor-pointer hover:shadow-md'
              : 'border-green-200 dark:border-green-800'
          }`}
          onClick={() => stats.created.containers.length > 0 && toggleSection('STATUS_CREATED')}
        >
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-green-500 dark:bg-green-600 rounded-lg flex items-center justify-center">
                <Plus className="h-5 w-5 text-white" />
              </div>
              <span className="font-semibold text-green-900 dark:text-green-100">Added</span>
            </div>
            {stats.created.containers.length > 0 && (
              expandedSection === 'STATUS_CREATED' ? (
                <ChevronUp className="h-4 w-4 text-green-700 dark:text-green-300" />
              ) : (
                <ChevronDown className="h-4 w-4 text-green-700 dark:text-green-300" />
              )
            )}
          </div>
          <div className="space-y-1">
            <p className="text-2xl font-bold text-green-900 dark:text-green-100">
              {stats.created.containers.length}
            </p>
            <p className="text-sm text-green-700 dark:text-green-300">
              {stats.created.containers.length === 1 ? 'container' : 'containers'}
              {stats.created.items > 0 && (
                <span className="ml-1">({stats.created.items} {stats.created.items === 1 ? 'item' : 'items'})</span>
              )}
            </p>
          </div>
          {expandedSection === 'STATUS_CREATED' && stats.created.containers.length > 0 && (
            <div className="mt-3 pt-3 border-t border-green-200 dark:border-green-800">
              <ul className="space-y-1 text-sm text-green-800 dark:text-green-200">
                {stats.created.containers.map((name, idx) => (
                  <li key={idx} className="flex items-center gap-1">
                    <Plus className="h-3 w-3 flex-shrink-0" />
                    <span className="font-mono truncate">{name}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Modified */}
        <div
          className={`bg-yellow-50 dark:bg-yellow-900/10 border-2 rounded-lg p-4 transition-all ${
            stats.updated.containers.length > 0
              ? 'border-yellow-500 dark:border-yellow-700 cursor-pointer hover:shadow-md'
              : 'border-yellow-200 dark:border-yellow-800'
          }`}
          onClick={() => stats.updated.containers.length > 0 && toggleSection('STATUS_UPDATED')}
        >
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-yellow-500 dark:bg-yellow-600 rounded-lg flex items-center justify-center">
                <Edit className="h-5 w-5 text-white" />
              </div>
              <span className="font-semibold text-yellow-900 dark:text-yellow-100">Modified</span>
            </div>
            {stats.updated.containers.length > 0 && (
              expandedSection === 'STATUS_UPDATED' ? (
                <ChevronUp className="h-4 w-4 text-yellow-700 dark:text-yellow-300" />
              ) : (
                <ChevronDown className="h-4 w-4 text-yellow-700 dark:text-yellow-300" />
              )
            )}
          </div>
          <div className="space-y-1">
            <p className="text-2xl font-bold text-yellow-900 dark:text-yellow-100">
              {stats.updated.containers.length}
            </p>
            <p className="text-sm text-yellow-700 dark:text-yellow-300">
              {stats.updated.containers.length === 1 ? 'container' : 'containers'}
              {stats.updated.items > 0 && (
                <span className="ml-1">({stats.updated.items} {stats.updated.items === 1 ? 'item' : 'items'})</span>
              )}
            </p>
          </div>
          {expandedSection === 'STATUS_UPDATED' && stats.updated.containers.length > 0 && (
            <div className="mt-3 pt-3 border-t border-yellow-200 dark:border-yellow-800">
              <ul className="space-y-1 text-sm text-yellow-800 dark:text-yellow-200">
                {stats.updated.containers.map((name, idx) => (
                  <li key={idx} className="flex items-center gap-1">
                    <Edit className="h-3 w-3 flex-shrink-0" />
                    <span className="font-mono truncate">{name}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Deleted */}
        <div
          className={`bg-red-50 dark:bg-red-900/10 border-2 rounded-lg p-4 transition-all ${
            stats.deleted.containers.length > 0
              ? 'border-red-500 dark:border-red-700 cursor-pointer hover:shadow-md'
              : 'border-red-200 dark:border-red-800'
          }`}
          onClick={() => stats.deleted.containers.length > 0 && toggleSection('STATUS_DELETED')}
        >
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-red-500 dark:bg-red-600 rounded-lg flex items-center justify-center">
                <Minus className="h-5 w-5 text-white" />
              </div>
              <span className="font-semibold text-red-900 dark:text-red-100">Deleted</span>
            </div>
            {stats.deleted.containers.length > 0 && (
              expandedSection === 'STATUS_DELETED' ? (
                <ChevronUp className="h-4 w-4 text-red-700 dark:text-red-300" />
              ) : (
                <ChevronDown className="h-4 w-4 text-red-700 dark:text-red-300" />
              )
            )}
          </div>
          <div className="space-y-1">
            <p className="text-2xl font-bold text-red-900 dark:text-red-100">
              {stats.deleted.containers.length}
            </p>
            <p className="text-sm text-red-700 dark:text-red-300">
              {stats.deleted.containers.length === 1 ? 'container' : 'containers'}
              {stats.deleted.items > 0 && (
                <span className="ml-1">({stats.deleted.items} {stats.deleted.items === 1 ? 'item' : 'items'})</span>
              )}
            </p>
          </div>
          {expandedSection === 'STATUS_DELETED' && stats.deleted.containers.length > 0 && (
            <div className="mt-3 pt-3 border-t border-red-200 dark:border-red-800">
              <ul className="space-y-1 text-sm text-red-800 dark:text-red-200">
                {stats.deleted.containers.map((name, idx) => (
                  <li key={idx} className="flex items-center gap-1">
                    <Minus className="h-3 w-3 flex-shrink-0" />
                    <span className="font-mono truncate line-through">{name}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Unchanged */}
        <div className="bg-gray-50 dark:bg-gray-900/10 border-2 border-gray-200 dark:border-gray-800 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-8 h-8 bg-gray-400 dark:bg-gray-600 rounded-lg flex items-center justify-center">
              <Box className="h-5 w-5 text-white" />
            </div>
            <span className="font-semibold text-gray-900 dark:text-gray-100">Unchanged</span>
          </div>
          <div className="space-y-1">
            <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
              {stats.unchanged.containers.length}
            </p>
            <p className="text-sm text-gray-700 dark:text-gray-300">
              {stats.unchanged.containers.length === 1 ? 'container' : 'containers'}
              {stats.unchanged.items > 0 && (
                <span className="ml-1">({stats.unchanged.items} {stats.unchanged.items === 1 ? 'item' : 'items'})</span>
              )}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

