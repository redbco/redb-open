'use client';

import { useState } from 'react';
import { Key, ArrowUp, Edit2, Trash2, MoreVertical, Check, X } from 'lucide-react';
import { PrivilegedDataBadge } from './PrivilegedDataBadge';
import type { SchemaColumn } from '@/lib/api/types';

interface ColumnRowProps {
  column: SchemaColumn;
  tableName: string;
  onModify?: (columnName: string) => void;
  onDrop?: (columnName: string) => void;
}

export function ColumnRow({ column, tableName, onModify, onDrop }: ColumnRowProps) {
  const [showActions, setShowActions] = useState(false);

  // Normalize field names (handle both camelCase and snake_case) - data from enriched schema endpoint
  const isPrimaryKey = column.isPrimaryKey || column.is_primary_key || false;
  const isAutoIncrement = column.isAutoIncrement || column.is_auto_increment || false;
  const isNullable = column.isNullable ?? column.is_nullable ?? true;
  const isUnique = column.isUnique || column.is_unique || false;
  const isIndexed = column.isIndexed || column.is_indexed || false;
  const varcharLength = column.varcharLength || column.varchar_length;
  const dataCategory = column.dataCategory || column.data_category || 'standard';
  const isPrivileged = column.isPrivileged || column.is_privileged || false;
  const privilegedConfidence = column.detectionConfidence || column.detection_confidence || 0;
  const privilegedDescription = column.privilegedDescription || column.privileged_description;
  const privilegedClassification = column.privilegedClassification || column.privileged_classification;
  const detectionMethod = column.detectionMethod || column.detection_method;
  const dataType = column.dataType || column.type || column.data_type || 'unknown';
  const columnDefault = column.columnDefault || column.column_default || column.defaultValue || column.default_value;
  const constraints = column.constraints || [];

  // Format data type with length if applicable
  const formattedType = varcharLength ? `${dataType}(${varcharLength})` : dataType;

  return (
    <div
      className="group flex items-center justify-between py-3 px-4 border-b border-border last:border-b-0 hover:bg-muted/50 transition-colors min-h-[60px]"
      onMouseEnter={() => setShowActions(true)}
      onMouseLeave={() => setShowActions(false)}
    >
      <div className="flex-1 grid grid-cols-1 md:grid-cols-12 gap-4 items-center min-h-0">
        {/* Column Name */}
        <div className="md:col-span-3 flex items-center gap-2">
          <span className="font-medium text-foreground font-mono text-sm">{column.name}</span>
          {isPrimaryKey && (
            <span
              className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400 text-xs"
              title="Primary Key"
            >
              <Key className="h-3 w-3" />
              PK
            </span>
          )}
          {isAutoIncrement && (
            <span
              className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-purple-100 text-purple-800 dark:bg-purple-900/20 dark:text-purple-400 text-xs"
              title="Auto Increment"
            >
              <ArrowUp className="h-3 w-3" />
              Inc
            </span>
          )}
        </div>

        {/* Data Type */}
        <div className="md:col-span-2">
          <span className="text-sm text-muted-foreground font-mono">{formattedType}</span>
        </div>

        {/* Constraints */}
        <div className="md:col-span-2 flex items-center gap-1 flex-wrap">
          {!isNullable && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300 text-xs">
              NOT NULL
            </span>
          )}
          {isUnique && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-indigo-100 text-indigo-700 dark:bg-indigo-900/20 dark:text-indigo-400 text-xs">
              UNIQUE
            </span>
          )}
          {isIndexed && !isPrimaryKey && (
            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-blue-100 text-blue-700 dark:bg-blue-900/20 dark:text-blue-400 text-xs">
              INDEXED
            </span>
          )}
          {columnDefault && (
            <span 
              className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-green-100 text-green-700 dark:bg-green-900/20 dark:text-green-400 text-xs"
              title={`Default: ${columnDefault}`}
            >
              DEFAULT
            </span>
          )}
        </div>

        {/* Data Category & Privileged Data (from enriched schema endpoint) */}
        <div className="md:col-span-4">
          <PrivilegedDataBadge
            dataCategory={dataCategory}
            isPrivileged={isPrivileged}
            confidence={privilegedConfidence}
            description={privilegedDescription}
            classification={privilegedClassification}
            detectionMethod={detectionMethod}
          />
          {!isPrivileged && dataCategory !== 'standard' && dataCategory && (
            <span className="inline-flex items-center px-2 py-0.5 rounded bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400 text-xs font-medium">
              {dataCategory}
            </span>
          )}
        </div>

        {/* Actions */}
        <div className="md:col-span-1 flex items-center justify-end gap-1">
          {showActions && (
            <>
              {onModify && (
                <button
                  onClick={() => onModify(column.name)}
                  className="p-1.5 rounded hover:bg-accent hover:text-accent-foreground transition-colors"
                  title="Modify Column"
                >
                  <Edit2 className="h-3.5 w-3.5" />
                </button>
              )}
              {onDrop && !isPrimaryKey && (
                <button
                  onClick={() => onDrop(column.name)}
                  className="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/20 hover:text-red-600 dark:hover:text-red-400 transition-colors"
                  title="Drop Column"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

