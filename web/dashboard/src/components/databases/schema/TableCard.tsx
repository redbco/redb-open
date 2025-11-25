'use client';

import { useState } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { Table, ChevronDown, ChevronRight, Settings, Upload, Trash2, Eraser, ExternalLink, Shield } from 'lucide-react';
import { ColumnRow } from './ColumnRow';
import { formatContainerCategory } from '@/lib/formatters';
import type { SchemaTable } from '@/lib/api/types';

interface TableCardProps {
  table: SchemaTable;
  onModifyTable?: (tableName: string) => void;
  onDeployTable?: (tableName: string) => void;
  onDropTable?: (tableName: string) => void;
  onWipeTable?: (tableName: string) => void;
  onModifyColumn?: (tableName: string, columnName: string) => void;
  onDropColumn?: (tableName: string, columnName: string) => void;
}

export function TableCard({
  table,
  onModifyTable,
  onDeployTable,
  onDropTable,
  onWipeTable,
  onModifyColumn,
  onDropColumn,
}: TableCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const params = useParams();
  const workspaceId = params?.workspaceId as string;
  const databaseName = params?.databaseName as string;

  // Normalize field names (handle both camelCase and snake_case) from enriched schema endpoint
  const databaseType = table.database_type;
  const objectType = table.object_type || table.tableType || table.table_type || 'table';
  const primaryCategory = table.primaryCategory || table.primary_category || table.container_classification || 'General';
  const classificationScores = table.classificationScores || table.classification_scores || [];
  const classificationConfidence =
    table.classificationConfidence || table.classification_confidence || table.container_classification_confidence || 0;
  const classificationSource = table.container_classification_source;

  // Count privileged columns by confidence level from enriched schema data
  const privilegedColumnStats = table.columns.reduce(
    (acc, col) => {
      const isPrivileged = col.isPrivileged || col.is_privileged || false;
      const confidence = col.detectionConfidence || col.detection_confidence || 0;
      
      if (isPrivileged) {
        acc.total++;
        if (confidence > 0.7) {
          acc.high++;
        } else if (confidence >= 0.4) {
          acc.medium++;
        } else if (confidence > 0) {
          acc.low++;
        }
      }
      return acc;
    },
    { total: 0, high: 0, medium: 0, low: 0 }
  );

  const privilegedColumnCount = privilegedColumnStats.high;

  // Format classification source for display
  const formatClassificationSource = (source?: string) => {
    if (!source) return 'auto';
    return source.toLowerCase() === 'automatic' || source.toLowerCase() === 'auto' ? 'auto' : source;
  };

  // Build link to table data page
  const tableDataLink = workspaceId && databaseName 
    ? `/workspaces/${workspaceId}/databases/${databaseName}/tables/${table.name}`
    : '#';
  
  // Sort columns by ordinal_position (from enriched schema endpoint)
  const sortedColumns = [...table.columns].sort((a, b) => {
    const posA = a.ordinal_position || a.ordinalPosition || 0;
    const posB = b.ordinal_position || b.ordinalPosition || 0;
    return posA - posB;
  });

  return (
    <div className="group bg-card border border-border rounded-xl overflow-hidden hover:shadow-lg hover:border-primary/20 transition-all duration-200">
      {/* Table Header */}
      <div className="p-5 border-b border-border bg-muted/30">
        <div className="flex items-start justify-between mb-3">
          <div className="flex items-center gap-3 flex-1">
            <button
              onClick={() => setIsExpanded(!isExpanded)}
              className="p-1 hover:bg-accent rounded transition-colors"
            >
              {isExpanded ? (
                <ChevronDown className="h-5 w-5 text-muted-foreground" />
              ) : (
                <ChevronRight className="h-5 w-5 text-muted-foreground" />
              )}
            </button>
            <div className={`w-10 h-10 bg-gradient-to-br rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200 ${
              privilegedColumnCount > 0 
                ? 'from-red-50 to-red-100 dark:from-red-900/20 dark:to-red-800/20' 
                : 'from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900'
            }`}>
              {privilegedColumnCount > 0 ? (
                <Shield className="h-5 w-5 text-red-600 dark:text-red-400" />
              ) : (
                <Table className="h-5 w-5 text-foreground" />
              )}
            </div>
            <div className="flex-1">
              <Link 
                href={tableDataLink}
                className="font-semibold text-lg text-foreground hover:text-primary hover:underline transition-colors flex items-center gap-1"
              >
                {table.name}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover:opacity-50 transition-opacity" />
              </Link>
              <div className="flex items-center gap-2 mt-1 text-sm text-muted-foreground">
                <span className="font-medium">
                  {formatContainerCategory(primaryCategory)} ({formatClassificationSource(classificationSource)}, {(classificationConfidence * 100).toFixed(0)}%)
                </span>
                <span>•</span>
                <span>{table.columns.length} column{table.columns.length !== 1 ? 's' : ''}</span>
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center gap-1">
            {onModifyTable && (
              <button
                onClick={() => onModifyTable(table.name)}
                className="p-2 text-muted-foreground hover:text-primary hover:bg-primary/10 rounded-md transition-colors"
                title="Modify Table"
              >
                <Settings className="h-4 w-4" />
              </button>
            )}
            {onDeployTable && (
              <button
                onClick={() => onDeployTable(table.name)}
                className="p-2 text-muted-foreground hover:text-blue-600 dark:hover:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded-md transition-colors"
                title="Deploy to Database"
              >
                <Upload className="h-4 w-4" />
              </button>
            )}
            {onDropTable && (
              <button
                onClick={() => onDropTable(table.name)}
                className="p-2 text-muted-foreground hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-md transition-colors"
                title="Drop Table"
              >
                <Trash2 className="h-4 w-4" />
              </button>
            )}
            {onWipeTable && (
              <button
                onClick={() => onWipeTable(table.name)}
                className="p-2 text-muted-foreground hover:text-orange-600 dark:hover:text-orange-400 hover:bg-orange-50 dark:hover:bg-orange-900/20 rounded-md transition-colors"
                title="Wipe Table Data"
              >
                <Eraser className="h-4 w-4" />
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Columns List */}
      {isExpanded && (
        <div className="divide-y divide-border">
          {table.columns.length === 0 ? (
            <div className="p-8 text-center text-muted-foreground">
              <p>No columns found in this table</p>
            </div>
          ) : (
            <>
              {/* Column Headers */}
              <div className="hidden md:grid grid-cols-12 gap-4 py-3 px-4 bg-muted/50 text-sm font-medium text-muted-foreground border-b border-border">
                <div className="col-span-3">Column Name</div>
                <div className="col-span-2">Data Type</div>
                <div className="col-span-2">Constraints</div>
                <div className="col-span-4">Classification</div>
                <div className="col-span-1 text-right">Actions</div>
              </div>

              {/* Column Rows */}
              {sortedColumns.map((column, index) => (
                <ColumnRow
                  key={`${table.name}-${column.name}-${index}`}
                  column={column}
                  tableName={table.name}
                  onModify={onModifyColumn ? (colName) => onModifyColumn(table.name, colName) : undefined}
                  onDrop={onDropColumn ? (colName) => onDropColumn(table.name, colName) : undefined}
                />
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}

