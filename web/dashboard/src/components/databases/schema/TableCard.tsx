'use client';

import { useState } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { Table, ChevronDown, ChevronRight, Edit2, Plus, Database, Eye, Info } from 'lucide-react';
import { ColumnRow } from './ColumnRow';
import type { SchemaTable } from '@/lib/api/types';

interface TableCardProps {
  table: SchemaTable;
  onModifyTable?: (tableName: string) => void;
  onAddColumn?: (tableName: string) => void;
  onModifyColumn?: (tableName: string, columnName: string) => void;
  onDropColumn?: (tableName: string, columnName: string) => void;
}

export function TableCard({
  table,
  onModifyTable,
  onAddColumn,
  onModifyColumn,
  onDropColumn,
}: TableCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [showClassificationDetails, setShowClassificationDetails] = useState(false);
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
    <div className="bg-card border border-border rounded-lg overflow-hidden hover:shadow-md transition-shadow">
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
            <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
              <Table className="h-5 w-5 text-primary" />
            </div>
            <div className="flex-1">
              <Link href={tableDataLink}>
                <h3 className="text-lg font-semibold text-foreground hover:text-primary transition-colors cursor-pointer">
                  {table.name}
                </h3>
              </Link>
              <div className="flex items-center gap-2 mt-1 text-sm text-muted-foreground">
                {databaseType && (
                  <span className="inline-flex items-center gap-1">
                    <Database className="h-3 w-3" />
                    {databaseType}
                  </span>
                )}
                {objectType && objectType !== 'table' && <span>• Type: {objectType}</span>}
                {table.item_count !== undefined && <span>• {table.item_count} item{table.item_count !== 1 ? 's' : ''}</span>}
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center gap-2">
            <Link href={tableDataLink}>
              <button className="inline-flex items-center gap-1 px-3 py-1.5 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors">
                <Eye className="h-3.5 w-3.5" />
                View Data
              </button>
            </Link>
            {onModifyTable && (
              <button
                onClick={() => onModifyTable(table.name)}
                className="inline-flex items-center gap-1 px-3 py-1.5 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              >
                <Edit2 className="h-3.5 w-3.5" />
                Modify
              </button>
            )}
            {onAddColumn && (
              <button
                onClick={() => onAddColumn(table.name)}
                className="inline-flex items-center gap-1 px-3 py-1.5 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              >
                <Plus className="h-3.5 w-3.5" />
                Add Column
              </button>
            )}
          </div>
        </div>

        {/* Classification Info from enriched schema endpoint */}
        <div className="flex items-center gap-4 text-sm flex-wrap">
          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-background border border-border">
            <span className="text-muted-foreground">Category:</span>
            <span className="font-medium text-foreground">{primaryCategory}</span>
            {classificationSource && (
              <span className="text-xs text-muted-foreground">({classificationSource})</span>
            )}
          </div>

          {classificationConfidence > 0 && (
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-background border border-border">
              <span className="text-muted-foreground">Confidence:</span>
              <span className="font-medium text-foreground">
                {(classificationConfidence * 100).toFixed(0)}%
              </span>
              {classificationScores.length > 1 && (
                <button
                  onClick={() => setShowClassificationDetails(!showClassificationDetails)}
                  className="ml-1 p-0.5 hover:bg-accent rounded transition-colors"
                  title="Show classification details"
                >
                  <Info className="h-3.5 w-3.5" />
                </button>
              )}
            </div>
          )}

          {privilegedColumnCount > 0 && (
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400 border border-red-200 dark:border-red-800">
              <span className="font-medium">
                {privilegedColumnCount} High-Confidence Privileged Column{privilegedColumnCount !== 1 ? 's' : ''}
              </span>
            </div>
          )}
          
          {privilegedColumnStats.total > privilegedColumnCount && (
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400 border border-yellow-200 dark:border-yellow-800">
              <span className="font-medium text-xs">
                +{privilegedColumnStats.total - privilegedColumnCount} Lower-Confidence
              </span>
            </div>
          )}

          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-background border border-border">
            <span className="text-muted-foreground">
              {table.columns.length} Column{table.columns.length !== 1 ? 's' : ''}
            </span>
          </div>
        </div>

        {/* Classification Details Dropdown */}
        {showClassificationDetails && classificationScores.length > 1 && (
          <div className="mt-3 p-3 bg-muted/30 rounded-md border border-border">
            <p className="text-xs font-medium text-muted-foreground mb-2">
              Classification Scores (from schema analysis):
            </p>
            <div className="space-y-1.5">
              {classificationScores.slice(0, 3).map((score, idx) => (
                <div key={idx} className="flex items-center gap-2 text-xs">
                  <div className="flex-shrink-0 w-12 text-right font-mono text-muted-foreground">
                    {(score.score * 100).toFixed(0)}%
                  </div>
                  <div className="flex-1">
                    <span className="font-medium text-foreground">{score.category}</span>
                    {score.reason && (
                      <span className="text-muted-foreground ml-2">- {score.reason}</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
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

