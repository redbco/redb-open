'use client';

import { useState } from 'react';
import { Table, ChevronDown, ChevronRight, Edit2, Plus, Database } from 'lucide-react';
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

  // Normalize field names (handle both camelCase and snake_case)
  const tableType = table.tableType || table.table_type || 'table';
  const primaryCategory = table.primaryCategory || table.primary_category || 'General';
  const classificationScores = table.classificationScores || table.classification_scores || [];
  const classificationConfidence =
    table.classificationConfidence || table.classification_confidence || 0;

  // Get primary classification score
  const primaryClassification = classificationScores.length > 0 ? classificationScores[0] : null;

  // Count privileged columns
  const privilegedColumnCount = table.columns.filter(
    (col) => (col.isPrivilegedData || col.is_privileged_data) && 
             (col.privilegedConfidence || col.privileged_confidence || 0) > 0.7
  ).length;

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
              <h3 className="text-lg font-semibold text-foreground">{table.name}</h3>
              <div className="flex items-center gap-2 mt-1 text-sm text-muted-foreground">
                {table.engine && (
                  <span className="inline-flex items-center gap-1">
                    <Database className="h-3 w-3" />
                    {table.engine}
                  </span>
                )}
                {table.schema && <span>• Schema: {table.schema}</span>}
                {tableType && <span>• Type: {tableType}</span>}
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center gap-2">
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

        {/* Classification Info */}
        <div className="flex items-center gap-4 text-sm">
          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-background border border-border">
            <span className="text-muted-foreground">Category:</span>
            <span className="font-medium text-foreground">{primaryCategory}</span>
          </div>

          {classificationConfidence > 0 && (
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-background border border-border">
              <span className="text-muted-foreground">Classification:</span>
              <span className="font-medium text-foreground">
                {(classificationConfidence * 100).toFixed(0)}% confidence
              </span>
            </div>
          )}

          {privilegedColumnCount > 0 && (
            <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400 border border-red-200 dark:border-red-800">
              <span className="font-medium">
                {privilegedColumnCount} Privileged Column{privilegedColumnCount !== 1 ? 's' : ''}
              </span>
            </div>
          )}

          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-md bg-background border border-border">
            <span className="text-muted-foreground">
              {table.columns.length} Column{table.columns.length !== 1 ? 's' : ''}
            </span>
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
              {table.columns.map((column, index) => (
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

