'use client';

import { useState } from 'react';
import { X, Edit2 } from 'lucide-react';
import type { SchemaColumn } from '@/lib/api/types';

interface ModifyColumnDialogProps {
  tableName: string;
  column: SchemaColumn;
  onClose: () => void;
  onModify: (tableName: string, columnName: string, modifications: {
    newName?: string;
    dataType?: string;
    length?: number;
    nullable?: boolean;
    defaultValue?: string;
    privilegedDataCategory?: string;
  }) => void;
}

export function ModifyColumnDialog({ tableName, column, onClose, onModify }: ModifyColumnDialogProps) {
  // Normalize field names
  const currentDataType = column.dataType || column.type || 'VARCHAR';
  const currentLength = column.varcharLength || column.varchar_length;
  const currentNullable = column.isNullable ?? true;
  const currentDefault = column.columnDefault || column.column_default || '';
  const currentDataCategory = column.dataCategory || column.data_category || 'standard';

  const [newColumnName, setNewColumnName] = useState(column.name);
  const [dataType, setDataType] = useState(currentDataType);
  const [length, setLength] = useState(currentLength?.toString() || '');
  const [nullable, setNullable] = useState(currentNullable);
  const [defaultValue, setDefaultValue] = useState(currentDefault);
  const [dataCategory, setDataCategory] = useState(currentDataCategory);
  const [isModifying, setIsModifying] = useState(false);

  const dataTypes = [
    'VARCHAR', 'CHAR', 'TEXT',
    'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
    'DECIMAL', 'FLOAT', 'DOUBLE',
    'DATE', 'DATETIME', 'TIMESTAMP', 'TIME',
    'BOOLEAN', 'JSON', 'BLOB'
  ];

  const dataCategories = [
    'standard', 'pii', 'personal_identity', 'financial',
    'health', 'authentication', 'secret', 'sensitive'
  ];

  const needsLength = ['VARCHAR', 'CHAR', 'DECIMAL'].includes(dataType);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    const modifications: any = {};

    if (newColumnName !== column.name) {
      modifications.newName = newColumnName;
    }
    if (dataType !== currentDataType) {
      modifications.dataType = dataType;
    }
    if (needsLength && length && parseInt(length) !== currentLength) {
      modifications.length = parseInt(length);
    }
    if (nullable !== currentNullable) {
      modifications.nullable = nullable;
    }
    if (defaultValue !== currentDefault) {
      modifications.defaultValue = defaultValue;
    }
    if (dataCategory !== currentDataCategory) {
      modifications.privilegedDataCategory = dataCategory;
    }

    if (Object.keys(modifications).length === 0) {
      onClose();
      return;
    }

    setIsModifying(true);
    try {
      await onModify(tableName, column.name, modifications);
      onClose();
    } catch (error) {
      console.error('Failed to modify column:', error);
    } finally {
      setIsModifying(false);
    }
  };

  // Generate SQL preview
  const generateSQL = () => {
    const changes: string[] = [];

    if (newColumnName !== column.name) {
      changes.push(`ALTER TABLE ${tableName} RENAME COLUMN ${column.name} TO ${newColumnName};`);
    }

    const typeChanged = dataType !== currentDataType;
    const lengthChanged = needsLength && length && parseInt(length) !== currentLength;
    const nullableChanged = nullable !== currentNullable;
    const defaultChanged = defaultValue !== currentDefault;

    if (typeChanged || lengthChanged || nullableChanged || defaultChanged) {
      let alterStmt = `ALTER TABLE ${tableName} MODIFY COLUMN ${newColumnName !== column.name ? newColumnName : column.name} ${dataType}`;
      if (needsLength && length) {
        alterStmt += `(${length})`;
      }
      if (!nullable) {
        alterStmt += ' NOT NULL';
      } else {
        alterStmt += ' NULL';
      }
      if (defaultValue) {
        alterStmt += ` DEFAULT '${defaultValue}'`;
      }
      alterStmt += ';';
      changes.push(alterStmt);
    }

    return changes.join('\n');
  };

  const hasChanges =
    newColumnName !== column.name ||
    dataType !== currentDataType ||
    (needsLength && length && parseInt(length) !== currentLength) ||
    nullable !== currentNullable ||
    defaultValue !== currentDefault ||
    dataCategory !== currentDataCategory;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
              <Edit2 className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Modify Column</h2>
              <p className="text-sm text-muted-foreground mt-0.5">
                Update properties for column <strong>{column.name}</strong> in table <strong>{tableName}</strong>
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="p-6 space-y-6">
          {/* Column Name */}
          <div>
            <label htmlFor="columnName" className="block text-sm font-medium text-foreground mb-2">
              Column Name
            </label>
            <input
              id="columnName"
              type="text"
              value={newColumnName}
              onChange={(e) => setNewColumnName(e.target.value)}
              placeholder="e.g., email_address"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary font-mono"
              required
            />
          </div>

          {/* Data Type and Length */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label htmlFor="dataType" className="block text-sm font-medium text-foreground mb-2">
                Data Type
              </label>
              <select
                id="dataType"
                value={dataType}
                onChange={(e) => setDataType(e.target.value)}
                className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              >
                {dataTypes.map((type) => (
                  <option key={type} value={type}>
                    {type}
                  </option>
                ))}
              </select>
            </div>

            {needsLength && (
              <div>
                <label htmlFor="length" className="block text-sm font-medium text-foreground mb-2">
                  Length
                </label>
                <input
                  id="length"
                  type="number"
                  value={length}
                  onChange={(e) => setLength(e.target.value)}
                  placeholder="255"
                  className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                />
              </div>
            )}
          </div>

          {/* Default Value */}
          <div>
            <label htmlFor="defaultValue" className="block text-sm font-medium text-foreground mb-2">
              Default Value
            </label>
            <input
              id="defaultValue"
              type="text"
              value={defaultValue}
              onChange={(e) => setDefaultValue(e.target.value)}
              placeholder="e.g., 0, 'default', NULL"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary font-mono"
            />
          </div>

          {/* Data Category */}
          <div>
            <label htmlFor="dataCategory" className="block text-sm font-medium text-foreground mb-2">
              Data Category / Classification
            </label>
            <select
              id="dataCategory"
              value={dataCategory}
              onChange={(e) => setDataCategory(e.target.value)}
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            >
              {dataCategories.map((cat) => (
                <option key={cat} value={cat}>
                  {cat.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase())}
                </option>
              ))}
            </select>
            <p className="text-xs text-muted-foreground mt-1">
              Update the data classification for this column
            </p>
          </div>

          {/* Nullable Checkbox */}
          <div>
            <label className="flex items-center gap-2 p-3 border border-input rounded-md cursor-pointer hover:bg-accent transition-colors">
              <input
                type="checkbox"
                checked={nullable}
                onChange={(e) => setNullable(e.target.checked)}
                className="rounded border-input"
              />
              <div>
                <span className="text-sm font-medium text-foreground">Nullable</span>
                <p className="text-xs text-muted-foreground">Allow NULL values for this column</p>
              </div>
            </label>
          </div>

          {/* SQL Preview */}
          {hasChanges && (
            <div className="bg-muted rounded-lg p-4">
              <p className="text-sm font-medium text-foreground mb-2">SQL Preview:</p>
              <pre className="text-xs font-mono text-muted-foreground overflow-x-auto whitespace-pre-wrap">
                {generateSQL()}
              </pre>
            </div>
          )}

          {/* Warning */}
          <div className="bg-yellow-50 dark:bg-yellow-900/10 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
            <p className="text-sm text-yellow-900 dark:text-yellow-100 font-medium">⚠️ Warning</p>
            <p className="text-sm text-yellow-800 dark:text-yellow-200 mt-1">
              Modifying column properties will directly affect the live database. This may cause data loss or application errors if not done carefully.
            </p>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center justify-end gap-3 pt-4 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              disabled={isModifying}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isModifying || !hasChanges || !newColumnName.trim()}
            >
              {isModifying ? 'Modifying...' : 'Modify Column'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

