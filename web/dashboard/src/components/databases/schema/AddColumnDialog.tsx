'use client';

import { useState } from 'react';
import { X, Plus } from 'lucide-react';

interface AddColumnDialogProps {
  tableName: string;
  onClose: () => void;
  onAdd: (tableName: string, columnDef: {
    name: string;
    dataType: string;
    length?: number;
    nullable: boolean;
    unique: boolean;
    primaryKey: boolean;
    autoIncrement: boolean;
    defaultValue?: string;
  }) => void;
}

export function AddColumnDialog({ tableName, onClose, onAdd }: AddColumnDialogProps) {
  const [columnName, setColumnName] = useState('');
  const [dataType, setDataType] = useState('VARCHAR');
  const [length, setLength] = useState('255');
  const [nullable, setNullable] = useState(true);
  const [unique, setUnique] = useState(false);
  const [primaryKey, setPrimaryKey] = useState(false);
  const [autoIncrement, setAutoIncrement] = useState(false);
  const [defaultValue, setDefaultValue] = useState('');
  const [isAdding, setIsAdding] = useState(false);

  const dataTypes = [
    'VARCHAR', 'CHAR', 'TEXT',
    'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
    'DECIMAL', 'FLOAT', 'DOUBLE',
    'DATE', 'DATETIME', 'TIMESTAMP', 'TIME',
    'BOOLEAN', 'JSON', 'BLOB'
  ];

  const needsLength = ['VARCHAR', 'CHAR', 'DECIMAL'].includes(dataType);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!columnName.trim()) return;

    setIsAdding(true);
    try {
      await onAdd(tableName, {
        name: columnName.trim(),
        dataType,
        length: needsLength && length ? parseInt(length) : undefined,
        nullable,
        unique,
        primaryKey,
        autoIncrement,
        defaultValue: defaultValue.trim() || undefined,
      });
      onClose();
    } catch (error) {
      console.error('Failed to add column:', error);
    } finally {
      setIsAdding(false);
    }
  };

  // Generate SQL preview
  const generateSQL = () => {
    let sql = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${dataType}`;
    if (needsLength && length) {
      sql += `(${length})`;
    }
    if (!nullable) {
      sql += ' NOT NULL';
    }
    if (unique) {
      sql += ' UNIQUE';
    }
    if (primaryKey) {
      sql += ' PRIMARY KEY';
    }
    if (autoIncrement) {
      sql += ' AUTO_INCREMENT';
    }
    if (defaultValue) {
      sql += ` DEFAULT '${defaultValue}'`;
    }
    sql += ';';
    return sql;
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
              <Plus className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Add Column</h2>
              <p className="text-sm text-muted-foreground mt-0.5">
                Add a new column to table <strong>{tableName}</strong>
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
              Column Name <span className="text-red-500">*</span>
            </label>
            <input
              id="columnName"
              type="text"
              value={columnName}
              onChange={(e) => setColumnName(e.target.value)}
              placeholder="e.g., email_address"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary font-mono"
              required
            />
          </div>

          {/* Data Type and Length */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label htmlFor="dataType" className="block text-sm font-medium text-foreground mb-2">
                Data Type <span className="text-red-500">*</span>
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
              Default Value (Optional)
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

          {/* Constraints */}
          <div className="space-y-3">
            <p className="text-sm font-medium text-foreground">Constraints</p>
            <div className="grid grid-cols-2 gap-3">
              <label className="flex items-center gap-2 p-3 border border-input rounded-md cursor-pointer hover:bg-accent transition-colors">
                <input
                  type="checkbox"
                  checked={nullable}
                  onChange={(e) => setNullable(e.target.checked)}
                  className="rounded border-input"
                />
                <span className="text-sm text-foreground">Nullable</span>
              </label>

              <label className="flex items-center gap-2 p-3 border border-input rounded-md cursor-pointer hover:bg-accent transition-colors">
                <input
                  type="checkbox"
                  checked={unique}
                  onChange={(e) => setUnique(e.target.checked)}
                  className="rounded border-input"
                />
                <span className="text-sm text-foreground">Unique</span>
              </label>

              <label className="flex items-center gap-2 p-3 border border-input rounded-md cursor-pointer hover:bg-accent transition-colors">
                <input
                  type="checkbox"
                  checked={primaryKey}
                  onChange={(e) => setPrimaryKey(e.target.checked)}
                  className="rounded border-input"
                />
                <span className="text-sm text-foreground">Primary Key</span>
              </label>

              <label className="flex items-center gap-2 p-3 border border-input rounded-md cursor-pointer hover:bg-accent transition-colors">
                <input
                  type="checkbox"
                  checked={autoIncrement}
                  onChange={(e) => setAutoIncrement(e.target.checked)}
                  className="rounded border-input"
                />
                <span className="text-sm text-foreground">Auto Increment</span>
              </label>
            </div>
          </div>

          {/* SQL Preview */}
          {columnName && (
            <div className="bg-muted rounded-lg p-4">
              <p className="text-sm font-medium text-foreground mb-2">SQL Preview:</p>
              <pre className="text-xs font-mono text-muted-foreground overflow-x-auto">
                {generateSQL()}
              </pre>
            </div>
          )}

          {/* Warning */}
          <div className="bg-yellow-50 dark:bg-yellow-900/10 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
            <p className="text-sm text-yellow-900 dark:text-yellow-100 font-medium">⚠️ Warning</p>
            <p className="text-sm text-yellow-800 dark:text-yellow-200 mt-1">
              This will modify the live database structure. Ensure the change is safe before proceeding.
            </p>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center justify-end gap-3 pt-4 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
              disabled={isAdding}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isAdding || !columnName.trim()}
            >
              {isAdding ? 'Adding...' : 'Add Column'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

