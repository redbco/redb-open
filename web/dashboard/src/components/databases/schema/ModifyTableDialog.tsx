'use client';

import { useState } from 'react';
import { X, Edit2 } from 'lucide-react';

interface ModifyTableDialogProps {
  tableName: string;
  onClose: () => void;
  onModify: (tableName: string, newName?: string, comment?: string) => void;
}

export function ModifyTableDialog({ tableName, onClose, onModify }: ModifyTableDialogProps) {
  const [newTableName, setNewTableName] = useState(tableName);
  const [tableComment, setTableComment] = useState('');
  const [isModifying, setIsModifying] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    setIsModifying(true);
    try {
      await onModify(
        tableName,
        newTableName !== tableName ? newTableName : undefined,
        tableComment || undefined
      );
      onClose();
    } catch (error) {
      console.error('Failed to modify table:', error);
    } finally {
      setIsModifying(false);
    }
  };

  const hasChanges = newTableName !== tableName || tableComment.trim().length > 0;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
              <Edit2 className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Modify Table</h2>
              <p className="text-sm text-muted-foreground mt-0.5">
                Update properties for table <strong>{tableName}</strong>
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
          {/* Table Name */}
          <div>
            <label htmlFor="tableName" className="block text-sm font-medium text-foreground mb-2">
              Table Name
            </label>
            <input
              id="tableName"
              type="text"
              value={newTableName}
              onChange={(e) => setNewTableName(e.target.value)}
              placeholder="Enter table name"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary font-mono"
              required
            />
            {newTableName !== tableName && (
              <p className="text-xs text-orange-600 dark:text-orange-400 mt-1">
                ⚠️ Renaming will execute: ALTER TABLE {tableName} RENAME TO {newTableName}
              </p>
            )}
          </div>

          {/* Table Comment */}
          <div>
            <label htmlFor="tableComment" className="block text-sm font-medium text-foreground mb-2">
              Table Comment (Optional)
            </label>
            <textarea
              id="tableComment"
              value={tableComment}
              onChange={(e) => setTableComment(e.target.value)}
              placeholder="Add a comment describing this table"
              rows={3}
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary resize-none"
            />
            <p className="text-xs text-muted-foreground mt-1">
              This comment will be added as metadata to the table
            </p>
          </div>

          {/* Preview */}
          {hasChanges && (
            <div className="bg-muted rounded-lg p-4">
              <p className="text-sm font-medium text-foreground mb-2">SQL Preview:</p>
              <pre className="text-xs font-mono text-muted-foreground overflow-x-auto">
                {newTableName !== tableName && (
                  <>ALTER TABLE {tableName} RENAME TO {newTableName};</>
                )}
                {tableComment && (
                  <>
                    {newTableName !== tableName && '\n'}
                    COMMENT ON TABLE {newTableName !== tableName ? newTableName : tableName} IS '{tableComment}';
                  </>
                )}
              </pre>
            </div>
          )}

          {/* Warning */}
          <div className="bg-yellow-50 dark:bg-yellow-900/10 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
            <p className="text-sm text-yellow-900 dark:text-yellow-100 font-medium">⚠️ Warning</p>
            <p className="text-sm text-yellow-800 dark:text-yellow-200 mt-1">
              Modifying table properties will directly affect the live database. Make sure you understand the implications before proceeding.
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
              disabled={isModifying || !hasChanges || !newTableName.trim()}
            >
              {isModifying ? 'Modifying...' : 'Modify Table'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

