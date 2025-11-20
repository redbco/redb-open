'use client';

import { useState, useEffect } from 'react';
import { ChevronDown } from 'lucide-react';
import { useDatabases } from '@/lib/hooks/useResources';
import { useDatabaseSchemaInfo } from '@/lib/hooks/useDatabaseSchemaInfo';

interface DatabaseContainerSelectorProps {
  workspaceId: string;
  onSelect: (selection: { database: string; databaseId: string; container: string | null; databaseType?: string } | null) => void;
  value: { database: string; container: string | null } | null;
  allowContainerSelection?: boolean; // If false, only select database
  label: string;
  disabled?: boolean;
}

export function DatabaseContainerSelector({
  workspaceId,
  onSelect,
  value,
  allowContainerSelection = true,
  label,
  disabled = false,
}: DatabaseContainerSelectorProps) {
  const [selectedDatabase, setSelectedDatabase] = useState<string>(value?.database || '');
  const [selectedContainer, setSelectedContainer] = useState<string>(value?.container || '');

  const { databases, isLoading: loadingDatabases } = useDatabases(workspaceId);
  const { schema, isLoading: loadingSchema } = useDatabaseSchemaInfo(
    workspaceId,
    selectedDatabase
  );

  // Get containers from schema - handle both new containers format and legacy tables format
  const containers = schema?.containers 
    ? schema.containers.map(c => ({
        name: c.object_name,
        columnCount: c.items?.length || 0
      }))
    : schema?.tables 
      ? schema.tables.map(t => ({
          name: t.name,
          columnCount: t.columns?.length || 0
        }))
      : [];

  useEffect(() => {
    if (selectedDatabase && (!allowContainerSelection || selectedContainer)) {
      const db = databases.find(d => d.database_name === selectedDatabase);
      if (db) {
        onSelect({
          database: selectedDatabase,
          databaseId: db.database_id,
          container: allowContainerSelection ? selectedContainer || null : null,
          databaseType: db?.database_type,
        });
      } else {
        onSelect(null);
      }
    } else {
      onSelect(null);
    }
  }, [selectedDatabase, selectedContainer, allowContainerSelection, databases, onSelect]);

  const handleDatabaseChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const dbName = e.target.value;
    setSelectedDatabase(dbName);
    setSelectedContainer(''); // Reset container when database changes
  };

  const handleContainerChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedContainer(e.target.value);
  };

  return (
    <div className="space-y-3">
      <div className="text-xs font-medium text-foreground mb-1.5">{label}</div>
      
      {/* Database Selector */}
      <div>
        <label className="block text-xs text-muted-foreground mb-1.5">
          1. Select Database
        </label>
        <div className="relative">
          <select
            value={selectedDatabase}
            onChange={handleDatabaseChange}
            disabled={disabled || loadingDatabases}
            className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background appearance-none cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary"
          >
            <option value="">Choose a database...</option>
            {databases.map((db) => (
              <option key={db.database_id} value={db.database_name}>
                {db.database_name} ({db.database_type})
              </option>
            ))}
          </select>
          <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
        </div>
      </div>

      {/* Container Selector */}
      {allowContainerSelection && selectedDatabase && (
        <div>
          <label className="block text-xs text-muted-foreground mb-1.5">
            2. Select Container (Table)
          </label>
          <div className="relative">
            <select
              value={selectedContainer}
              onChange={handleContainerChange}
              disabled={disabled || loadingSchema}
              className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background appearance-none cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary"
            >
              <option value="">
                {loadingSchema ? 'Loading containers...' : 'Choose a container...'}
              </option>
              {containers.map((container) => (
                <option key={container.name} value={container.name}>
                  {container.name} ({container.columnCount} column{container.columnCount !== 1 ? 's' : ''})
                </option>
              ))}
            </select>
            <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
          </div>
          {loadingSchema && (
            <p className="text-xs text-muted-foreground mt-1">Loading containers...</p>
          )}
          {!loadingSchema && containers.length === 0 && selectedDatabase && (
            <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
              No containers found in this database
            </p>
          )}
        </div>
      )}

      {!allowContainerSelection && selectedDatabase && (
        <p className="text-xs text-muted-foreground mt-2">
          New container will be created in this database
        </p>
      )}
    </div>
  );
}

