'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useDatabaseSchema, useDatabase } from '@/lib/hooks/useDatabases';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Database, ArrowLeft, AlertCircle, X } from 'lucide-react';
import Link from 'next/link';
import { SchemaOverview } from '@/components/databases/schema/SchemaOverview';
import { TableCard } from '@/components/databases/schema/TableCard';
import { DeploySchemaDialog } from '@/components/databases/schema/DeploySchemaDialog';
import { ModifyTableDialog } from '@/components/databases/schema/ModifyTableDialog';
import { AddColumnDialog } from '@/components/databases/schema/AddColumnDialog';
import { ModifyColumnDialog } from '@/components/databases/schema/ModifyColumnDialog';
import type { SchemaColumn } from '@/lib/api/types';

interface SchemaPageProps {
  params: Promise<{
    workspaceId: string;
    databaseName: string;
  }>;
}

export default function SchemaPage({ params }: SchemaPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [databaseName, setDatabaseName] = useState<string>('');
  const [showDeployDialog, setShowDeployDialog] = useState(false);
  const [showModifyTableDialog, setShowModifyTableDialog] = useState(false);
  const [showAddColumnDialog, setShowAddColumnDialog] = useState(false);
  const [showModifyColumnDialog, setShowModifyColumnDialog] = useState(false);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [selectedColumn, setSelectedColumn] = useState<SchemaColumn | null>(null);
  const [tableFilter, setTableFilter] = useState<string>('');
  const { showToast } = useToast();
  const router = useRouter();

  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wid, databaseName: dbName }) => {
      setWorkspaceId(wid);
      setDatabaseName(dbName);
    });
  }, [params]);

  const { database, isLoading: isDatabaseLoading } = useDatabase(workspaceId, databaseName);
  const { schema, isLoading: isSchemaLoading, error, refetch } = useDatabaseSchema(workspaceId, databaseName);

  const isLoading = isDatabaseLoading || isSchemaLoading;

  // Filter tables based on search query
  const filteredTables = schema?.tables?.filter((table) =>
    table.name.toLowerCase().includes(tableFilter.toLowerCase())
  ) || [];

  // Handler functions for dialogs
  const handleDeploySchema = async (repoName: string, branchName: string, paradigm?: string) => {
    try {
      // TODO: Implement actual API call to deploy schema
      console.log('Deploy schema:', { repoName, branchName, paradigm });
      showToast({
        type: 'success',
        title: 'Schema Deployed',
        message: `Schema deployed to ${repoName}/${branchName}`,
      });
    } catch (err) {
      showToast({
        type: 'error',
        title: 'Deployment Failed',
        message: err instanceof Error ? err.message : 'Failed to deploy schema',
      });
    }
  };

  const handleModifyTable = async (tableName: string, newName?: string, comment?: string) => {
    try {
      // TODO: Implement actual API call to modify table
      console.log('Modify table:', { tableName, newName, comment });
      showToast({
        type: 'success',
        title: 'Table Modified',
        message: `Table ${tableName} has been updated`,
      });
      refetch();
    } catch (err) {
      showToast({
        type: 'error',
        title: 'Modification Failed',
        message: err instanceof Error ? err.message : 'Failed to modify table',
      });
    }
  };

  const handleAddColumn = async (tableName: string, columnDef: any) => {
    try {
      // TODO: Implement actual API call to add column
      console.log('Add column:', { tableName, columnDef });
      showToast({
        type: 'success',
        title: 'Column Added',
        message: `Column ${columnDef.name} added to ${tableName}`,
      });
      refetch();
    } catch (err) {
      showToast({
        type: 'error',
        title: 'Addition Failed',
        message: err instanceof Error ? err.message : 'Failed to add column',
      });
    }
  };

  const handleModifyColumn = async (tableName: string, columnName: string, modifications: any) => {
    try {
      // TODO: Implement actual API call to modify column
      console.log('Modify column:', { tableName, columnName, modifications });
      showToast({
        type: 'success',
        title: 'Column Modified',
        message: `Column ${columnName} has been updated`,
      });
      refetch();
    } catch (err) {
      showToast({
        type: 'error',
        title: 'Modification Failed',
        message: err instanceof Error ? err.message : 'Failed to modify column',
      });
    }
  };

  const handleDropColumn = async (tableName: string, columnName: string) => {
    if (!confirm(`Are you sure you want to drop column "${columnName}" from table "${tableName}"? This action cannot be undone.`)) {
      return;
    }

    try {
      // TODO: Implement actual API call to drop column
      console.log('Drop column:', { tableName, columnName });
      showToast({
        type: 'success',
        title: 'Column Dropped',
        message: `Column ${columnName} has been removed from ${tableName}`,
      });
      refetch();
    } catch (err) {
      showToast({
        type: 'error',
        title: 'Drop Failed',
        message: err instanceof Error ? err.message : 'Failed to drop column',
      });
    }
  };

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing Schema',
      message: 'Fetching latest database schema...',
    });
  };

  // Loading state
  if (!workspaceId || !databaseName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/databases`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h2 className="text-3xl font-bold text-foreground">Database Schema</h2>
            <p className="text-muted-foreground mt-2">
              {databaseName}
            </p>
          </div>
        </div>

        {/* Error Display */}
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <AlertCircle className="h-12 w-12 mx-auto" />
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Schema</h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <button
            onClick={refetch}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Loading state
  if (isLoading || !schema) {
    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/databases`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h2 className="text-3xl font-bold text-foreground">Database Schema</h2>
            <p className="text-muted-foreground mt-2">
              {databaseName}
            </p>
          </div>
        </div>

        {/* Loading Skeletons */}
        <div className="space-y-4">
          <div className="h-32 bg-muted animate-pulse rounded-lg"></div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-32 bg-muted animate-pulse rounded-lg"></div>
            ))}
          </div>
          <div className="h-64 bg-muted animate-pulse rounded-lg"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Link
          href={`/workspaces/${workspaceId}/databases`}
          className="p-2 hover:bg-accent rounded-md transition-colors"
        >
          <ArrowLeft className="h-5 w-5" />
        </Link>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
              <Database className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">Database Schema</h2>
              <p className="text-muted-foreground mt-1">
                {databaseName} {database?.database_vendor && `• ${database.database_vendor}`}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Schema Overview */}
      <SchemaOverview
        schema={schema}
        databaseName={databaseName}
        onRefresh={handleRefresh}
        onDeploySchema={() => setShowDeployDialog(true)}
        isRefreshing={isSchemaLoading}
      />

      {/* Tables List */}
      {schema.tables && schema.tables.length > 0 ? (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-xl font-semibold text-foreground">
              Tables ({filteredTables.length}{filteredTables.length !== schema.tables.length ? ` of ${schema.tables.length}` : ''})
            </h3>
            <div className="flex items-center gap-3">
              {/* Search/Filter Input */}
              <div className="relative">
                <input
                  type="text"
                  placeholder="Filter tables..."
                  value={tableFilter}
                  onChange={(e) => setTableFilter(e.target.value)}
                  className="w-64 px-3 py-2 pl-9 bg-background border border-input rounded-md text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                />
                <svg
                  className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                  />
                </svg>
                {tableFilter && (
                  <button
                    onClick={() => setTableFilter('')}
                    className="absolute right-2 top-1/2 -translate-y-1/2 p-1 hover:bg-accent rounded transition-colors"
                    title="Clear filter"
                  >
                    <X className="h-3 w-3 text-muted-foreground" />
                  </button>
                )}
              </div>
            </div>
          </div>

          {filteredTables.length > 0 ? (
            <div className="grid grid-cols-1 gap-6">
              {filteredTables.map((table, index) => (
                <TableCard
                  key={`${table.name}-${index}`}
                  table={table}
                  onModifyTable={(tableName) => {
                    setSelectedTable(tableName);
                    setShowModifyTableDialog(true);
                  }}
                  onAddColumn={(tableName) => {
                    setSelectedTable(tableName);
                    setShowAddColumnDialog(true);
                  }}
                  onModifyColumn={(tableName, columnName) => {
                    const column = table.columns.find((c) => c.name === columnName);
                    if (column) {
                      setSelectedTable(tableName);
                      setSelectedColumn(column);
                      setShowModifyColumnDialog(true);
                    }
                  }}
                  onDropColumn={handleDropColumn}
                />
              ))}
            </div>
          ) : (
            <div className="bg-card border border-border rounded-lg p-8 text-center">
              <Database className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
              <h3 className="text-lg font-semibold text-foreground mb-1">No Tables Match Filter</h3>
              <p className="text-muted-foreground text-sm mb-4">
                No tables found matching "{tableFilter}"
              </p>
              <button
                onClick={() => setTableFilter('')}
                className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              >
                Clear Filter
              </button>
            </div>
          )}
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Database className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">No Tables Found</h3>
          <p className="text-muted-foreground">
            This database doesn't have any tables yet, or the schema couldn't be detected.
          </p>
        </div>
      )}

      {/* Dialogs */}
      {showDeployDialog && (
        <DeploySchemaDialog
          databaseName={databaseName}
          onClose={() => setShowDeployDialog(false)}
          onDeploy={handleDeploySchema}
        />
      )}

      {showModifyTableDialog && selectedTable && (
        <ModifyTableDialog
          tableName={selectedTable}
          onClose={() => {
            setShowModifyTableDialog(false);
            setSelectedTable('');
          }}
          onModify={handleModifyTable}
        />
      )}

      {showAddColumnDialog && selectedTable && (
        <AddColumnDialog
          tableName={selectedTable}
          onClose={() => {
            setShowAddColumnDialog(false);
            setSelectedTable('');
          }}
          onAdd={handleAddColumn}
        />
      )}

      {showModifyColumnDialog && selectedTable && selectedColumn && (
        <ModifyColumnDialog
          tableName={selectedTable}
          column={selectedColumn}
          onClose={() => {
            setShowModifyColumnDialog(false);
            setSelectedTable('');
            setSelectedColumn(null);
          }}
          onModify={handleModifyColumn}
        />
      )}
    </div>
  );
}

