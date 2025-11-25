'use client';

import { useState, useEffect, useMemo } from 'react';
import { useDatabase } from '@/lib/hooks/useDatabases';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Database, ArrowLeft, AlertCircle, X, RefreshCw } from 'lucide-react';
import Link from 'next/link';
import { DatabaseIcon } from '@/components/databases/DatabaseIcon';
import { formatDatabaseType } from '@/lib/formatters';
import { SchemaOverview } from '@/components/databases/schema/SchemaOverview';
import { TableCard } from '@/components/databases/schema/TableCard';
import { DeploySchemaDialog } from '@/components/databases/schema/DeploySchemaDialog';
import { ModifyTableDialog } from '@/components/databases/schema/ModifyTableDialog';
import { AddColumnDialog } from '@/components/databases/schema/AddColumnDialog';
import { ModifyColumnDialog } from '@/components/databases/schema/ModifyColumnDialog';
import { DatabaseCommitTimeline } from '@/components/databases/DatabaseCommitTimeline';
import type { SchemaColumn } from '@/lib/api/types';

// Type for schema items from API response
interface SchemaItemResponse {
  item_name: string;
  item_display_name?: string;
  data_type: string;
  unified_data_type?: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  is_unique: boolean;
  is_indexed: boolean;
  is_required: boolean;
  is_array: boolean;
  default_value?: string;
  constraints?: Array<Record<string, unknown>>;
  is_privileged: boolean;
  privileged_classification?: string;
  detection_confidence?: number;
  detection_method?: string;
  ordinal_position: number;
  max_length?: number;
  precision?: number;
  scale?: number;
  item_comment?: string;
}

interface ContainerResponse {
  object_name: string;
  object_type: string;
  database_type?: string;
  container_classification?: string;
  container_classification_confidence?: number;
  container_classification_source?: string;
  item_count?: number;
  status?: string;
  items?: SchemaItemResponse[];
}

interface TableResponse {
  name: string;
  columns: SchemaColumn[];
}

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

  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wid, databaseName: dbName }) => {
      setWorkspaceId(wid);
      setDatabaseName(dbName);
    });
  }, [params]);

  const { database, isLoading, error, refetch } = useDatabase(workspaceId, databaseName);

  // Extract schema from database.resource_containers
  const schema = useMemo(() => {
    if (!database?.resource_containers) return null;
    return {
      containers: database.resource_containers,
    };
  }, [database]);

  // Extract commit timeline
  const commitTimeline = database?.commit_timeline || [];

  // Filter and sort containers/tables based on search query (alphabetically by name)
  const filteredTables = useMemo(() => {
    if (!schema?.containers) return [];
    
    return schema.containers
      .filter((container) => {
        return container.object_name.toLowerCase().includes(tableFilter.toLowerCase());
      })
      .sort((a, b) => a.object_name.localeCompare(b.object_name))
      .map((container) => ({
        name: container.object_name,
        object_type: container.object_type,
        database_type: container.database_type,
        container_classification: container.container_classification,
        container_classification_confidence: container.container_classification_confidence,
        container_classification_source: container.container_classification_source,
        item_count: container.item_count,
        status: container.status,
        columns: (container.items || []).map((item): SchemaColumn => ({
          name: item.item_name,
          dataType: item.data_type,
          data_type: item.data_type,
          isNullable: item.is_nullable,
          is_nullable: item.is_nullable,
          isPrimaryKey: item.is_primary_key,
          is_primary_key: item.is_primary_key,
          isUnique: item.is_unique,
          is_unique: item.is_unique,
          isIndexed: item.is_indexed,
          is_indexed: item.is_indexed,
          isArray: item.is_array,
          defaultValue: item.default_value,
          default_value: item.default_value,
          constraints: (item.constraints || []) as unknown as string[],
          isPrivileged: item.is_privileged,
          is_privileged: item.is_privileged,
          privilegedClassification: item.privileged_classification,
          privileged_classification: item.privileged_classification,
          detectionConfidence: item.detection_confidence,
          detection_confidence: item.detection_confidence,
          detectionMethod: item.detection_method,
          detection_method: item.detection_method,
          ordinalPosition: item.ordinal_position,
          ordinal_position: item.ordinal_position,
        })),
        primaryCategory: container.container_classification,
        classificationConfidence: container.container_classification_confidence,
      }));
  }, [schema, tableFilter]);

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

  const handleAddColumn = async (tableName: string, columnDef: Record<string, unknown>) => {
    try {
      // TODO: Implement actual API call to add column
      console.log('Add column:', { tableName, columnDef });
      showToast({
        type: 'success',
        title: 'Column Added',
        message: `Column ${columnDef.name as string} added to ${tableName}`,
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

  const handleModifyColumn = async (tableName: string, columnName: string, modifications: Record<string, unknown>) => {
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
      <div className="flex items-center justify-between bg-gradient-to-r from-blue-50/50 to-transparent dark:from-blue-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/databases`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900 rounded-xl flex items-center justify-center border border-border shadow-sm">
              <DatabaseIcon type={database?.database_type || ''} className="h-7 w-7" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">{databaseName}</h2>
              <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                <span className="font-medium">
                  {database?.database_type ? formatDatabaseType(database.database_type) : 'Database'}
                </span>
                {database?.instance_name && (
                  <>
                    <span>•</span>
                    <Link
                      href={`/workspaces/${workspaceId}/instances/${database.instance_name}`}
                      className="hover:underline hover:text-primary transition-colors"
                    >
                      {database.instance_name}
                    </Link>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Schema Overview */}
      <SchemaOverview
        schema={schema}
        database={database || undefined}
        workspaceId={workspaceId}
      />

      {/* Commit Timeline */}
      <DatabaseCommitTimeline
        commitTimeline={commitTimeline}
        workspaceId={workspaceId}
        repoName={database?.connected_repo_name}
        branchName={database?.connected_branch_name}
      />

      {/* Tables List */}
      {schema?.containers && schema.containers.length > 0 ? (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-xl font-semibold text-foreground">
              Containers ({filteredTables.length}{filteredTables.length !== schema.containers.length ? ` of ${schema.containers.length}` : ''})
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
                  onDeployTable={(tableName) => {
                    // TODO: Implement deploy to database functionality
                    console.log('Deploy table:', tableName);
                  }}
                  onDropTable={(tableName) => {
                    // TODO: Implement drop table functionality
                    console.log('Drop table:', tableName);
                  }}
                  onWipeTable={(tableName) => {
                    // TODO: Implement wipe table data functionality
                    console.log('Wipe table:', tableName);
                  }}
                  onModifyColumn={(tableName, columnName) => {
                    const column = table.columns.find((c: SchemaColumn) => c.name === columnName);
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
                No tables found matching &ldquo;{tableFilter}&rdquo;
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
            This database doesn&apos;t have any tables yet, or the schema couldn&apos;t be detected.
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

