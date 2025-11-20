'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useCommit } from '@/lib/hooks/useCommit';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { GitCommit, ArrowLeft, AlertCircle } from 'lucide-react';
import Link from 'next/link';
import { CommitSchemaOverview } from '@/components/repositories/CommitSchemaOverview';
import { TableCard } from '@/components/databases/schema/TableCard';
import type { DatabaseSchema } from '@/lib/api/types';

interface CommitSchemaPageProps {
  params: Promise<{
    workspaceId: string;
    repoName: string;
    branchName: string;
    commitCode: string;
  }>;
}

export default function CommitSchemaPage({ params }: CommitSchemaPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [repoName, setRepoName] = useState<string>('');
  const [branchName, setBranchName] = useState<string>('');
  const [commitCode, setCommitCode] = useState<string>('');
  const router = useRouter();
  const { showToast } = useToast();

  useEffect(() => {
    params.then(({ workspaceId, repoName, branchName, commitCode }) => {
      setWorkspaceId(workspaceId);
      setRepoName(decodeURIComponent(repoName));
      setBranchName(decodeURIComponent(branchName));
      setCommitCode(decodeURIComponent(commitCode));
    });
  }, [params]);

  const { commit, isLoading, error } = useCommit(workspaceId, repoName, branchName, commitCode);

  if (!workspaceId || !repoName || !branchName || !commitCode) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  // Handle back navigation
  const handleViewHistory = () => {
    router.push(`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`);
  };

  const handleDeploySchema = () => {
    // TODO: Implement deploy schema functionality
    showToast({ type: 'info', title: 'Coming Soon', message: 'Deploy schema functionality coming soon' });
  };

  // Parse schema structure from commit
  let schema: DatabaseSchema = { tables: [] };
  if (commit?.schema_structure) {
    try {
      let parsedSchema: any;
      if (typeof commit.schema_structure === 'string') {
        parsedSchema = JSON.parse(commit.schema_structure);
      } else {
        parsedSchema = commit.schema_structure;
      }
      
      // Convert tables from object to array format
      let tablesArray: any[] = [];
      if (parsedSchema.tables && typeof parsedSchema.tables === 'object') {
        tablesArray = Object.values(parsedSchema.tables).map((table: any) => {
          // Convert columns from object to array format and normalize field names
          let columnsArray: any[] = [];
          if (table.columns && typeof table.columns === 'object') {
            columnsArray = Object.values(table.columns).map((col: any) => ({
              name: col.name,
              dataType: col.data_type || col.dataType,
              type: col.data_type || col.type,
              isArray: col.is_array || col.isArray,
              isUnique: col.is_unique || col.isUnique,
              isNullable: col.nullable !== undefined ? col.nullable : col.isNullable !== undefined ? col.isNullable : true,
              isGenerated: col.is_generated || col.isGenerated,
              isPrimaryKey: col.is_primary_key || col.isPrimaryKey,
              is_primary_key: col.is_primary_key || col.isPrimaryKey,
              columnDefault: col.default || col.columnDefault || col.column_default,
              column_default: col.default || col.column_default,
              isAutoIncrement: col.auto_increment || col.isAutoIncrement || col.is_auto_increment,
              is_auto_increment: col.auto_increment || col.is_auto_increment,
              varcharLength: col.varchar_length || col.varcharLength,
              varchar_length: col.varchar_length,
              dataCategory: col.data_category || col.dataCategory,
              data_category: col.data_category,
              isPrivilegedData: col.is_privileged_data || col.isPrivilegedData,
              is_privileged_data: col.is_privileged_data,
              privilegedConfidence: col.privileged_confidence || col.privilegedConfidence,
              privileged_confidence: col.privileged_confidence,
              privilegedDescription: col.privileged_description || col.privilegedDescription,
              privileged_description: col.privileged_description,
            }));
          }
          
          // Convert constraints from object to proper format if needed
          let constraintsArray = table.constraints;
          if (table.constraints && typeof table.constraints === 'object' && !Array.isArray(table.constraints)) {
            // Keep as object but ensure it's in the right format
            constraintsArray = table.constraints;
          }
          
          // Convert indexes from object to proper format if needed
          let indexesArray = table.indexes;
          if (table.indexes && typeof table.indexes === 'object' && !Array.isArray(table.indexes)) {
            indexesArray = table.indexes;
          }
          
          return {
            name: table.name,
            schema: table.schema,
            engine: table.engine,
            columns: columnsArray,
            indexes: indexesArray,
            tableType: table.table_type || table.tableType,
            table_type: table.table_type,
            primaryKey: table.primary_key || table.primaryKey,
            primaryCategory: table.primary_category || table.primaryCategory,
            primary_category: table.primary_category,
            constraints: constraintsArray,
            classificationScores: table.classification_scores || table.classificationScores,
            classification_scores: table.classification_scores,
            classificationConfidence: table.classification_confidence || table.classificationConfidence,
            classification_confidence: table.classification_confidence,
          };
        });
      }
      
      // Ensure schema has the expected structure
      schema = {
        tables: tablesArray,
        schemas: parsedSchema.schemas,
        triggers: parsedSchema.triggers,
        enumTypes: parsedSchema.enumTypes || parsedSchema.types, // Handle both field names
        enums: parsedSchema.enums,
        functions: parsedSchema.functions,
        sequences: parsedSchema.sequences,
        extensions: parsedSchema.extensions,
        views: parsedSchema.views,
        procedures: parsedSchema.procedures,
      };
    } catch (err) {
      console.error('[CommitSchemaPage] Error parsing schema structure:', err);
      console.error('[CommitSchemaPage] Schema structure:', commit.schema_structure);
    }
  }

  // Check if this commit is HEAD (is_head flag or inferred from commit data)
  // Note: The backend should provide is_head flag, but we need to check the actual field name
  const isHead = !!(commit as any)?.is_head || !!(commit as any)?.isHead;
  
  // Check if this commit is deployed
  // Assumption: If the branch has an attached database, the HEAD commit is deployed
  // This is a simplified check - in a real implementation, we'd need backend support
  const isDeployed = isHead; // Simplified for now

  if (error) {
    return (
      <div className="space-y-6">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h2 className="text-3xl font-bold text-foreground">Commit Schema</h2>
            <p className="text-muted-foreground mt-1">
              {repoName} • {branchName} • {commitCode}
            </p>
          </div>
        </div>

        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-6">
          <div className="flex items-start gap-3">
            <AlertCircle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-destructive font-semibold">Error Loading Commit</p>
              <p className="text-destructive/80 text-sm mt-1">{error.message}</p>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (isLoading || !commit) {
    return (
      <div className="space-y-6">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h2 className="text-3xl font-bold text-foreground">Commit Schema</h2>
            <p className="text-muted-foreground mt-1">Loading commit details...</p>
          </div>
        </div>

        <div className="flex items-center justify-center min-h-[400px]">
          <LoadingSpinner size="lg" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Link
          href={`/workspaces/${workspaceId}/repositories/${encodeURIComponent(repoName)}/branches/${encodeURIComponent(branchName)}`}
          className="p-2 hover:bg-accent rounded-md transition-colors"
        >
          <ArrowLeft className="h-5 w-5" />
        </Link>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
              <GitCommit className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">Commit Schema</h2>
              <p className="text-muted-foreground mt-1">
                {repoName} • {branchName} • {commitCode}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Schema Overview */}
      <CommitSchemaOverview
        schema={schema}
        commitCode={commitCode}
        commitMessage={commit.commit_message}
        commitDate={commit.created}
        branchName={branchName}
        repoName={repoName}
        isHead={isHead}
        isDeployed={isDeployed}
        onDeploySchema={handleDeploySchema}
        onViewHistory={handleViewHistory}
      />

      {/* Tables List */}
      {schema.tables && Array.isArray(schema.tables) && schema.tables.length > 0 ? (
        <div className="space-y-4">
          <h3 className="text-xl font-semibold text-foreground">
            Tables ({schema.tables.length})
          </h3>
          {schema.tables.map((table, index) => (
            <TableCard
              key={`${table.name}-${index}`}
              table={table}
              // Note: We don't provide modification callbacks for version-controlled view
              // In the future, these could create new commits
            />
          ))}
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <AlertCircle className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-xl font-semibold text-foreground mb-2">No Schema Data</h3>
          <p className="text-muted-foreground">
            This commit does not contain any table definitions
          </p>
          {commit?.schema_structure && (
            <p className="text-xs text-muted-foreground mt-2">
              Schema structure type: {typeof commit.schema_structure}
            </p>
          )}
        </div>
      )}

      {/* Future Enhancement Notice */}
      <div className="bg-blue-50 dark:bg-blue-900/10 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0 w-5 h-5 rounded-full bg-blue-600 dark:bg-blue-400 flex items-center justify-center mt-0.5">
            <span className="text-white text-xs font-bold">i</span>
          </div>
          <div className="flex-1">
            <p className="text-sm text-blue-900 dark:text-blue-100 font-medium">
              Schema Editing Coming Soon
            </p>
            <p className="text-sm text-blue-800 dark:text-blue-200 mt-1">
              In a future release, you&apos;ll be able to modify this schema directly. 
              Any changes will automatically create a new commit, preserving the version history.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

