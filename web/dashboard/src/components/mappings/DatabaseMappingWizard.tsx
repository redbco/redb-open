'use client';

import { useState, useEffect } from 'react';
import { AlertCircle, ArrowRight, CheckCircle2, Info } from 'lucide-react';
import { ResourceSelection, ContainerType } from '@/lib/api/types';
import { useDatabases } from '@/lib/hooks/useResources';
import { useDatabaseSchemaInfo } from '@/lib/hooks/useDatabaseSchemaInfo';
import { useDatabaseCapabilities } from '@/lib/hooks/useDatabaseCapabilities';
import {
  detectContainerType,
  getContainerTypeName,
  areContainerTypesCompatible,
  getSupportedContainerTypes,
} from '@/lib/utils/container-type-detector';
import { ContainerTypeBadge } from './ContainerTypeBadge';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';

interface DatabaseMappingWizardProps {
  workspaceId: string;
  onMappingDataChange: (data: DatabaseMappingData | null) => void;
}

export interface DatabaseMappingData {
  sourceDatabaseId: string;
  sourceDatabaseName: string;
  sourceDatabaseType?: string;
  targetDatabaseId: string;
  targetDatabaseName: string;
  targetDatabaseType?: string;
  containerMappings: ContainerMapping[];
}

export interface ContainerMapping {
  sourceContainerName: string;
  sourceContainerType: ContainerType;
  targetContainerName: string;
  targetContainerType: ContainerType;
  autoMap: boolean;
}

type WizardStep = 'select-databases' | 'configure-mappings' | 'review';

export function DatabaseMappingWizard({
  workspaceId,
  onMappingDataChange,
}: DatabaseMappingWizardProps) {
  const [currentStep, setCurrentStep] = useState<WizardStep>('select-databases');

  // Step 1: Database selection
  const [sourceDatabase, setSourceDatabase] = useState<ResourceSelection | null>(null);
  const [targetDatabase, setTargetDatabase] = useState<ResourceSelection | null>(null);

  // Step 2: Container mappings
  const [containerMappings, setContainerMappings] = useState<ContainerMapping[]>([]);

  // Data fetching
  const { databases, isLoading: loadingDatabases } = useDatabases(workspaceId);
  const { schema: sourceSchema, isLoading: loadingSourceSchema } = useDatabaseSchemaInfo(
    workspaceId,
    sourceDatabase?.databaseName || ''
  );
  const { schema: targetSchema, isLoading: loadingTargetSchema } = useDatabaseSchemaInfo(
    workspaceId,
    targetDatabase?.databaseName || ''
  );

  const { capabilities: sourceCapabilities } = useDatabaseCapabilities(
    sourceDatabase?.databaseType
  );
  const { capabilities: targetCapabilities } = useDatabaseCapabilities(
    targetDatabase?.databaseType
  );

  // Auto-generate container mappings when both databases are selected
  useEffect(() => {
    if (
      sourceDatabase &&
      targetDatabase &&
      sourceSchema?.tables &&
      sourceCapabilities &&
      targetCapabilities
    ) {
      const sourceParadigm = sourceCapabilities.paradigms[0];
      const targetParadigm = targetCapabilities.paradigms[0];

      const newMappings: ContainerMapping[] = sourceSchema.tables.map((table) => {
        const sourceContainerType = detectContainerType(
          sourceDatabase.databaseType,
          sourceParadigm
        );

        // Try to find a matching target container
        let targetContainerName = table.name;
        let targetContainerType: ContainerType = detectContainerType(
          targetDatabase.databaseType,
          targetParadigm
        );

        // Check if there's a matching table in the target schema
        if (targetSchema?.tables) {
          const matchingTarget = targetSchema.tables.find((t) => t.name === table.name);
          if (matchingTarget) {
            targetContainerName = matchingTarget.name;
          }
        }

        return {
          sourceContainerName: table.name,
          sourceContainerType,
          targetContainerName,
          targetContainerType,
          autoMap: sourceContainerType === targetContainerType,
        };
      });

      setContainerMappings(newMappings);
    }
  }, [sourceDatabase, targetDatabase, sourceSchema, targetSchema, sourceCapabilities, targetCapabilities]);

  // Update parent component when data changes
  useEffect(() => {
    if (
      currentStep === 'review' &&
      sourceDatabase &&
      targetDatabase &&
      containerMappings.length > 0
    ) {
      onMappingDataChange({
        sourceDatabaseId: sourceDatabase.databaseId!,
        sourceDatabaseName: sourceDatabase.databaseName!,
        sourceDatabaseType: sourceDatabase.databaseType,
        targetDatabaseId: targetDatabase.databaseId!,
        targetDatabaseName: targetDatabase.databaseName!,
        targetDatabaseType: targetDatabase.databaseType,
        containerMappings,
      });
    } else {
      onMappingDataChange(null);
    }
  }, [
    currentStep,
    sourceDatabase,
    targetDatabase,
    containerMappings,
    onMappingDataChange,
  ]);

  const handleSourceDatabaseSelect = (dbId: string, dbName: string, dbType?: string) => {
    setSourceDatabase({
      type: 'database',
      resourceId: dbId,
      resourceName: dbName,
      databaseId: dbId,
      databaseName: dbName,
      databaseType: dbType,
      uri: `redb://database/${dbId}`,
    });
  };

  const handleTargetDatabaseSelect = (dbId: string, dbName: string, dbType?: string) => {
    setTargetDatabase({
      type: 'database',
      resourceId: dbId,
      resourceName: dbName,
      databaseId: dbId,
      databaseName: dbName,
      databaseType: dbType,
      uri: `redb://database/${dbId}`,
    });
  };

  const handleContainerMappingChange = (
    index: number,
    field: keyof ContainerMapping,
    value: any
  ) => {
    const newMappings = [...containerMappings];
    newMappings[index] = { ...newMappings[index], [field]: value };
    setContainerMappings(newMappings);
  };

  const canProceedToMappings =
    sourceDatabase && targetDatabase && sourceSchema?.tables && sourceSchema.tables.length > 0;

  const canProceedToReview = containerMappings.length > 0;

  return (
    <div className="space-y-6">
      {/* Step Indicator */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <div
            className={`flex items-center justify-center h-8 w-8 rounded-full ${
              currentStep === 'select-databases'
                ? 'bg-primary text-primary-foreground'
                : 'bg-muted text-muted-foreground'
            }`}
          >
            {currentStep !== 'select-databases' ? (
              <CheckCircle2 className="h-5 w-5" />
            ) : (
              '1'
            )}
          </div>
          <span className="text-sm font-medium">Select Databases</span>
        </div>

        <ArrowRight className="h-4 w-4 text-muted-foreground" />

        <div className="flex items-center space-x-2">
          <div
            className={`flex items-center justify-center h-8 w-8 rounded-full ${
              currentStep === 'configure-mappings'
                ? 'bg-primary text-primary-foreground'
                : currentStep === 'review'
                ? 'bg-green-500 text-white'
                : 'bg-muted text-muted-foreground'
            }`}
          >
            {currentStep === 'review' ? <CheckCircle2 className="h-5 w-5" /> : '2'}
          </div>
          <span className="text-sm font-medium">Configure Mappings</span>
        </div>

        <ArrowRight className="h-4 w-4 text-muted-foreground" />

        <div className="flex items-center space-x-2">
          <div
            className={`flex items-center justify-center h-8 w-8 rounded-full ${
              currentStep === 'review'
                ? 'bg-primary text-primary-foreground'
                : 'bg-muted text-muted-foreground'
            }`}
          >
            3
          </div>
          <span className="text-sm font-medium">Review</span>
        </div>
      </div>

      {/* Step 1: Select Databases */}
      {currentStep === 'select-databases' && (
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Source Database */}
            <div className="p-4 border border-border rounded-lg bg-blue-50/50 dark:bg-blue-950/20">
              <h3 className="text-sm font-semibold text-foreground mb-3">Source Database</h3>
              {loadingDatabases ? (
                <LoadingSpinner size="sm" />
              ) : (
                <select
                  className="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground"
                  value={sourceDatabase?.databaseId || ''}
                  onChange={(e) => {
                    const db = databases.find((d) => d.database_id === e.target.value);
                    if (db) {
                      handleSourceDatabaseSelect(
                        db.database_id,
                        db.database_name,
                        db.database_type
                      );
                    }
                  }}
                >
                  <option value="">Select source database...</option>
                  {databases.map((db) => (
                    <option key={db.database_id} value={db.database_id}>
                      {db.database_name} ({db.database_type || 'unknown'})
                    </option>
                  ))}
                </select>
              )}
            </div>

            {/* Target Database */}
            <div className="p-4 border border-border rounded-lg bg-green-50/50 dark:bg-green-950/20">
              <h3 className="text-sm font-semibold text-foreground mb-3">Target Database</h3>
              {loadingDatabases ? (
                <LoadingSpinner size="sm" />
              ) : (
                <select
                  className="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground"
                  value={targetDatabase?.databaseId || ''}
                  onChange={(e) => {
                    const db = databases.find((d) => d.database_id === e.target.value);
                    if (db) {
                      handleTargetDatabaseSelect(
                        db.database_id,
                        db.database_name,
                        db.database_type
                      );
                    }
                  }}
                >
                  <option value="">Select target database...</option>
                  {databases.map((db) => (
                    <option key={db.database_id} value={db.database_id}>
                      {db.database_name} ({db.database_type || 'unknown'})
                    </option>
                  ))}
                </select>
              )}
            </div>
          </div>

          {sourceDatabase && targetDatabase && loadingSourceSchema && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <LoadingSpinner size="sm" />
              <span>Loading source database schema...</span>
            </div>
          )}

          {sourceDatabase && !loadingSourceSchema && (!sourceSchema?.tables || sourceSchema.tables.length === 0) && (
            <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg p-4">
              <div className="flex items-start gap-2">
                <AlertCircle className="h-5 w-5 text-amber-600 dark:text-amber-400 mt-0.5" />
                <div>
                  <h4 className="text-sm font-medium text-amber-900 dark:text-amber-100">
                    No containers found
                  </h4>
                  <p className="text-sm text-amber-700 dark:text-amber-300 mt-1">
                    The source database has no containers (tables, collections, etc.) to map.
                  </p>
                </div>
              </div>
            </div>
          )}

          <div className="flex justify-end">
            <button
              type="button"
              onClick={() => setCurrentStep('configure-mappings')}
              disabled={!canProceedToMappings}
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next: Configure Mappings
            </button>
          </div>
        </div>
      )}

      {/* Step 2: Configure Mappings */}
      {currentStep === 'configure-mappings' && (
        <div className="space-y-4">
          <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
            <div className="flex items-start gap-2">
              <Info className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5" />
              <div>
                <h4 className="text-sm font-medium text-blue-900 dark:text-blue-100">
                  Configure Container Mappings
                </h4>
                <p className="text-sm text-blue-700 dark:text-blue-300 mt-1">
                  Specify how each source container should be mapped to the target database.
                  Auto-mapping will be enabled for compatible container types.
                </p>
              </div>
            </div>
          </div>

          <div className="space-y-2 max-h-96 overflow-y-auto">
            {containerMappings.map((mapping, index) => {
              const compatibility = areContainerTypesCompatible(
                mapping.sourceContainerType,
                mapping.targetContainerType
              );

              return (
                <div
                  key={index}
                  className="p-3 border border-border rounded-lg bg-card flex items-center gap-3"
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <ContainerTypeBadge
                        type={mapping.sourceContainerType}
                        size="sm"
                      />
                      <span className="text-sm font-medium truncate">
                        {mapping.sourceContainerName}
                      </span>
                    </div>
                  </div>

                  <ArrowRight className="h-4 w-4 text-muted-foreground flex-shrink-0" />

                  <div className="flex-1 min-w-0">
                    <input
                      type="text"
                      value={mapping.targetContainerName}
                      onChange={(e) =>
                        handleContainerMappingChange(
                          index,
                          'targetContainerName',
                          e.target.value
                        )
                      }
                      className="w-full px-2 py-1 text-sm border border-border rounded bg-background"
                      placeholder="Target container name"
                    />
                  </div>

                  <select
                    value={mapping.targetContainerType}
                    onChange={(e) =>
                      handleContainerMappingChange(
                        index,
                        'targetContainerType',
                        e.target.value as ContainerType
                      )
                    }
                    className="px-2 py-1 text-sm border border-border rounded bg-background"
                  >
                    {getSupportedContainerTypes(
                      targetCapabilities?.paradigms[0] || 'relational'
                    ).map((type) => (
                      <option key={type} value={type}>
                        {getContainerTypeName(type)}
                      </option>
                    ))}
                  </select>

                  {!compatibility.compatible && (
                    <AlertCircle className="h-4 w-4 text-amber-500" aria-label={compatibility.warning} />
                  )}
                </div>
              );
            })}
          </div>

          <div className="flex justify-between">
            <button
              type="button"
              onClick={() => setCurrentStep('select-databases')}
              className="px-4 py-2 border border-border rounded-md hover:bg-accent"
            >
              Back
            </button>
            <button
              type="button"
              onClick={() => setCurrentStep('review')}
              disabled={!canProceedToReview}
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Next: Review
            </button>
          </div>
        </div>
      )}

      {/* Step 3: Review */}
      {currentStep === 'review' && (
        <div className="space-y-4">
          <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg p-4">
            <div className="flex items-start gap-2">
              <CheckCircle2 className="h-5 w-5 text-green-600 dark:text-green-400 mt-0.5" />
              <div>
                <h4 className="text-sm font-medium text-green-900 dark:text-green-100">
                  Review and Create
                </h4>
                <p className="text-sm text-green-700 dark:text-green-300 mt-1">
                  Review the mapping configuration below. This will create {containerMappings.length} individual mappings.
                </p>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4 p-4 border border-border rounded-lg bg-muted/30">
            <div>
              <span className="text-xs text-muted-foreground">Source Database</span>
              <p className="text-sm font-medium">{sourceDatabase?.databaseName}</p>
            </div>
            <div>
              <span className="text-xs text-muted-foreground">Target Database</span>
              <p className="text-sm font-medium">{targetDatabase?.databaseName}</p>
            </div>
          </div>

          <div>
            <h4 className="text-sm font-medium mb-2">
              Mappings to be created ({containerMappings.length})
            </h4>
            <div className="space-y-1 max-h-64 overflow-y-auto text-xs">
              {containerMappings.map((mapping, index) => (
                <div
                  key={index}
                  className="flex items-center gap-2 p-2 bg-muted/50 rounded"
                >
                  <span className="flex-1 font-mono truncate">
                    {mapping.sourceContainerName}
                  </span>
                  <ArrowRight className="h-3 w-3 text-muted-foreground" />
                  <span className="flex-1 font-mono truncate">
                    {mapping.targetContainerName}
                  </span>
                  <ContainerTypeBadge
                    type={mapping.sourceContainerType}
                    size="sm"
                    showLabel={false}
                  />
                  <ArrowRight className="h-3 w-3 text-muted-foreground" />
                  <ContainerTypeBadge
                    type={mapping.targetContainerType}
                    size="sm"
                    showLabel={false}
                  />
                </div>
              ))}
            </div>
          </div>

          <div className="flex justify-between">
            <button
              type="button"
              onClick={() => setCurrentStep('configure-mappings')}
              className="px-4 py-2 border border-border rounded-md hover:bg-accent"
            >
              Back
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

