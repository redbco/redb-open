'use client';

import { useState, useRef } from 'react';
import { useRouter } from 'next/navigation';
import { X, AlertCircle } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { MappingCreationWizard, MappingCreationData, WizardStep, MappingCreationWizardHandle } from './MappingCreationWizard';
import { DatabaseMappingWizard, DatabaseMappingData } from './DatabaseMappingWizard';
import { MappingTypeSelector, MappingFlowType } from './MappingTypeSelector';

interface CreateMappingDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function CreateMappingDialog({ workspaceId, onClose, onSuccess }: CreateMappingDialogProps) {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // Mapping type selection
  const [mappingFlowType, setMappingFlowType] = useState<MappingFlowType | null>(null);
  
  // Container mapping data
  const [containerMappingData, setContainerMappingData] = useState<MappingCreationData | null>(null);
  
  // Database mapping data
  const [databaseMappingData, setDatabaseMappingData] = useState<DatabaseMappingData | null>(null);

  // Track wizard step for container mappings
  const [containerWizardStep, setContainerWizardStep] = useState<WizardStep>('select-types');
  const wizardRef = useRef<MappingCreationWizardHandle>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (mappingFlowType === 'container' && !containerMappingData) {
      setError('Please complete all required fields for the container mapping');
      return;
    }

    if (mappingFlowType === 'database' && !databaseMappingData) {
      setError('Please complete all required fields for the database mapping');
      return;
    }

    setError(null);
    setIsLoading(true);

    try {
      if (mappingFlowType === 'container' && containerMappingData) {
        // Check if this is a new target container mapping
        if (containerMappingData.isNewTarget) {
          // Use the new endpoint for creating mapping with table deployment
          const request = {
            mapping_name: containerMappingData.mappingName,
            mapping_description: containerMappingData.mappingDescription,
            source_database_name: containerMappingData.source.databaseName!,
            source_table_name: containerMappingData.source.tableName!,
            target_database_name: containerMappingData.target.databaseName!,
            target_table_name: containerMappingData.target.tableName!,
          };

          const response = await api.mappings.createWithDeploy(workspaceId, request);
          
          // Navigate to the newly created mapping detail page
          router.push(`/workspaces/${workspaceId}/mappings/${encodeURIComponent(response.mapping.mapping_name)}`);
          
          onSuccess();
        } else {
          // Use existing endpoint for mapping to existing container
          const request = {
            mapping_name: containerMappingData.mappingName,
            mapping_description: containerMappingData.mappingDescription,
            scope: 'table',
            source: containerMappingData.source.uri,
            target: containerMappingData.target.uri,
          };

          const response = await api.mappings.create(workspaceId, request);
          
          // Navigate to the newly created mapping detail page
          router.push(`/workspaces/${workspaceId}/mappings/${encodeURIComponent(response.mapping.mapping_name)}`);
          
          onSuccess();
        }
      } else if (mappingFlowType === 'database' && databaseMappingData) {
        // Create multiple container mappings for database-to-database
        // This will create one mapping for each container in the source database
        const createdMappings = [];

        for (const containerMapping of databaseMappingData.containerMappings) {
          const mappingName = `${databaseMappingData.sourceDatabaseName}_${containerMapping.sourceContainerName}_to_${databaseMappingData.targetDatabaseName}_${containerMapping.targetContainerName}`;
          
          const request = {
            mapping_name: mappingName.toLowerCase().replace(/[^a-z0-9_]/g, '_'),
            mapping_description: `Auto-generated mapping from ${databaseMappingData.sourceDatabaseName}.${containerMapping.sourceContainerName} to ${databaseMappingData.targetDatabaseName}.${containerMapping.targetContainerName}`,
            scope: 'table',
            source: `redb://data/database/${databaseMappingData.sourceDatabaseId}/table/${containerMapping.sourceContainerName}`,
            target: `redb://data/database/${databaseMappingData.targetDatabaseId}/table/${containerMapping.targetContainerName}`,
          };

          try {
            const response = await api.mappings.create(workspaceId, request);
            createdMappings.push(response.mapping);
          } catch (err) {
            console.error(`Failed to create mapping for ${containerMapping.sourceContainerName}:`, err);
            // Continue with other mappings even if one fails
          }
        }

        if (createdMappings.length > 0) {
          // Navigate to the first created mapping
          router.push(`/workspaces/${workspaceId}/mappings/${encodeURIComponent(createdMappings[0].mapping_name)}`);
          onSuccess();
        } else {
          throw new Error('Failed to create any mappings');
        }
      }
    } catch (err) {
      console.error('Failed to create mapping:', err);
      setError(err instanceof Error ? err.message : 'Failed to create mapping');
    } finally {
      setIsLoading(false);
    }
  };

  const canSubmit = 
    (mappingFlowType === 'container' && containerMappingData !== null && !isLoading) ||
    (mappingFlowType === 'database' && databaseMappingData !== null && !isLoading);

  // Handle back button click
  const handleBack = () => {
    // For container mapping, check if we're in the wizard
    if (mappingFlowType === 'container') {
      if (containerWizardStep === 'select-resources' && wizardRef.current) {
        // If on step 2, go back to step 1 within wizard
        wizardRef.current.goBackOneStep();
        return;
      }
    }
    
    // Otherwise, go back to type selection
    setMappingFlowType(null);
    setContainerMappingData(null);
    setDatabaseMappingData(null);
    setContainerWizardStep('select-types');
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-6xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border sticky top-0 bg-card z-10">
          <div>
            <h2 className="text-2xl font-bold text-foreground">
              {!mappingFlowType && 'Create Mapping'}
              {mappingFlowType === 'container' && 'Create Container Mapping'}
              {mappingFlowType === 'database' && 'Create Database Mapping'}
            </h2>
            <p className="text-sm text-muted-foreground mt-1">
              {!mappingFlowType && 'Choose a mapping type to get started'}
              {mappingFlowType === 'container' && 'Map data between individual containers'}
              {mappingFlowType === 'database' && 'Map entire databases with bulk operations'}
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
            disabled={isLoading}
          >
            <X className="h-6 w-6" />
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="p-6 space-y-6">
            {error && (
              <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4 flex items-start">
                <AlertCircle className="h-5 w-5 text-destructive mt-0.5 mr-3 flex-shrink-0" />
                <div>
                  <h4 className="text-sm font-medium text-destructive">Error</h4>
                  <p className="text-sm text-destructive/80 mt-1">{error}</p>
                </div>
              </div>
            )}

            {/* Mapping Type Selection */}
            {!mappingFlowType && (
              <MappingTypeSelector
                selected={mappingFlowType}
                onSelect={setMappingFlowType}
              />
            )}

            {/* Container Mapping Wizard */}
            {mappingFlowType === 'container' && (
              <MappingCreationWizard
                ref={wizardRef}
                workspaceId={workspaceId}
                onMappingDataChange={setContainerMappingData}
                onStepChange={setContainerWizardStep}
              />
            )}

            {/* Database Mapping Wizard */}
            {mappingFlowType === 'database' && (
              <>
                <div className="flex items-center justify-between pb-3 border-b border-border">
                  <h3 className="text-lg font-semibold text-foreground">Database Mapping</h3>
                  <button
                    type="button"
                    onClick={() => {
                      setMappingFlowType(null);
                      setDatabaseMappingData(null);
                    }}
                    className="text-sm text-muted-foreground hover:text-foreground"
                    disabled={isLoading}
                  >
                    Change Type
                  </button>
                </div>
                <DatabaseMappingWizard
                  workspaceId={workspaceId}
                  onMappingDataChange={setDatabaseMappingData}
                />
              </>
            )}
          </div>

          {/* Footer */}
          {mappingFlowType && (
            <div className="flex items-center justify-between p-6 border-t border-border bg-muted/50 sticky bottom-0">
              <div className="flex items-center space-x-3">
                <button
                  type="button"
                  onClick={handleBack}
                  className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors text-foreground"
                  disabled={isLoading}
                >
                  Back
                </button>
              </div>
              <div className="flex items-center gap-3">
                <div className="text-sm text-muted-foreground">
                  {!canSubmit && 'Complete all required fields to create the mapping'}
                  {mappingFlowType === 'database' && databaseMappingData && (
                    <span className="font-medium">
                      {databaseMappingData.containerMappings.length} mappings will be created
                    </span>
                  )}
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    type="button"
                    onClick={onClose}
                    className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors text-foreground"
                    disabled={isLoading}
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
                    disabled={!canSubmit}
                  >
                    {isLoading ? (
                      <>
                        <LoadingSpinner size="sm" className="mr-2" />
                        Creating...
                      </>
                    ) : mappingFlowType === 'database' ? (
                      `Create ${databaseMappingData?.containerMappings.length || 0} Mappings`
                    ) : (
                      'Create Mapping'
                    )}
                  </button>
                </div>
              </div>
            </div>
          )}
        </form>
      </div>
    </div>
  );
}
