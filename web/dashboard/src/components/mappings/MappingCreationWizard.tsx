'use client';

import { useState, useEffect, forwardRef, useImperativeHandle } from 'react';
import { ArrowRight, ArrowLeft, AlertCircle, Check } from 'lucide-react';
import { ResourceSelection, ResourceType } from '@/lib/api/types';
import { ResourceTypeSelector } from './ResourceTypeSelector';
import { ResourceCategorySelector, ResourceCategory } from './ResourceCategorySelector';
import { DatabaseContainerSelector } from './DatabaseContainerSelector';
import { TargetModeSelector, TargetMode } from './TargetModeSelector';
import { StreamSelector } from './StreamSelector';
import { validateMappingPair, getValidTargetTypes } from '@/lib/utils/mapping-validator';
import { generateMappingName, generateMappingDescription } from '@/lib/utils/mapping-name-generator';

interface MappingCreationWizardProps {
  workspaceId: string;
  onMappingDataChange: (data: MappingCreationData | null) => void;
  onStepChange?: (step: WizardStep) => void; // Expose current step to parent
}

export interface MappingCreationWizardHandle {
  goBackOneStep: () => void;
}

export interface MappingCreationData {
  mappingName: string;
  mappingDescription: string;
  source: ResourceSelection;
  target: ResourceSelection;
  generateRules: boolean;
  isNewTarget?: boolean; // Flag to indicate if target is a new container
}

export type WizardStep = 'select-types' | 'select-resources';

export const MappingCreationWizard = forwardRef<MappingCreationWizardHandle, MappingCreationWizardProps>(
  function MappingCreationWizard({
    workspaceId,
    onMappingDataChange,
    onStepChange,
  }, ref) {
    const [step, setStep] = useState<WizardStep>('select-types');
    
    // Expose goBackOneStep to parent via ref
    useImperativeHandle(ref, () => ({
      goBackOneStep: () => {
        if (step === 'select-resources') {
          setStep('select-types');
        }
      },
    }));
    
    // Notify parent when step changes
    useEffect(() => {
      onStepChange?.(step);
    }, [step, onStepChange]);
  
    // Source state
    const [sourceType, setSourceType] = useState<ResourceType | null>(null);
    const [sourceDatabase, setSourceDatabase] = useState<{ database: string; databaseId: string; container: string | null; databaseType?: string } | null>(null);
    const [sourceStream, setSourceStream] = useState<{ stream: string; topic: string | null } | null>(null);
  
    // Target state
    const [targetCategory, setTargetCategory] = useState<ResourceCategory | null>(null);
    const [targetType, setTargetType] = useState<ResourceType | null>(null);
    const [targetMode, setTargetMode] = useState<TargetMode | null>(null); // new vs existing
    const [targetDatabase, setTargetDatabase] = useState<{ database: string; databaseId: string; container: string | null; databaseType?: string } | null>(null);
    const [targetStream, setTargetStream] = useState<{ stream: string; topic: string | null } | null>(null);
    const [newTargetContainerName, setNewTargetContainerName] = useState<string>(''); // Name for new container
  
  const [generateRules, setGenerateRules] = useState(true);
  
  // Validation state
  const [validationError, setValidationError] = useState<string | null>(null);

  // Get allowed types based on category
  const getCategoryTypes = (category: ResourceCategory): ResourceType[] => {
    const containerTypes: ResourceType[] = [
      'tabular-record-set',
      'document',
      'keyvalue-item',
      'graph-node',
      'graph-relationship',
      'search-document',
      'vector',
      'timeseries-point',
      'blob-object',
    ];
    
    const endpointTypes: ResourceType[] = ['stream', 'webhook'];
    const aiTypes: ResourceType[] = ['mcp-resource', 'mcp-tool'];
    
    switch (category) {
      case 'containers':
        return containerTypes;
      case 'endpoints':
        return endpointTypes;
      case 'ai':
        return aiTypes;
      default:
        return [];
    }
  };

  // Filter allowed types for target
  const allowedTargetTypes = sourceType ? getValidTargetTypes(sourceType) : undefined;
  const filteredTargetTypes = targetCategory 
    ? getCategoryTypes(targetCategory).filter(t => !allowedTargetTypes || allowedTargetTypes.includes(t))
    : [];

  // Can proceed to next step
  const canProceedToResources = sourceType !== null && targetType !== null;

  // Validate and update parent when on resources step
  // Build ResourceSelection objects from new state for validation and data passing
  useEffect(() => {
    if (step !== 'select-resources') {
      onMappingDataChange(null);
      return;
    }

    setValidationError(null);

    // Build source ResourceSelection
    let sourceSelection: ResourceSelection | null = null;
    if (sourceDatabase?.database && sourceDatabase?.container && sourceDatabase?.databaseId) {
      sourceSelection = {
        type: sourceType!,
        resourceId: sourceDatabase.container,
        resourceName: sourceDatabase.container,
        databaseName: sourceDatabase.database,
        databaseId: sourceDatabase.databaseId,
        tableName: sourceDatabase.container, // For name generation
        containerName: sourceDatabase.container, // For name generation
        uri: `redb://data/database/${sourceDatabase.databaseId}/table/${sourceDatabase.container}`,
      };
    } else if (sourceStream?.stream) {
      sourceSelection = {
        type: 'stream',
        resourceId: sourceStream.stream,
        resourceName: sourceStream.stream,
        uri: `stream://${sourceStream.stream}`,
      };
    }

    // Build target ResourceSelection
    let targetSelection: ResourceSelection | null = null;
    if (targetCategory === 'containers' && targetDatabase?.database && targetDatabase?.databaseId) {
      if (targetMode === 'new') {
        // For new container, use the entered name
        const containerName = newTargetContainerName.trim() || '__new__';
        targetSelection = {
          type: targetType!,
          resourceId: containerName,
          resourceName: containerName,
          databaseName: targetDatabase.database,
          databaseId: targetDatabase.databaseId,
          tableName: containerName, // For name generation
          containerName: containerName, // For name generation
          uri: `redb://data/database/${targetDatabase.databaseId}/table/${containerName}`,
        };
      } else if (targetDatabase.container) {
        targetSelection = {
          type: targetType!,
          resourceId: targetDatabase.container,
          resourceName: targetDatabase.container,
          databaseName: targetDatabase.database,
          databaseId: targetDatabase.databaseId,
          tableName: targetDatabase.container, // For name generation
          containerName: targetDatabase.container, // For name generation
          uri: `redb://data/database/${targetDatabase.databaseId}/table/${targetDatabase.container}`,
        };
      }
    } else if (targetType === 'stream' && targetStream?.stream) {
      targetSelection = {
        type: 'stream',
        resourceId: targetStream.stream,
        resourceName: targetStream.stream,
        uri: `stream://${targetStream.stream}`,
      };
    } else if (targetCategory === 'ai') {
      // MCP placeholder
      targetSelection = {
        type: 'mcp-resource',
        resourceId: '__placeholder__',
        resourceName: '__placeholder__',
        uri: 'mcp://__placeholder__',
      };
    }

    if (!sourceSelection || !targetSelection) {
      onMappingDataChange(null);
      return;
    }

    // Validate source-target pair
    if (!validateMappingPair(sourceSelection.type, targetSelection.type)) {
      setValidationError(
        `Mapping from ${sourceSelection.type} to ${targetSelection.type} is not supported`
      );
      onMappingDataChange(null);
      return;
    }

    // Auto-generate mapping name
    const autoName = generateMappingName(sourceSelection, targetSelection);
    const autoDescription = generateMappingDescription(sourceSelection, targetSelection);

    // All validation passed
    onMappingDataChange({
      mappingName: autoName,
      mappingDescription: autoDescription,
      source: sourceSelection,
      target: targetSelection,
      generateRules,
      isNewTarget: targetMode === 'new' && targetCategory === 'containers',
    });
  }, [
    step,
    sourceType,
    sourceDatabase,
    sourceStream,
    targetCategory,
    targetType,
    targetMode,
    targetDatabase,
    targetStream,
    newTargetContainerName,
    generateRules,
    onMappingDataChange,
  ]);

  const handleProceedToResources = () => {
    if (canProceedToResources) {
      setStep('select-resources');
    }
  };

  const handleBackToTypes = () => {
    setStep('select-types');
    // Keep resource selections so user can see what they had selected
  };

  // Show auto-mapping toggle if both database and target are selected
  // For now, we'll enable it when source and target databases are selected
  const showAutoMappingToggle = sourceDatabase?.container && targetDatabase?.container;

  return (
    <div className="space-y-4">
      {/* Step indicator */}
      <div className="flex items-center justify-center space-x-2 pb-2">
        <div className={`flex items-center space-x-2 ${step === 'select-types' ? 'text-primary' : 'text-muted-foreground'}`}>
          <div className={`flex items-center justify-center w-6 h-6 rounded-full border-2 text-xs font-semibold ${
            step === 'select-types' ? 'border-primary bg-primary/10' : 'border-muted-foreground/30 bg-muted'
          }`}>
            {step === 'select-resources' ? <Check className="h-3.5 w-3.5" /> : '1'}
          </div>
          <span className="text-xs font-medium">Select Types</span>
        </div>
        <div className="w-8 h-px bg-border"></div>
        <div className={`flex items-center space-x-2 ${step === 'select-resources' ? 'text-primary' : 'text-muted-foreground'}`}>
          <div className={`flex items-center justify-center w-6 h-6 rounded-full border-2 text-xs font-semibold ${
            step === 'select-resources' ? 'border-primary bg-primary/10' : 'border-muted-foreground/30'
          }`}>
            2
          </div>
          <span className="text-xs font-medium">Select Resources</span>
        </div>
      </div>

      {/* Fixed height container */}
      <div className="min-h-[600px] max-h-[600px]">
        {step === 'select-types' && (
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto_1fr] gap-4 items-start">
            {/* Source Panel */}
            <div className="bg-blue-50/50 dark:bg-blue-950/20 border-2 border-blue-200 dark:border-blue-800/30 rounded-lg p-3 h-[600px] overflow-y-auto">
              <div className="mb-2 flex items-center space-x-2">
                <div className="h-1.5 w-1.5 rounded-full bg-blue-500"></div>
                <h3 className="text-xs font-semibold text-blue-900 dark:text-blue-100 uppercase tracking-wide">
                  Source Type
                </h3>
              </div>
              <ResourceTypeSelector
                onSelect={(type) => setSourceType(type)}
                selected={sourceType}
                isTargetSelector={false}
                enableStatsFiltering={true}
              />
            </div>

            {/* Arrow Indicator */}
            <div className="hidden lg:flex items-center justify-center pt-16">
              <div className="flex flex-col items-center space-y-1">
                <ArrowRight className="h-6 w-6 text-primary" />
                <span className="text-[10px] text-muted-foreground font-medium">Maps to</span>
              </div>
            </div>

            {/* Arrow for mobile */}
            <div className="lg:hidden flex justify-center py-1">
              <div className="flex items-center space-x-2">
                <div className="h-px w-12 bg-border"></div>
                <ArrowRight className="h-5 w-5 text-primary" />
                <div className="h-px w-12 bg-border"></div>
              </div>
            </div>

            {/* Target Panel */}
            <div className="bg-green-50/50 dark:bg-green-950/20 border-2 border-green-200 dark:border-green-800/30 rounded-lg p-3 h-[600px] overflow-y-auto">
              <div className="mb-2 flex items-center space-x-2">
                <div className="h-1.5 w-1.5 rounded-full bg-green-500"></div>
                <h3 className="text-xs font-semibold text-green-900 dark:text-green-100 uppercase tracking-wide">
                  Target Type {!sourceType && ' (Select a source first)'}
                </h3>
              </div>
              
              {!targetCategory ? (
                <ResourceCategorySelector
                  onSelect={setTargetCategory}
                  selected={targetCategory}
                  disabled={!sourceType}
                />
              ) : (
                <div className="space-y-3">
                  <button
                    onClick={() => {
                      setTargetCategory(null);
                      setTargetType(null);
                    }}
                    className="text-xs text-primary hover:underline flex items-center space-x-1"
                  >
                    <ArrowLeft className="h-3 w-3" />
                    <span>Back to categories</span>
                  </button>
                  <ResourceTypeSelector
                    onSelect={(type) => setTargetType(type)}
                    selected={targetType}
                    allowedTypes={filteredTargetTypes}
                    isTargetSelector={true}
                  />
                </div>
              )}
            </div>
          </div>
        )}

        {step === 'select-resources' && (
          <div className="min-h-[600px] max-h-[600px] overflow-y-auto">
            <div className="space-y-4 pb-4">
              {/* Back button */}
              <button
                onClick={handleBackToTypes}
                className="text-xs text-primary hover:underline flex items-center space-x-1"
              >
                <ArrowLeft className="h-3 w-3" />
                <span>Back to type selection</span>
              </button>

              {/* Source and Target Selection */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {/* Source Selection */}
                <div className="bg-blue-50/50 dark:bg-blue-950/20 border-2 border-blue-200 dark:border-blue-800/30 rounded-lg p-4">
                  <div className="mb-3 flex items-center space-x-2">
                    <div className="h-1.5 w-1.5 rounded-full bg-blue-500"></div>
                    <h3 className="text-xs font-semibold text-blue-900 dark:text-blue-100 uppercase tracking-wide">
                      Source Resource
                    </h3>
                  </div>
                  
                  {/* Database Container Source */}
                  {sourceType && ['tabular-record-set', 'document', 'graph-node', 'graph-relationship', 'vector', 'keyvalue-item', 'search-document', 'timeseries-point', 'blob-object'].includes(sourceType) && (
                    <DatabaseContainerSelector
                      workspaceId={workspaceId}
                      onSelect={setSourceDatabase}
                      value={sourceDatabase ? { database: sourceDatabase.database, container: sourceDatabase.container } : null}
                      allowContainerSelection={true}
                      label={`Select Source ${sourceType}`}
                    />
                  )}
                  
                  {/* Stream Source */}
                  {sourceType === 'stream' && (
                    <StreamSelector
                      workspaceId={workspaceId}
                      onSelect={setSourceStream}
                      value={sourceStream}
                      allowTopicSelection={true}
                      label="Select Source Stream"
                    />
                  )}
                  
                  {/* Webhook Source - Placeholder */}
                  {sourceType === 'webhook' && (
                    <div className="p-3 border border-border rounded-lg">
                      <p className="text-xs text-muted-foreground">Webhook source selection coming soon</p>
                    </div>
                  )}
                </div>

                {/* Target Selection */}
                <div className="bg-green-50/50 dark:bg-green-950/20 border-2 border-green-200 dark:border-green-800/30 rounded-lg p-4">
                  <div className="mb-3 flex items-center space-x-2">
                    <div className="h-1.5 w-1.5 rounded-full bg-green-500"></div>
                    <h3 className="text-xs font-semibold text-green-900 dark:text-green-100 uppercase tracking-wide">
                      Target Resource
                    </h3>
                  </div>
                  
                  {/* MCP Placeholder */}
                  {targetCategory === 'ai' && (
                    <div className="p-4 border border-primary/30 bg-primary/5 rounded-lg">
                      <p className="text-sm text-foreground font-medium">MCP Integration</p>
                      <p className="text-xs text-muted-foreground mt-1">
                        MCP resource and tool targeting will be available in a future release.
                      </p>
                    </div>
                  )}
                  
                  {/* Data Container Targets */}
                  {targetCategory === 'containers' && targetType && (
                    <div className="space-y-4">
                      {/* New vs Existing */}
                      <TargetModeSelector
                        onSelect={setTargetMode}
                        selected={targetMode}
                        resourceTypeLabel="Container"
                      />
                      
                      {/* Database Selection */}
                      {targetMode === 'new' && (
                        <>
                          <DatabaseContainerSelector
                            workspaceId={workspaceId}
                            onSelect={setTargetDatabase}
                            value={targetDatabase ? { database: targetDatabase.database, container: null } : null}
                            allowContainerSelection={false}
                            label="Select Target Database"
                          />
                          
                          {/* New Container Name Input */}
                          {targetDatabase?.database && (
                            <div>
                              <label className="block text-xs text-muted-foreground mb-1.5">
                                New Container Name
                              </label>
                              <input
                                type="text"
                                value={newTargetContainerName}
                                onChange={(e) => setNewTargetContainerName(e.target.value)}
                                placeholder="Enter name for new container"
                                className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-2 focus:ring-primary"
                              />
                              <p className="text-xs text-muted-foreground mt-1.5">
                                The new container will be created in the selected database
                              </p>
                            </div>
                          )}
                        </>
                      )}
                      
                      {targetMode === 'existing' && (
                        <DatabaseContainerSelector
                          workspaceId={workspaceId}
                          onSelect={setTargetDatabase}
                          value={targetDatabase ? { database: targetDatabase.database, container: targetDatabase.container } : null}
                          allowContainerSelection={true}
                          label="Select Target Container"
                        />
                      )}
                    </div>
                  )}
                  
                  {/* Stream Target */}
                  {targetType === 'stream' && (
                    <div className="space-y-4">
                      <TargetModeSelector
                        onSelect={setTargetMode}
                        selected={targetMode}
                        resourceTypeLabel="Stream"
                      />
                      
                      {targetMode === 'new' && (
                        <div className="p-3 border border-primary/30 bg-primary/5 rounded-lg">
                          <p className="text-xs text-foreground font-medium">Creating New Stream</p>
                          <p className="text-xs text-muted-foreground mt-1">
                            A new stream will be created based on your mapping configuration.
                          </p>
                        </div>
                      )}
                      
                      {targetMode === 'existing' && (
                        <StreamSelector
                          workspaceId={workspaceId}
                          onSelect={setTargetStream}
                          value={targetStream}
                          allowTopicSelection={true}
                          label="Select Target Stream"
                          allowNew={false}
                        />
                      )}
                    </div>
                  )}
                  
                  {/* Webhook Target */}
                  {targetType === 'webhook' && (
                    <div className="space-y-4">
                      <TargetModeSelector
                        onSelect={setTargetMode}
                        selected={targetMode}
                        resourceTypeLabel="Webhook"
                      />
                      
                      {targetMode && (
                        <div className="p-3 border border-border rounded-lg">
                          <p className="text-xs text-muted-foreground">
                            Webhook {targetMode === 'new' ? 'creation' : 'selection'} coming soon
                          </p>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>

              {/* Mapping Name (Optional) */}
              <div className="border border-border rounded-lg p-4 bg-muted/20">
                <label className="block text-xs font-medium text-foreground mb-2">
                  Mapping Name (Optional)
                </label>
                <input
                  type="text"
                  placeholder="Leave blank for auto-generated name"
                  className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-2 focus:ring-primary"
                />
                <p className="text-xs text-muted-foreground mt-1.5">
                  If left blank, a name will be automatically generated based on source and target
                </p>
              </div>

              {/* Auto-Mapping Toggle */}
              {showAutoMappingToggle && !validationError && (
                <div className="border border-border rounded-lg p-4 bg-muted/30">
                  <div className="flex items-start space-x-3">
                    <input
                      type="checkbox"
                      id="generate-rules"
                      checked={generateRules}
                      onChange={(e) => setGenerateRules(e.target.checked)}
                      className="mt-0.5 h-4 w-4 rounded border-border text-primary focus:ring-primary"
                    />
                    <div className="flex-1">
                      <label
                        htmlFor="generate-rules"
                        className="text-xs font-medium text-foreground cursor-pointer block"
                      >
                        Generate Automatic Mapping Rules
                      </label>
                      <p className="text-xs text-muted-foreground mt-1 leading-relaxed">
                        Automatically create field mapping rules by matching field names and types between source and
                        target. Disable this if you want to create a blank mapping and define rules manually.
                      </p>
                    </div>
                  </div>
                </div>
              )}

              {/* Validation Error */}
              {validationError && (
                <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4 flex items-start">
                  <AlertCircle className="h-5 w-5 text-destructive mt-0.5 mr-3 flex-shrink-0" />
                  <div>
                    <h4 className="text-sm font-medium text-destructive">Invalid Mapping Configuration</h4>
                    <p className="text-sm text-destructive/80 mt-1">{validationError}</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Navigation buttons */}
      {step === 'select-types' && (
        <div className="flex justify-end pt-2 border-t border-border">
          <button
            onClick={handleProceedToResources}
            disabled={!canProceedToResources}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed hover:bg-primary/90 transition-colors flex items-center space-x-2"
          >
            <span>Next: Select Resources</span>
            <ArrowRight className="h-4 w-4" />
          </button>
        </div>
      )}
    </div>
  );
});
