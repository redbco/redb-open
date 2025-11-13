'use client';

import { useState, useEffect } from 'react';
import { ArrowRight, AlertCircle } from 'lucide-react';
import { ResourceSelection } from '@/lib/api/types';
import { ResourcePicker } from './ResourcePicker';
import { validateMappingPair, getValidTargetTypes, requiresAutoMapping } from '@/lib/utils/mapping-validator';
import { generateMappingName, generateMappingDescription } from '@/lib/utils/mapping-name-generator';

interface MappingCreationWizardProps {
  workspaceId: string;
  onMappingDataChange: (data: MappingCreationData | null) => void;
}

export interface MappingCreationData {
  mappingName: string;
  mappingDescription: string;
  source: ResourceSelection;
  target: ResourceSelection;
  generateRules: boolean;
}

export function MappingCreationWizard({
  workspaceId,
  onMappingDataChange,
}: MappingCreationWizardProps) {
  const [mappingName, setMappingName] = useState('');
  const [mappingDescription, setMappingDescription] = useState('');
  const [source, setSource] = useState<ResourceSelection | null>(null);
  const [target, setTarget] = useState<ResourceSelection | null>(null);
  const [generateRules, setGenerateRules] = useState(true);
  const [isManualName, setIsManualName] = useState(false); // Track if user manually entered a name

  // Validation state
  const [validationError, setValidationError] = useState<string | null>(null);

  // Validate and update parent when data changes
  useEffect(() => {
    setValidationError(null);

    // Check if basic fields are filled
    if (!mappingName.trim()) {
      onMappingDataChange(null);
      return;
    }

    if (!source || !target) {
      onMappingDataChange(null);
      return;
    }

    // Validate source-target pair
    if (!validateMappingPair(source.type, target.type)) {
      setValidationError(
        `Mapping from ${source.type} to ${target.type} is not supported`
      );
      onMappingDataChange(null);
      return;
    }

    // All validation passed, update parent
    onMappingDataChange({
      mappingName: mappingName.trim(),
      mappingDescription: mappingDescription.trim(),
      source,
      target,
      generateRules,
    });
  }, [mappingName, mappingDescription, source, target, generateRules, onMappingDataChange]);

  // Auto-generate mapping name and description based on source/target selections
  useEffect(() => {
    if (source && target) {
      // Only auto-generate name if user hasn't manually entered one
      if (!isManualName) {
        const autoName = generateMappingName(source, target);
        setMappingName(autoName);
      }
      
      // Always auto-generate description
      const autoDescription = generateMappingDescription(source, target);
      setMappingDescription(autoDescription);
    }
  }, [source, target, isManualName]);

  // Handle manual name changes
  const handleNameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newName = e.target.value;
    setMappingName(newName);
    // Mark as manual if user types something, or unmark if they clear it
    setIsManualName(newName.trim().length > 0);
  };

  // Get allowed target types based on source selection
  const allowedTargetTypes = source ? getValidTargetTypes(source.type) : undefined;
  const showAutoMappingToggle =
    source && target && requiresAutoMapping(source.type, target.type);

  return (
    <div className="space-y-4">
      {/* Mapping Name */}
      <div>
        <label className="block text-sm font-medium text-foreground mb-2">
          Mapping Name <span className="text-muted-foreground text-xs font-normal">(optional - auto-generated if empty)</span>
        </label>
        <input
          type="text"
          value={mappingName}
          onChange={handleNameChange}
          className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
          placeholder="Auto-generated based on source and target"
        />
        {isManualName && (
          <p className="text-xs text-muted-foreground mt-1">
            Using custom name. Clear the field to use auto-generated name.
          </p>
        )}
        {!isManualName && mappingName && (
          <p className="text-xs text-muted-foreground mt-1">
            Auto-generated: <span className="font-mono">{mappingName}</span>
          </p>
        )}
      </div>

      {/* Source and Target Pickers with Visual Arrow */}
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto_1fr] gap-4 items-start">
        {/* Source Panel */}
        <div className="bg-blue-50/50 dark:bg-blue-950/20 border-2 border-blue-200 dark:border-blue-800/30 rounded-lg p-3">
          <div className="mb-2 flex items-center space-x-2">
            <div className="h-1.5 w-1.5 rounded-full bg-blue-500"></div>
            <h3 className="text-xs font-semibold text-blue-900 dark:text-blue-100 uppercase tracking-wide">
              Source - Select Resource Type
            </h3>
          </div>
          <ResourcePicker
            workspaceId={workspaceId}
            label=""
            value={source}
            onChange={setSource}
            placeholder="Choose where data comes from"
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
        <div className="bg-green-50/50 dark:bg-green-950/20 border-2 border-green-200 dark:border-green-800/30 rounded-lg p-3">
          <div className="mb-2 flex items-center space-x-2">
            <div className="h-1.5 w-1.5 rounded-full bg-green-500"></div>
            <h3 className="text-xs font-semibold text-green-900 dark:text-green-100 uppercase tracking-wide">
              Target - Select Resource Type
            </h3>
          </div>
          <ResourcePicker
            workspaceId={workspaceId}
            label=""
            value={target}
            onChange={setTarget}
            placeholder="Choose where data goes to"
            allowedTypes={allowedTargetTypes}
            disabled={!source}
          />
          {!source && (
            <div className="mt-2 text-[10px] text-muted-foreground italic">
              Select a source first to enable target selection
            </div>
          )}
        </div>
      </div>

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

      {/* Auto-Mapping Toggle */}
      {showAutoMappingToggle && !validationError && (
        <div className="border border-border rounded-lg p-3 bg-muted/30">
          <div className="flex items-start space-x-2">
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
                className="text-xs font-medium text-foreground cursor-pointer"
              >
                Automatically generate mapping rules
              </label>
              <p className="text-[10px] text-muted-foreground mt-0.5">
                Auto-create column mappings based on name and type similarity
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Mapping Summary */}
      {source && target && !validationError && (
        <div className="bg-primary/5 border border-primary/20 rounded-lg p-3">
          <h4 className="text-xs font-semibold text-foreground mb-1.5">Mapping Summary</h4>
          <div className="text-xs text-muted-foreground space-y-0.5">
            <p>
              <span className="font-medium text-foreground">Type:</span>{' '}
              {source.type} → {target.type}
            </p>
            <p>
              <span className="font-medium text-foreground">Source:</span> {source.resourceName}
              {source.databaseName && ` (${source.databaseName})`}
            </p>
            <p>
              <span className="font-medium text-foreground">Target:</span> {target.resourceName}
              {target.databaseName && ` (${target.databaseName})`}
            </p>
            {showAutoMappingToggle && (
              <p>
                <span className="font-medium text-foreground">Rules:</span>{' '}
                {generateRules ? 'Auto-generated' : 'Empty (manual)'}
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

