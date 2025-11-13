'use client';

import { X, Edit2, Save, AlertCircle } from 'lucide-react';
import { useState } from 'react';
import type { MappingRule, SchemaColumn } from '@/lib/api/types';
import { parseResourceURI, createDisplayLabel } from '@/lib/utils/uri-parser';
import { getColumnTypeDisplay, getColumnConstraints } from '@/lib/hooks/useDatabaseSchemaInfo';
import { RuleValidationBadge } from './RuleValidationIndicator';
import type { ValidationResult } from '@/lib/utils/type-validator';

interface RuleDetailsPanelProps {
  rule: MappingRule;
  sourceColumn?: SchemaColumn | null;
  targetColumn?: SchemaColumn | null;
  validation?: ValidationResult;
  onClose: () => void;
  onUpdateDescription?: (ruleId: string, ruleName: string, description: string) => Promise<void>;
}

export function RuleDetailsPanel({
  rule,
  sourceColumn,
  targetColumn,
  validation,
  onClose,
  onUpdateDescription,
}: RuleDetailsPanelProps) {
  const [isEditingDescription, setIsEditingDescription] = useState(false);
  const [description, setDescription] = useState(rule.mapping_rule_description || '');
  const [isSaving, setIsSaving] = useState(false);

  const sourceParsed = parseResourceURI(rule.mapping_rule_source);
  const targetParsed = parseResourceURI(rule.mapping_rule_target);

  const handleSaveDescription = async () => {
    if (!onUpdateDescription) return;

    try {
      setIsSaving(true);
      await onUpdateDescription(rule.mapping_rule_id, rule.mapping_rule_name, description);
      setIsEditingDescription(false);
    } catch (error) {
      console.error('Error saving description:', error);
    } finally {
      setIsSaving(false);
    }
  };

  const renderColumnDetails = (
    column: SchemaColumn | null | undefined,
    parsed: ReturnType<typeof parseResourceURI>,
    title: string
  ) => {
    return (
      <div className="space-y-3">
        <h4 className="font-semibold text-foreground text-sm">{title}</h4>
        
        {/* URI Display */}
        <div>
          <div className="text-xs text-muted-foreground mb-1">Resource URI</div>
          <div className="text-xs font-mono bg-muted px-2 py-1.5 rounded break-all">
            {parsed?.rawUri || 'Invalid URI'}
          </div>
        </div>

        {/* Parsed Components */}
        {parsed && (
          <div className="grid grid-cols-2 gap-3">
            <div>
              <div className="text-xs text-muted-foreground mb-1">Database</div>
              <div className="text-sm font-medium text-foreground">{parsed.databaseName}</div>
            </div>
            {parsed.tableName && (
              <div>
                <div className="text-xs text-muted-foreground mb-1">Table</div>
                <div className="text-sm font-medium text-foreground">{parsed.tableName}</div>
              </div>
            )}
            {parsed.columnName && (
              <div>
                <div className="text-xs text-muted-foreground mb-1">Column</div>
                <div className="text-sm font-medium text-foreground">{parsed.columnName}</div>
              </div>
            )}
          </div>
        )}

        {/* Column Schema Info */}
        {column ? (
          <div className="space-y-2 pt-2 border-t border-border">
            <div>
              <div className="text-xs text-muted-foreground mb-1">Data Type</div>
              <div className="text-sm font-mono font-medium text-foreground">
                {getColumnTypeDisplay(column)}
              </div>
            </div>

            <div>
              <div className="text-xs text-muted-foreground mb-1">Constraints</div>
              <div className="flex flex-wrap gap-1.5">
                {getColumnConstraints(column).map((constraint) => (
                  <span
                    key={constraint}
                    className="text-xs px-2 py-1 bg-primary/10 text-primary rounded font-medium"
                  >
                    {constraint}
                  </span>
                ))}
                {getColumnConstraints(column).length === 0 && (
                  <span className="text-xs text-muted-foreground">None</span>
                )}
              </div>
            </div>

            {(column.columnDefault || column.column_default) && (
              <div>
                <div className="text-xs text-muted-foreground mb-1">Default Value</div>
                <div className="text-sm font-mono text-foreground">
                  {column.columnDefault || column.column_default}
                </div>
              </div>
            )}

            {(column.dataCategory || column.data_category) && (
              <div>
                <div className="text-xs text-muted-foreground mb-1">Data Category</div>
                <div className="text-sm text-foreground">
                  {column.dataCategory || column.data_category}
                </div>
              </div>
            )}

            {(column.isPrivilegedData || column.is_privileged_data) && (
              <div className="flex items-start gap-2 p-2 bg-yellow-50 dark:bg-yellow-900/20 rounded border border-yellow-200 dark:border-yellow-800/30">
                <AlertCircle className="h-4 w-4 text-yellow-600 dark:text-yellow-400 flex-shrink-0 mt-0.5" />
                <div>
                  <div className="text-xs font-medium text-yellow-800 dark:text-yellow-200">
                    Privileged Data
                  </div>
                  {(column.privilegedDescription || column.privileged_description) && (
                    <div className="text-xs text-yellow-700 dark:text-yellow-300 mt-0.5">
                      {column.privilegedDescription || column.privileged_description}
                    </div>
                  )}
                  {(column.privilegedConfidence || column.privileged_confidence) && (
                    <div className="text-xs text-yellow-700 dark:text-yellow-300 mt-0.5">
                      Confidence: {((column.privilegedConfidence || column.privileged_confidence || 0) * 100).toFixed(0)}%
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        ) : (
          <div className="text-sm text-muted-foreground italic pt-2 border-t border-border">
            Schema information not available
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="fixed inset-y-0 right-0 w-full max-w-2xl bg-background border-l border-border shadow-2xl z-50 overflow-hidden flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-border">
        <div>
          <h3 className="text-lg font-semibold text-foreground">{rule.mapping_rule_name}</h3>
          <p className="text-sm text-muted-foreground mt-0.5">Mapping Rule Details</p>
        </div>
        <button
          onClick={onClose}
          className="p-2 hover:bg-accent rounded-md transition-colors"
          title="Close"
        >
          <X className="h-5 w-5" />
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto px-6 py-6 space-y-6">
        {/* Validation Status */}
        {validation && (
          <div>
            <h4 className="font-semibold text-foreground text-sm mb-2">Validation Status</h4>
            <RuleValidationBadge validation={validation} size="lg" />
          </div>
        )}

        {/* Description */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <h4 className="font-semibold text-foreground text-sm">Description</h4>
            {onUpdateDescription && !isEditingDescription && (
              <button
                onClick={() => setIsEditingDescription(true)}
                className="text-xs text-primary hover:text-primary/80 flex items-center gap-1"
              >
                <Edit2 className="h-3 w-3" />
                Edit
              </button>
            )}
          </div>
          
          {isEditingDescription ? (
            <div className="space-y-2">
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                className="w-full px-3 py-2 bg-background border border-border rounded-md text-sm resize-none focus:outline-none focus:ring-2 focus:ring-primary"
                rows={3}
                placeholder="Enter rule description..."
              />
              <div className="flex justify-end gap-2">
                <button
                  onClick={() => {
                    setDescription(rule.mapping_rule_description || '');
                    setIsEditingDescription(false);
                  }}
                  disabled={isSaving}
                  className="px-3 py-1.5 text-xs border border-border rounded-md hover:bg-accent transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSaveDescription}
                  disabled={isSaving}
                  className="px-3 py-1.5 text-xs bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors flex items-center gap-1.5"
                >
                  {isSaving ? (
                    <>Saving...</>
                  ) : (
                    <>
                      <Save className="h-3 w-3" />
                      Save
                    </>
                  )}
                </button>
              </div>
            </div>
          ) : (
            <p className="text-sm text-foreground">
              {rule.mapping_rule_description || (
                <span className="text-muted-foreground italic">No description</span>
              )}
            </p>
          )}
        </div>

        {/* Transformation */}
        {rule.mapping_rule_transformation_name && (
          <div>
            <h4 className="font-semibold text-foreground text-sm mb-2">Transformation</h4>
            <div className="bg-muted px-3 py-2 rounded-md">
              <div className="text-sm font-medium text-foreground">
                {rule.mapping_rule_transformation_name}
              </div>
              {rule.mapping_rule_transformation_options && (
                <div className="text-xs text-muted-foreground mt-1">
                  Options: {rule.mapping_rule_transformation_options}
                </div>
              )}
            </div>
          </div>
        )}

        {/* Source Details */}
        <div className="bg-card border border-border rounded-lg p-4">
          {renderColumnDetails(sourceColumn, sourceParsed, 'Source')}
        </div>

        {/* Target Details */}
        <div className="bg-card border border-border rounded-lg p-4">
          {renderColumnDetails(targetColumn, targetParsed, 'Target')}
        </div>

        {/* Metadata */}
        {rule.mapping_rule_metadata && (
          <div>
            <h4 className="font-semibold text-foreground text-sm mb-2">Metadata</h4>
            <div className="bg-muted px-3 py-3 rounded-md space-y-2 text-xs">
              {rule.mapping_rule_metadata.match_score !== undefined && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Match Score:</span>
                  <span className="font-medium text-foreground">
                    {(rule.mapping_rule_metadata.match_score * 100).toFixed(0)}%
                  </span>
                </div>
              )}
              {rule.mapping_rule_metadata.match_type && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Match Type:</span>
                  <span className="font-medium text-foreground">
                    {rule.mapping_rule_metadata.match_type}
                  </span>
                </div>
              )}
              {rule.mapping_rule_metadata.type_compatible !== undefined && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Type Compatible:</span>
                  <span className="font-medium text-foreground">
                    {rule.mapping_rule_metadata.type_compatible ? 'Yes' : 'No'}
                  </span>
                </div>
              )}
              {rule.mapping_rule_metadata.generated_at && (
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Generated At:</span>
                  <span className="font-medium text-foreground">
                    {new Date(rule.mapping_rule_metadata.generated_at).toLocaleString()}
                  </span>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

