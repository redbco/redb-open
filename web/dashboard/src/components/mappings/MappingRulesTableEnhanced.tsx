'use client';

import { useState, useMemo, useCallback } from 'react';
import { ArrowRight, Trash2, ChevronDown, ChevronRight, Search, Filter, Shield } from 'lucide-react';
import type { MappingRule, Transformation, SchemaColumn, ResourceItem } from '@/lib/api/types';
import { parseResourceURI, createDisplayLabel } from '@/lib/utils/uri-parser';
import { validateMappingRule, type ValidationResult } from '@/lib/utils/type-validator';
import { getColumnTypeDisplay, getColumnConstraints } from '@/lib/hooks/useDatabaseSchemaInfo';
import { RuleValidationIcon } from './RuleValidationIndicator';
import { TransformationEditor } from './TransformationEditor';

interface EnhancedMappingRule extends MappingRule {
  sourceColumn?: SchemaColumn | null;
  targetColumn?: SchemaColumn | null;
  validation?: ValidationResult;
}

interface MappingRulesTableEnhancedProps {
  rules: MappingRule[];
  transformations: Transformation[];
  isLoading: boolean;
  sourceSchemaColumns: Map<string, SchemaColumn>; // Key: tableName.columnName
  targetSchemaColumns: Map<string, SchemaColumn>; // Key: tableName.columnName
  sourceContainerItems?: ResourceItem[]; // Items from the source container (mapping level)
  targetContainerItems?: ResourceItem[]; // Items from the target container (mapping level)
  showDisplayNames?: boolean; // Whether to show user-defined display names instead of real column names
  onDelete?: (ruleId: string) => void;
  onUpdateTransformation?: (ruleId: string, ruleName: string, transformationName: string) => Promise<void>;
  onRuleClick?: (rule: MappingRule) => void;
}

type FilterType = 'all' | 'valid' | 'warnings' | 'errors';

export function MappingRulesTableEnhanced({
  rules,
  transformations,
  isLoading,
  sourceSchemaColumns,
  targetSchemaColumns,
  sourceContainerItems,
  targetContainerItems,
  showDisplayNames = false,
  onDelete,
  onUpdateTransformation,
  onRuleClick,
}: MappingRulesTableEnhancedProps) {
  const [expandedRules, setExpandedRules] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState('');
  const [filterType, setFilterType] = useState<FilterType>('all');

  // Enhance rules with schema information and validation
  const enhancedRules: EnhancedMappingRule[] = useMemo(() => {
    return rules.map((rule) => {
      // Parse source and target URIs
      const sourceParsed = parseResourceURI(rule.mapping_rule_source);
      const targetParsed = parseResourceURI(rule.mapping_rule_target);

      // Find column info
      let sourceColumn: SchemaColumn | null = null;
      let targetColumn: SchemaColumn | null = null;

      if (sourceParsed?.tableName && sourceParsed?.columnName) {
        const key = `${sourceParsed.tableName}.${sourceParsed.columnName}`;
        sourceColumn = sourceSchemaColumns.get(key) || null;
      }

      if (targetParsed?.tableName && targetParsed?.columnName) {
        const key = `${targetParsed.tableName}.${targetParsed.columnName}`;
        targetColumn = targetSchemaColumns.get(key) || null;
      }

      // Validate the rule
      const validation = validateMappingRule(
        sourceColumn,
        targetColumn,
        rule.mapping_rule_transformation_name
      );

      return {
        ...rule,
        sourceColumn,
        targetColumn,
        validation,
      };
    });
  }, [rules, sourceSchemaColumns, targetSchemaColumns]);

  // Filter and search rules
  const filteredRules = useMemo(() => {
    let filtered = enhancedRules;

    // Apply search filter
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(
        (rule) =>
          rule.mapping_rule_name.toLowerCase().includes(query) ||
          rule.mapping_rule_source.toLowerCase().includes(query) ||
          rule.mapping_rule_target.toLowerCase().includes(query) ||
          rule.mapping_rule_transformation_name?.toLowerCase().includes(query)
      );
    }

    // Apply validation filter
    if (filterType !== 'all') {
      filtered = filtered.filter((rule) => {
        if (!rule.validation) return false;
        switch (filterType) {
          case 'valid':
            return rule.validation.level === 'success';
          case 'warnings':
            return rule.validation.level === 'warning' || rule.validation.level === 'info';
          case 'errors':
            return rule.validation.level === 'error';
          default:
            return true;
        }
      });
    }

    return filtered;
  }, [enhancedRules, searchQuery, filterType]);

  const toggleExpand = (ruleId: string) => {
    const newExpanded = new Set(expandedRules);
    if (newExpanded.has(ruleId)) {
      newExpanded.delete(ruleId);
    } else {
      newExpanded.add(ruleId);
    }
    setExpandedRules(newExpanded);
  };

  // Helper function to get privileged badge color based on confidence level
  const getPrivilegedColor = (confidence: number | null | undefined): string => {
    if (confidence === null || confidence === undefined) {
      return 'text-red-500 dark:text-red-400'; // Default to high
    }
    
    if (confidence > 0.7) return 'text-red-500 dark:text-red-400'; // High confidence
    if (confidence >= 0.4) return 'text-orange-500 dark:text-orange-400'; // Medium confidence
    if (confidence > 0) return 'text-yellow-500 dark:text-yellow-400'; // Low confidence
    
    return 'text-red-500 dark:text-red-400'; // Default
  };

  const formatColumnInfo = useCallback((column: SchemaColumn | null | undefined, uri: string, resourceItems?: ResourceItem[]) => {
    const parsed = parseResourceURI(uri);
    
    // Try to find the resource item details from the rule's source_items or target_items
    const resourceItem = resourceItems?.find(item => item.resource_uri === uri);
    
    if (!column && !resourceItem) {
      return {
        label: createDisplayLabel(uri),
        tableName: parsed?.tableName || null,
        type: 'unknown',
        constraints: [],
        isPrivileged: false,
        privilegedClassification: null,
        detectionConfidence: null,
        detectionMethod: null,
      };
    }

    // Use resource item details if available (more complete than schema column)
    if (resourceItem) {
      // Use display name if showDisplayNames is true and display name is available
      const useDisplayName = showDisplayNames && resourceItem.item_display_name;
      const columnName = useDisplayName 
        ? resourceItem.item_display_name 
        : resourceItem.item_name;
      
      return {
        label: columnName,
        // Don't include tableName when using display names (they often already include it)
        tableName: useDisplayName ? null : (parsed?.tableName || null),
        type: resourceItem.unified_data_type || resourceItem.data_type,
        constraints: [
          ...(resourceItem.is_primary_key ? ['PK'] : []),
          ...(resourceItem.is_unique ? ['UNIQUE'] : []),
          ...(resourceItem.is_indexed ? ['INDEX'] : []),
          ...(resourceItem.is_required || !resourceItem.is_nullable ? ['NOT NULL'] : []),
          ...(resourceItem.is_array ? ['ARRAY'] : []),
        ],
        isPrivileged: resourceItem.is_privileged,
        privilegedClassification: resourceItem.privileged_classification,
        detectionConfidence: resourceItem.detection_confidence,
        detectionMethod: resourceItem.detection_method,
        resourceItem,
      };
    }

    // Fallback to schema column
    // For schema columns, we don't have display names, so just use the real name
    return {
      label: column?.name || createDisplayLabel(uri),
      tableName: parsed?.tableName || null,
      type: getColumnTypeDisplay(column!),
      constraints: getColumnConstraints(column!),
      isPrivileged: column?.is_privileged_data || column?.isPrivilegedData || false,
      privilegedClassification: column?.privileged_description || column?.privilegedDescription || null,
      detectionConfidence: column?.privileged_confidence || column?.privilegedConfidence || null,
      detectionMethod: null,
      resourceItem: null,
    };
  }, [showDisplayNames]);

  if (isLoading) {
    return (
      <div className="space-y-2">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="bg-card border border-border rounded-lg p-4 animate-pulse">
            <div className="h-4 bg-muted rounded w-3/4"></div>
          </div>
        ))}
      </div>
    );
  }

  if (rules.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-12 text-center">
        <ArrowRight className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-xl font-semibold text-foreground mb-2">No Mapping Rules</h3>
        <p className="text-muted-foreground">
          This mapping doesn&apos;t have any rules yet. Add rules to define column mappings.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Search and Filter */}
      <div className="flex items-center gap-3">
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search rules by name, column, or transformation..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 bg-background border border-border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-primary"
          />
        </div>
        <div className="relative">
          <select
            value={filterType}
            onChange={(e) => setFilterType(e.target.value as FilterType)}
            className="pl-3 pr-10 py-2 bg-background border border-border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-primary appearance-none cursor-pointer"
          >
            <option value="all">All Rules</option>
            <option value="valid">Valid Only</option>
            <option value="warnings">Warnings</option>
            <option value="errors">Errors Only</option>
          </select>
          <Filter className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
        </div>
      </div>

      {/* Table */}
      <div className="bg-card border border-border rounded-lg overflow-visible">
        <div className="overflow-x-auto">
          <table className="w-full table-fixed">
            <colgroup>
              <col className="w-8" />
              <col className="w-[35%]" />
              <col className="w-[200px]" />
              <col className="w-[35%]" />
              <col className="w-[100px]" />
              <col className="w-20" />
            </colgroup>
            <thead className="bg-muted/50 border-b border-border">
              <tr>
                <th className="px-3 py-3"></th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Source Column
                </th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Transformation
                </th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Target Column
                </th>
                <th className="px-4 py-3 text-center text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Status
                </th>
                <th className="px-4 py-3 text-right text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {filteredRules.map((rule) => {
                const isExpanded = expandedRules.has(rule.mapping_rule_id);
                
                // Use mapping-level container items instead of rule-level items
                const sourceInfo = formatColumnInfo(rule.sourceColumn, rule.mapping_rule_source, sourceContainerItems);
                const targetInfo = formatColumnInfo(rule.targetColumn, rule.mapping_rule_target, targetContainerItems);

                return (
                  <tr
                    key={rule.mapping_rule_id}
                    className="hover:bg-accent/50 transition-colors cursor-pointer"
                    onClick={() => onRuleClick?.(rule)}
                  >
                    <td className="px-3 py-3 align-top">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleExpand(rule.mapping_rule_id);
                        }}
                        className="text-muted-foreground hover:text-foreground transition-colors"
                      >
                        {isExpanded ? (
                          <ChevronDown className="h-4 w-4" />
                        ) : (
                          <ChevronRight className="h-4 w-4" />
                        )}
                      </button>
                    </td>
                    
                    {/* Source Column */}
                    <td className="px-4 py-3 align-top">
                      <div className="flex flex-col gap-1">
                        <div className="flex items-center gap-2">
                          <div className="font-mono text-sm text-foreground font-medium">
                            {sourceInfo.tableName && sourceInfo.label ? `${sourceInfo.tableName}.${sourceInfo.label}` : sourceInfo.label}
                          </div>
                          {sourceInfo.isPrivileged && (
                            <div className="group relative">
                              <Shield className={`h-4 w-4 ${getPrivilegedColor(sourceInfo.detectionConfidence)}`} />
                              <div className="invisible group-hover:visible absolute z-10 px-3 py-2 text-xs bg-gray-900 text-white rounded-md shadow-lg whitespace-nowrap -top-2 left-6">
                                <div className="font-semibold mb-1">Privileged Data</div>
                                {sourceInfo.privilegedClassification && (
                                  <div>Classification: {sourceInfo.privilegedClassification}</div>
                                )}
                                {sourceInfo.detectionConfidence !== null && sourceInfo.detectionConfidence !== undefined && (
                                  <div>Confidence: {(sourceInfo.detectionConfidence * 100).toFixed(0)}%</div>
                                )}
                                {sourceInfo.detectionMethod && (
                                  <div>Method: {sourceInfo.detectionMethod}</div>
                                )}
                              </div>
                            </div>
                          )}
                        </div>
                        <div className="flex items-center gap-1.5 flex-wrap">
                          <span className="text-xs font-mono px-1.5 py-0.5 bg-muted rounded text-muted-foreground">
                            {sourceInfo.type}
                          </span>
                          {sourceInfo.constraints.map((constraint) => (
                            <span
                              key={constraint}
                              className="text-[10px] px-1.5 py-0.5 bg-primary/10 text-primary rounded font-medium"
                            >
                              {constraint}
                            </span>
                          ))}
                        </div>
                        {isExpanded && (
                          <div className="text-xs text-muted-foreground mt-2 space-y-1 pt-2 border-t border-border">
                            <div>
                              <span className="font-semibold">URI:</span> {rule.mapping_rule_source}
                            </div>
                            {sourceInfo.resourceItem && (
                              <>
                                {sourceInfo.resourceItem.default_value && (
                                  <div>
                                    <span className="font-semibold">Default:</span> {sourceInfo.resourceItem.default_value}
                                  </div>
                                )}
                                {sourceInfo.resourceItem.item_comment && (
                                  <div>
                                    <span className="font-semibold">Description:</span> {sourceInfo.resourceItem.item_comment}
                                  </div>
                                )}
                              </>
                            )}
                            {rule.sourceColumn && (
                              <>
                                {(rule.sourceColumn.columnDefault || rule.sourceColumn.column_default) && (
                                  <div>
                                    <span className="font-semibold">Default:</span> {rule.sourceColumn.columnDefault || rule.sourceColumn.column_default}
                                  </div>
                                )}
                                {(rule.sourceColumn.dataCategory || rule.sourceColumn.data_category) && (
                                  <div>
                                    <span className="font-semibold">Category:</span> {rule.sourceColumn.dataCategory || rule.sourceColumn.data_category}
                                  </div>
                                )}
                              </>
                            )}
                          </div>
                        )}
                      </div>
                    </td>

                    {/* Transformation */}
                    <td className="px-4 py-3 align-top" onClick={(e) => e.stopPropagation()}>
                      <div className="flex items-start">
                        {onUpdateTransformation ? (
                          <TransformationEditor
                            currentTransformation={rule.mapping_rule_transformation_name || null}
                            transformations={transformations}
                            onSave={async (transformationName) => {
                              await onUpdateTransformation(
                                rule.mapping_rule_id,
                                rule.mapping_rule_name,
                                transformationName
                              );
                            }}
                          />
                        ) : (
                          <span className="text-xs text-muted-foreground">
                            {rule.mapping_rule_transformation_name || 'Direct'}
                          </span>
                        )}
                      </div>
                    </td>

                    {/* Target Column */}
                    <td className="px-4 py-3 align-top">
                      <div className="flex flex-col gap-1">
                        <div className="flex items-center gap-2">
                          <div className="font-mono text-sm text-foreground font-medium">
                            {targetInfo.tableName && targetInfo.label ? `${targetInfo.tableName}.${targetInfo.label}` : targetInfo.label}
                          </div>
                          {targetInfo.isPrivileged && (
                            <div className="group relative">
                              <Shield className={`h-4 w-4 ${getPrivilegedColor(targetInfo.detectionConfidence)}`} />
                              <div className="invisible group-hover:visible absolute z-10 px-3 py-2 text-xs bg-gray-900 text-white rounded-md shadow-lg whitespace-nowrap -top-2 left-6">
                                <div className="font-semibold mb-1">Privileged Data</div>
                                {targetInfo.privilegedClassification && (
                                  <div>Classification: {targetInfo.privilegedClassification}</div>
                                )}
                                {targetInfo.detectionConfidence !== null && targetInfo.detectionConfidence !== undefined && (
                                  <div>Confidence: {(targetInfo.detectionConfidence * 100).toFixed(0)}%</div>
                                )}
                                {targetInfo.detectionMethod && (
                                  <div>Method: {targetInfo.detectionMethod}</div>
                                )}
                              </div>
                            </div>
                          )}
                        </div>
                        <div className="flex items-center gap-1.5 flex-wrap">
                          <span className="text-xs font-mono px-1.5 py-0.5 bg-muted rounded text-muted-foreground">
                            {targetInfo.type}
                          </span>
                          {targetInfo.constraints.map((constraint) => (
                            <span
                              key={constraint}
                              className="text-[10px] px-1.5 py-0.5 bg-primary/10 text-primary rounded font-medium"
                            >
                              {constraint}
                            </span>
                          ))}
                        </div>
                        {isExpanded && (
                          <div className="text-xs text-muted-foreground mt-2 space-y-1 pt-2 border-t border-border">
                            <div>
                              <span className="font-semibold">URI:</span> {rule.mapping_rule_target}
                            </div>
                            {targetInfo.resourceItem && (
                              <>
                                {targetInfo.resourceItem.default_value && (
                                  <div>
                                    <span className="font-semibold">Default:</span> {targetInfo.resourceItem.default_value}
                                  </div>
                                )}
                                {targetInfo.resourceItem.item_comment && (
                                  <div>
                                    <span className="font-semibold">Description:</span> {targetInfo.resourceItem.item_comment}
                                  </div>
                                )}
                              </>
                            )}
                            {rule.targetColumn && (
                              <>
                                {(rule.targetColumn.columnDefault || rule.targetColumn.column_default) && (
                                  <div>
                                    <span className="font-semibold">Default:</span> {rule.targetColumn.columnDefault || rule.targetColumn.column_default}
                                  </div>
                                )}
                                {(rule.targetColumn.dataCategory || rule.targetColumn.data_category) && (
                                  <div>
                                    <span className="font-semibold">Category:</span> {rule.targetColumn.dataCategory || rule.targetColumn.data_category}
                                  </div>
                                )}
                              </>
                            )}
                          </div>
                        )}
                      </div>
                    </td>

                    {/* Validation Status */}
                    <td className="px-4 py-3 align-top">
                      <div className="flex justify-center">
                        {rule.validation && <RuleValidationIcon validation={rule.validation} />}
                      </div>
                    </td>

                    {/* Actions */}
                    <td className="px-4 py-3 align-top" onClick={(e) => e.stopPropagation()}>
                      <div className="flex items-center justify-end gap-2">
                        {onDelete && (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              onDelete(rule.mapping_rule_id);
                            }}
                            className="p-1.5 text-muted-foreground hover:text-destructive hover:bg-destructive/10 rounded-md transition-colors"
                            title="Delete rule"
                          >
                            <Trash2 className="h-4 w-4" />
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {filteredRules.length === 0 && (
          <div className="p-8 text-center text-muted-foreground">
            No rules match your search or filter criteria
          </div>
        )}
      </div>
    </div>
  );
}

