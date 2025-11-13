'use client';

import { useState, useEffect, useMemo } from 'react';
import { useRouter } from 'next/navigation';
import { useMappingRules } from '@/lib/hooks/useMappingRules';
import { useMapping } from '@/lib/hooks/useMappings';
import { useTransformations } from '@/lib/hooks/useTransformations';
import { useMultipleDatabaseSchemas } from '@/lib/hooks/useDatabaseSchemaInfo';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { ArrowLeft, Plus, RefreshCw, Trash2, Link as Link2, CheckCircle, AlertCircle, AlertTriangle } from 'lucide-react';
import { MappingRulesTableEnhanced } from '@/components/mappings/MappingRulesTableEnhanced';
import { RuleDetailsPanel } from '@/components/mappings/RuleDetailsPanel';
import { AddMappingRuleDialog } from '@/components/mappings/AddMappingRuleDialog';
import { ValidationResultsModal } from '@/components/mappings/ValidationResultsModal';
import { api } from '@/lib/api/endpoints';
import { parseResourceURI } from '@/lib/utils/uri-parser';
import { validateMappingRule, getValidationSummary } from '@/lib/utils/type-validator';
import type { MappingRule, SchemaColumn, ValidateMappingResponse } from '@/lib/api/types';

interface MappingDetailPageProps {
  params: Promise<{
    workspaceId: string;
    mappingName: string;
  }>;
}

export default function MappingDetailPage({ params }: MappingDetailPageProps) {
  const router = useRouter();
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [mappingName, setMappingName] = useState<string>('');
  const [showAddRuleDialog, setShowAddRuleDialog] = useState(false);
  const [selectedRule, setSelectedRule] = useState<MappingRule | null>(null);
  const [showValidationModal, setShowValidationModal] = useState(false);
  const [validationResults, setValidationResults] = useState<ValidateMappingResponse['data'] | null>(null);
  const [isValidating, setIsValidating] = useState(false);
  const [showDisplayNames, setShowDisplayNames] = useState(false);
  const { showToast } = useToast();
  
  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wsId, mappingName: mName }) => {
      setWorkspaceId(wsId);
      setMappingName(decodeURIComponent(mName));
    });
  }, [params]);

  // Fetch data
  const { mapping, isLoading: mappingLoading, refetch: refetchMapping } = useMapping(workspaceId, mappingName);
  const { mappingRules, isLoading: rulesLoading, error: rulesError, refetch: refetchRules } = useMappingRules(workspaceId, mappingName);
  const { transformations, isLoading: transformationsLoading } = useTransformations();

  // Extract database names from the mapping rule metadata
  const { sourceDatabaseName, targetDatabaseName } = useMemo(() => {
    let sourceDb: string | null = null;
    let targetDb: string | null = null;

    // PRIORITY 1: Get database names from rule metadata (this has actual names, not IDs)
    if (mappingRules.length > 0) {
      for (const rule of mappingRules) {
        if (rule.mapping_rule_metadata) {
          if (!sourceDb && rule.mapping_rule_metadata.source_database_name) {
            sourceDb = rule.mapping_rule_metadata.source_database_name;
          }
          if (!targetDb && rule.mapping_rule_metadata.target_database_name) {
            targetDb = rule.mapping_rule_metadata.target_database_name;
          }
          
          // If we found both, break early
          if (sourceDb && targetDb) {
            break;
          }
        }
      }
    }

    // PRIORITY 2: Try to get from mapping metadata (mapping_object field)
    if (!sourceDb || !targetDb) {
      try {
        const mappingObj = mapping?.map_object;
        if (mappingObj) {
          if (!sourceDb && mappingObj.source_database_name) {
            sourceDb = mappingObj.source_database_name;
          }
          if (!targetDb && mappingObj.target_database_name) {
            targetDb = mappingObj.target_database_name;
          }
        }
      } catch (e) {
        console.warn('Failed to parse mapping object:', e);
      }
    }

    // Note: We intentionally do NOT fall back to parsing URIs as they contain database IDs, not names
    // The API requires database names to fetch database details
    // If metadata is not available, the schemas will simply not be fetched

    // Log what we found for debugging
    if (process.env.NODE_ENV === 'development') {
      console.log('[MappingDetail] Database names extracted:', { 
        sourceDb, 
        targetDb,
        fromRulesMetadata: mappingRules.some(r => r.mapping_rule_metadata?.source_database_name || r.mapping_rule_metadata?.target_database_name),
        fromMappingObject: !!mapping?.map_object,
      });
    }

    return { sourceDatabaseName: sourceDb, targetDatabaseName: targetDb };
  }, [mapping, mappingRules]);

  // Fetch schemas for both databases
  const databaseNames = useMemo(() => {
    const names: (string | null)[] = [];
    if (sourceDatabaseName) names.push(sourceDatabaseName);
    if (targetDatabaseName && targetDatabaseName !== sourceDatabaseName) {
      names.push(targetDatabaseName);
    }
    return names;
  }, [sourceDatabaseName, targetDatabaseName]);

  const { schemas, isLoading: schemasLoading } = useMultipleDatabaseSchemas(workspaceId, databaseNames);

  // Build column maps for quick lookup
  const { sourceSchemaColumns, targetSchemaColumns } = useMemo(() => {
    const sourceColumns = new Map<string, SchemaColumn>();
    const targetColumns = new Map<string, SchemaColumn>();

    if (sourceDatabaseName && schemas[sourceDatabaseName]) {
      const schema = schemas[sourceDatabaseName];
      schema.tables?.forEach((table) => {
        table.columns?.forEach((column) => {
          const key = `${table.name}.${column.name}`;
          sourceColumns.set(key, column);
        });
      });
    }

    if (targetDatabaseName && schemas[targetDatabaseName]) {
      const schema = schemas[targetDatabaseName];
      schema.tables?.forEach((table) => {
        table.columns?.forEach((column) => {
          const key = `${table.name}.${column.name}`;
          targetColumns.set(key, column);
        });
      });
    }

    return { sourceSchemaColumns: sourceColumns, targetSchemaColumns: targetColumns };
  }, [schemas, sourceDatabaseName, targetDatabaseName]);

  // Compute unmapped items from container items
  const { unmappedSourceItems, unmappedTargetItems } = useMemo(() => {
    const sourceItems = mapping?.source_container_items || [];
    const targetItems = mapping?.target_container_items || [];

    // Build set of mapped resource URIs
    const mappedSourceUris = new Set(
      mappingRules.flatMap(rule => rule.source_items?.map(item => item.resource_uri) || [rule.mapping_rule_source])
    );
    const mappedTargetUris = new Set(
      mappingRules.flatMap(rule => rule.target_items?.map(item => item.resource_uri) || [rule.mapping_rule_target])
    );

    // Find unmapped items
    const unmappedSource = sourceItems.filter(item => !mappedSourceUris.has(item.resource_uri));
    const unmappedTarget = targetItems.filter(item => !mappedTargetUris.has(item.resource_uri));

    return {
      unmappedSourceItems: unmappedSource,
      unmappedTargetItems: unmappedTarget,
    };
  }, [mapping, mappingRules]);

  // Compute validation summary for rules
  const validationSummary = useMemo(() => {
    const validations = mappingRules.map((rule) => {
      const sourceParsed = parseResourceURI(rule.mapping_rule_source);
      const targetParsed = parseResourceURI(rule.mapping_rule_target);
      
      let sourceColumn = null;
      let targetColumn = null;
      
      if (sourceParsed?.tableName && sourceParsed?.columnName) {
        const key = `${sourceParsed.tableName}.${sourceParsed.columnName}`;
        sourceColumn = sourceSchemaColumns.get(key) || null;
      }
      
      if (targetParsed?.tableName && targetParsed?.columnName) {
        const key = `${targetParsed.tableName}.${targetParsed.columnName}`;
        targetColumn = targetSchemaColumns.get(key) || null;
      }
      
      return validateMappingRule(
        sourceColumn,
        targetColumn,
        rule.mapping_rule_transformation_name
      );
    });
    
    return getValidationSummary(validations);
  }, [mappingRules, sourceSchemaColumns, targetSchemaColumns]);

  // Find column info for selected rule
  const selectedRuleWithSchema = useMemo(() => {
    if (!selectedRule) return null;

    const sourceParsed = parseResourceURI(selectedRule.mapping_rule_source);
    const targetParsed = parseResourceURI(selectedRule.mapping_rule_target);

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

    const validation = validateMappingRule(
      sourceColumn,
      targetColumn,
      selectedRule.mapping_rule_transformation_name
    );

    return {
      sourceColumn,
      targetColumn,
      validation,
    };
  }, [selectedRule, sourceSchemaColumns, targetSchemaColumns]);

  const handleRefresh = () => {
    refetchMapping();
    refetchRules();
    showToast({
      type: 'info',
      title: 'Refreshing mapping data...',
    });
  };

  const handleDeleteRule = async (ruleId: string) => {
    if (!confirm('Are you sure you want to delete this rule?')) return;

    try {
      const ruleName = mappingRules.find(r => r.mapping_rule_id === ruleId)?.mapping_rule_name;
      if (!ruleName) return;

      await api.mappingRules.remove(workspaceId, mappingName, ruleName, { delete_rule: true });
      showToast({
        type: 'success',
        title: 'Rule deleted successfully',
      });
      
      // Close details panel if the deleted rule was selected
      if (selectedRule?.mapping_rule_id === ruleId) {
        setSelectedRule(null);
      }
      
      refetchRules();
      refetchMapping(); // Refresh mapping to update validation status
    } catch (error) {
      console.error('Error deleting rule:', error);
      showToast({
        type: 'error',
        title: 'Failed to delete rule',
      });
    }
  };

  const handleUpdateTransformation = async (ruleId: string, ruleName: string, transformationName: string) => {
    try {
      await api.mappingRules.modify(workspaceId, mappingName, ruleName, {
        transformation: transformationName,
      });
      
      showToast({
        type: 'success',
        title: 'Transformation updated',
      });
      
      refetchRules();
      refetchMapping(); // Refresh mapping to update validation status
    } catch (error) {
      console.error('Error updating transformation:', error);
      showToast({
        type: 'error',
        title: 'Failed to update transformation',
      });
      throw error;
    }
  };

  const handleUpdateDescription = async (ruleId: string, ruleName: string, description: string) => {
    try {
      // Note: The API endpoint doesn't currently support updating just the description
      // This is a placeholder for future implementation when the API supports it
      console.log('Updating description for rule:', ruleId, 'New description:', description);
      
      await api.mappingRules.modify(workspaceId, mappingName, ruleName, {
        rule_name: ruleName, // Keep the same name
      });
      
      showToast({
        type: 'success',
        title: 'Description updated',
      });
      
      refetchRules();
      refetchMapping(); // Refresh mapping to update validation status
    } catch (error) {
      console.error('Error updating description:', error);
      showToast({
        type: 'error',
        title: 'Failed to update description',
      });
      throw error;
    }
  };

  const handleDeleteMapping = async () => {
    if (!confirm(`Are you sure you want to delete mapping "${mappingName}"? This will remove all associated rules and cannot be undone.`)) {
      return;
    }

    try {
      await api.mappings.delete(workspaceId, mappingName);
      showToast({
        type: 'success',
        title: 'Mapping deleted successfully',
      });
      router.push(`/workspaces/${workspaceId}/mappings`); // Navigate back to list
    } catch (error: unknown) {
      showToast({
        type: 'error',
        title: 'Failed to delete mapping',
        message: error instanceof Error ? error.message : 'An error occurred while deleting the mapping.',
      });
    }
  };

  const handleValidateMapping = async () => {
    setIsValidating(true);
    try {
      const result = await api.mappings.validate(workspaceId, mappingName);
      setValidationResults(result.data);
      setShowValidationModal(true);
      showToast({
        type: result.data.is_valid ? 'success' : 'warning',
        title: result.data.is_valid ? 'Mapping is valid' : 'Mapping has validation issues',
      });
      // Refresh mapping to get updated validation status
      refetchMapping();
    } catch (error: unknown) {
      showToast({
        type: 'error',
        title: 'Validation failed',
        message: error instanceof Error ? error.message : 'An error occurred during validation.',
      });
    } finally {
      setIsValidating(false);
    }
  };

  const getRelationshipStatusColor = (status?: string) => {
    switch (status?.toLowerCase()) {
      case 'active':
      case 'running':
        return 'text-red-600 dark:text-red-400';
      case 'pending':
        return 'text-yellow-600 dark:text-yellow-400';
      case 'stopped':
        return 'text-gray-600 dark:text-gray-400';
      case 'error':
        return 'text-red-600 dark:text-red-400';
      default:
        return 'text-gray-600 dark:text-gray-400';
    }
  };

  const getRelationshipStatusBg = (status?: string) => {
    switch (status?.toLowerCase()) {
      case 'active':
      case 'running':
        return 'bg-red-100 dark:bg-red-900/30 border-red-200 dark:border-red-800/30';
      case 'pending':
        return 'bg-yellow-100 dark:bg-yellow-900/30 border-yellow-200 dark:border-yellow-800/30';
      case 'stopped':
        return 'bg-gray-100 dark:bg-gray-900/30 border-gray-200 dark:border-gray-800/30';
      case 'error':
        return 'bg-red-100 dark:bg-red-900/30 border-red-200 dark:border-red-800/30';
      default:
        return 'bg-gray-100 dark:bg-gray-900/30 border-gray-200 dark:border-gray-800/30';
    }
  };

  const getRelationshipSeverity = (status?: string): 'critical' | 'warning' | 'info' => {
    switch (status?.toLowerCase()) {
      case 'active':
      case 'running':
        return 'critical';
      case 'pending':
        return 'warning';
      default:
        return 'info';
    }
  };

  if (!workspaceId || !mappingName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  const isLoading = rulesLoading || mappingLoading || schemasLoading;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <button
          onClick={() => router.back()}
          className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground mb-4"
        >
          <ArrowLeft className="h-4 w-4 mr-1" />
          Back to Mappings
        </button>
        
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold text-foreground">{mappingName}</h2>
            <p className="text-muted-foreground mt-2">
              {mapping?.mapping_description || 'Mapping rules define how data is transformed from source to target'}
            </p>
            {((sourceDatabaseName && mapping?.source_table_name) || (targetDatabaseName && mapping?.target_table_name)) && (
              <div className="flex items-center gap-3 mt-3 text-sm">
                {sourceDatabaseName && mapping?.source_table_name && (
                  <div className="flex items-center gap-1.5">
                    <span className="text-muted-foreground">Source:</span>
                    <span className="font-mono font-medium text-foreground">
                      {sourceDatabaseName}.{mapping.source_table_name}
                    </span>
                  </div>
                )}
                {targetDatabaseName && mapping?.target_table_name && (
                  <div className="flex items-center gap-1.5">
                    <span className="text-muted-foreground">Target:</span>
                    <span className="font-mono font-medium text-foreground">
                      {targetDatabaseName}.{mapping.target_table_name}
                    </span>
                  </div>
                )}
                {/* Compact Rules Status */}
                {!isLoading && mappingRules.length > 0 && (
                  <>
                    <div className="h-4 w-px bg-border" />
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-muted-foreground">Rules:</span>
                      <div className="flex items-center gap-1.5">
                        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-card border border-border">
                          {validationSummary.total}
                        </span>
                        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">
                          {validationSummary.valid}
                        </span>
                        {(validationSummary.warnings + validationSummary.info) > 0 && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300">
                            {validationSummary.warnings + validationSummary.info}
                          </span>
                        )}
                        {validationSummary.errors > 0 && (
                          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300">
                            {validationSummary.errors}
                          </span>
                        )}
                      </div>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
          <div className="flex items-center space-x-3">
            {/* Display Names Toggle */}
            <label className="inline-flex items-center cursor-pointer px-4 py-2 bg-background border border-border rounded-md hover:bg-accent transition-colors">
              <input
                type="checkbox"
                checked={showDisplayNames}
                onChange={(e) => setShowDisplayNames(e.target.checked)}
                className="sr-only peer"
              />
              <div className="relative w-9 h-5 bg-muted peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white dark:after:bg-gray-100 after:border-gray-300 dark:after:border-gray-400 after:border after:rounded-full after:h-4 after:w-4 after:transition-all after:shadow-sm peer-checked:bg-gray-500 peer-checked:after:border-primary-foreground/20"></div>
              <span className="ml-3 text-sm font-medium text-foreground">
                Use Item Names
              </span>
            </label>
            <button
              onClick={handleRefresh}
              className="inline-flex items-center px-4 py-2 bg-background border border-border text-foreground rounded-md hover:bg-accent transition-colors"
              disabled={isLoading}
            >
              <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={() => setShowAddRuleDialog(true)}
              className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              disabled={transformationsLoading}
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Rule
            </button>
            <button
              onClick={handleDeleteMapping}
              className="inline-flex items-center px-4 py-2 border border-destructive text-destructive rounded-md hover:bg-destructive hover:text-destructive-foreground transition-colors"
            >
              <Trash2 className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Error State */}
      {rulesError && (
        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4">
          <p className="text-destructive text-sm">{rulesError.message}</p>
        </div>
      )}

      {/* Schema Loading Notice */}
      {schemasLoading && databaseNames.length > 0 && (
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800/30 rounded-lg p-4">
          <p className="text-blue-800 dark:text-blue-200 text-sm">
            Loading database schemas for validation...
          </p>
        </div>
      )}

      {/* Relationship Status - Warning Banner */}
      {mapping?.relationship_infos && mapping.relationship_infos.length > 0 && (
        <div className={`border-2 rounded-lg p-5 ${
          mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running')
            ? 'bg-red-50 dark:bg-red-900/20 border-red-500 dark:border-red-700'
            : mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'pending')
            ? 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-500 dark:border-yellow-700'
            : 'bg-card border-border'
        }`}>
          <div className="flex items-start gap-4">
            {/* Icon */}
            <div className={`flex-shrink-0 w-12 h-12 rounded-lg flex items-center justify-center ${
              mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running')
                ? 'bg-red-100 dark:bg-red-900/40'
                : mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'pending')
                ? 'bg-yellow-100 dark:bg-yellow-900/40'
                : 'bg-card'
            }`}>
              {mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running') ? (
                <AlertTriangle className="h-6 w-6 text-red-600 dark:text-red-400" />
              ) : mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'pending') ? (
                <AlertTriangle className="h-6 w-6 text-yellow-600 dark:text-yellow-400" />
              ) : (
                <Link2 className="h-6 w-6 text-muted-foreground" />
              )}
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-2">
                <h3 className={`text-base font-bold ${
                  mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running')
                    ? 'text-red-800 dark:text-red-200'
                    : mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'pending')
                    ? 'text-yellow-800 dark:text-yellow-200'
                    : 'text-foreground'
                }`}>
                  {mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running')
                    ? '⚠️ Active Relationship - High Risk'
                    : mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'pending')
                    ? '⚠️ Pending Relationship - Caution Required'
                    : 'Relationship Status'}
                </h3>
              </div>

              {/* Warning Message */}
              {mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running') && (
                <div className="mb-3 p-3 bg-red-100 dark:bg-red-900/30 border border-red-300 dark:border-red-700 rounded-md">
                  <p className="text-sm font-semibold text-red-900 dark:text-red-100 mb-1">
                    ⛔ Critical Warning: This mapping is part of an active (running) relationship
                  </p>
                  <p className="text-sm text-red-800 dark:text-red-200">
                    Modifying this mapping while the relationship is running can cause data inconsistencies, replication errors, or data loss. 
                    It is <span className="font-bold">strongly recommended</span> to stop the relationship before making any changes.
                  </p>
                </div>
              )}

              {mapping.relationship_infos.some(rel => rel.status?.toLowerCase() === 'pending') && (
                <div className="mb-3 p-3 bg-yellow-100 dark:bg-yellow-900/30 border border-yellow-300 dark:border-yellow-700 rounded-md">
                  <p className="text-sm font-semibold text-yellow-900 dark:text-yellow-100 mb-1">
                    ⚠️ Warning: This mapping is part of a pending relationship
                  </p>
                  <p className="text-sm text-yellow-800 dark:text-yellow-200">
                    Changes to this mapping may affect the relationship when it starts. Ensure modifications are intentional.
                  </p>
                </div>
              )}

              {/* Relationship List */}
              <div className="space-y-2">
                {mapping.relationship_infos.map((rel, idx) => {
                  const severity = getRelationshipSeverity(rel.status);
                  const isActive = rel.status?.toLowerCase() === 'active' || rel.status?.toLowerCase() === 'running';
                  return (
                    <div key={idx} className={`flex items-center justify-between p-3 rounded-md border ${
                      severity === 'critical' 
                        ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800/50'
                        : severity === 'warning'
                        ? 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800/50'
                        : 'bg-background border-border'
                    }`}>
                      <div className="flex items-center gap-2 flex-1 min-w-0">
                        <span className={`text-sm font-medium ${
                          severity === 'critical' 
                            ? 'text-red-800 dark:text-red-200'
                            : severity === 'warning'
                            ? 'text-yellow-800 dark:text-yellow-200'
                            : 'text-muted-foreground'
                        }`}>
                          Relationship:
                        </span>
                        <span className="font-mono text-sm text-foreground font-semibold truncate">
                          {rel.relationship_name}
                        </span>
                      </div>
                      <div className="flex-shrink-0 ml-3">
                        <span className={`inline-flex items-center px-3 py-1.5 rounded-full text-xs font-bold border-2 ${getRelationshipStatusBg(rel.status)}`}>
                          <span className={`inline-flex items-center ${getRelationshipStatusColor(rel.status)}`}>
                            {isActive && (
                              <span className="inline-flex h-2.5 w-2.5 rounded-full bg-red-600 dark:bg-red-400 animate-pulse mr-2"></span>
                            )}
                            {rel.status?.toLowerCase() === 'pending' && (
                              <AlertTriangle className="h-3.5 w-3.5 mr-1.5" />
                            )}
                            <span className="uppercase tracking-wide">{rel.status}</span>
                          </span>
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Validation Status */}
      <div className="bg-card border border-border rounded-lg p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {mapping?.validated ? (
              <div className="w-10 h-10 bg-green-100 dark:bg-green-900/30 rounded-lg flex items-center justify-center">
                <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400" />
              </div>
            ) : (
              <div className="w-10 h-10 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg flex items-center justify-center">
                <AlertCircle className="h-5 w-5 text-yellow-600 dark:text-yellow-400" />
              </div>
            )}
            <div>
              <div className="text-sm font-semibold text-foreground">
                {mapping?.validated ? 'Validated' : 'Not Validated'}
              </div>
              {mapping?.validated_at && (
                <div className="text-xs text-muted-foreground">
                  Last validated: {new Date(mapping.validated_at).toLocaleString()}
                </div>
              )}
            </div>
          </div>
          <button
            onClick={handleValidateMapping}
            disabled={isValidating}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isValidating ? (
              <>
                <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                Validating...
              </>
            ) : (
              <>
                <CheckCircle className="h-4 w-4 mr-2" />
                Validate Mapping
              </>
            )}
          </button>
        </div>
        {(mapping?.validation_errors && mapping.validation_errors.length > 0) || 
         (mapping?.validation_warnings && mapping.validation_warnings.length > 0) ? (
          <div className="mt-3 pt-3 border-t border-border flex items-center gap-4 text-sm">
            {mapping.validation_errors && mapping.validation_errors.length > 0 && (
              <div className="flex items-center gap-1.5">
                <span className="inline-flex h-2 w-2 rounded-full bg-red-600 dark:bg-red-400"></span>
                <span className="text-red-700 dark:text-red-300 font-medium">
                  {mapping.validation_errors.length} error{mapping.validation_errors.length !== 1 ? 's' : ''}
                </span>
              </div>
            )}
            {mapping.validation_warnings && mapping.validation_warnings.length > 0 && (
              <div className="flex items-center gap-1.5">
                <span className="inline-flex h-2 w-2 rounded-full bg-yellow-600 dark:bg-yellow-400"></span>
                <span className="text-yellow-700 dark:text-yellow-300 font-medium">
                  {mapping.validation_warnings.length} warning{mapping.validation_warnings.length !== 1 ? 's' : ''}
                </span>
              </div>
            )}
          </div>
        ) : null}
      </div>

      {/* Unmapped Columns Warning */}
      {!isLoading && (unmappedSourceItems.length > 0 || unmappedTargetItems.length > 0) && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800/30 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <div className="flex-shrink-0 mt-0.5">
              <svg className="h-5 w-5 text-yellow-600 dark:text-yellow-400" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 5zm0 9a1 1 0 100-2 1 1 0 000 2z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="flex-1">
              <h4 className="text-sm font-semibold text-yellow-800 dark:text-yellow-200 mb-1">
                Unmapped Columns Detected
              </h4>
              <div className="text-sm text-yellow-700 dark:text-yellow-300 space-y-1">
                {unmappedSourceItems.length > 0 && (
                  <div>
                    <span className="font-medium">{unmappedSourceItems.length}</span> source column{unmappedSourceItems.length !== 1 ? 's' : ''} not mapped
                    {unmappedSourceItems.some(item => item.is_privileged) && (
                      <span className="ml-2 text-xs px-2 py-0.5 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 rounded">
                        {unmappedSourceItems.filter(item => item.is_privileged).length} privileged
                      </span>
                    )}
                  </div>
                )}
                {unmappedTargetItems.length > 0 && (
                  <div>
                    <span className="font-medium">{unmappedTargetItems.length}</span> target column{unmappedTargetItems.length !== 1 ? 's' : ''} not mapped
                    {unmappedTargetItems.some(item => item.is_privileged) && (
                      <span className="ml-2 text-xs px-2 py-0.5 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 rounded">
                        {unmappedTargetItems.filter(item => item.is_privileged).length} privileged
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Enhanced Rules Table */}
      <div>
        <MappingRulesTableEnhanced
          rules={mappingRules}
          transformations={transformations}
          isLoading={isLoading}
          sourceSchemaColumns={sourceSchemaColumns}
          targetSchemaColumns={targetSchemaColumns}
          sourceContainerItems={mapping?.source_container_items}
          targetContainerItems={mapping?.target_container_items}
          showDisplayNames={showDisplayNames}
          onDelete={handleDeleteRule}
          onUpdateTransformation={handleUpdateTransformation}
          onRuleClick={setSelectedRule}
        />
      </div>

      {/* Add Rule Dialog */}
      {showAddRuleDialog && !transformationsLoading && (
        <AddMappingRuleDialog
          workspaceId={workspaceId}
          mappingName={mappingName}
          transformations={transformations}
          onClose={() => setShowAddRuleDialog(false)}
          onSuccess={() => {
            refetchRules();
            refetchMapping(); // Refresh mapping to update validation status
          }}
        />
      )}

      {/* Rule Details Panel */}
      {selectedRule && (
        <RuleDetailsPanel
          rule={selectedRule}
          sourceColumn={selectedRuleWithSchema?.sourceColumn}
          targetColumn={selectedRuleWithSchema?.targetColumn}
          validation={selectedRuleWithSchema?.validation}
          onClose={() => setSelectedRule(null)}
          onUpdateDescription={handleUpdateDescription}
        />
      )}

      {/* Validation Results Modal */}
      {showValidationModal && validationResults && (
        <ValidationResultsModal
          results={validationResults}
          onClose={() => setShowValidationModal(false)}
        />
      )}
    </div>
  );
}
