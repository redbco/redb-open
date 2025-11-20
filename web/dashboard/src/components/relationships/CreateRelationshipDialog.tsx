'use client';

import { useState, useEffect } from 'react';
import { X, Loader2 } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';
import type { Mapping, CreateRelationshipRequest } from '@/lib/api/types';

interface CreateRelationshipDialogProps {
  isOpen: boolean;
  onClose: () => void;
  workspaceName: string;
  onSuccess: () => void;
}

// Parse resource URI format: redb:/data/database/{id}/table/{name}/column/{col}
function parseResourceURI(uri: string): { databaseId: string; tableName: string } | null {
  try {
    // Remove protocol prefix
    let path = uri.replace(/^redb:\/+/, '');
    
    // Split by / and filter empty strings
    const parts = path.split('/').filter(p => p !== '');
    
    // Expected format: data/database/{id}/table/{name}/column/{col}
    if (parts.length < 7) {
      return null;
    }
    
    if (parts[0] !== 'data' || parts[1] !== 'database' || parts[3] !== 'table' || parts[5] !== 'column') {
      return null;
    }
    
    return {
      databaseId: parts[2],
      tableName: parts[4]
    };
  } catch (error) {
    console.error('Error parsing resource URI:', error);
    return null;
  }
}

export function CreateRelationshipDialog({ 
  isOpen, 
  onClose, 
  workspaceName,
  onSuccess 
}: CreateRelationshipDialogProps) {
  const { showToast } = useToast();
  const [mappings, setMappings] = useState<Mapping[]>([]);
  const [selectedMapping, setSelectedMapping] = useState<Mapping | null>(null);
  const [selectedMappingDetails, setSelectedMappingDetails] = useState<Mapping | null>(null);
  const [relationshipName, setRelationshipName] = useState('');
  const [relationshipDescription, setRelationshipDescription] = useState('');
  const [relationshipType] = useState('replication');
  const [isLoadingMappings, setIsLoadingMappings] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [sourceInfo, setSourceInfo] = useState<{ databaseId: string; tableName: string } | null>(null);
  const [targetInfo, setTargetInfo] = useState<{ databaseId: string; tableName: string } | null>(null);
  const [isManualName, setIsManualName] = useState(false);

  // Fetch mappings when dialog opens
  useEffect(() => {
    if (isOpen) {
      loadMappings();
    } else {
      // Reset form when dialog closes
      setSelectedMapping(null);
      setSelectedMappingDetails(null);
      setRelationshipName('');
      setRelationshipDescription('');
      setSourceInfo(null);
      setTargetInfo(null);
      setIsManualName(false);
    }
  }, [isOpen, workspaceName]);

  // Fetch mapping details when a mapping is selected
  useEffect(() => {
    const fetchMappingDetails = async () => {
      if (!selectedMapping) {
        setSelectedMappingDetails(null);
        setSourceInfo(null);
        setTargetInfo(null);
        return;
      }

      try {
        // Fetch full mapping details including rules
        const mappingDetails = await api.mappings.show(workspaceName, selectedMapping.mapping_name);
        setSelectedMappingDetails(mappingDetails.mapping);
        
        if (mappingDetails.mapping.mapping_rules && mappingDetails.mapping.mapping_rules.length > 0) {
          const firstRule = mappingDetails.mapping.mapping_rules[0];
          
          // Parse source and target URIs
          const source = parseResourceURI(firstRule.mapping_rule_source);
          const target = parseResourceURI(firstRule.mapping_rule_target);
          
          setSourceInfo(source);
          setTargetInfo(target);
          
          // Auto-generate relationship name if not manually set
          if (!isManualName && source && target) {
            // Get database names from metadata if available
            const sourceDatabaseName = firstRule.mapping_rule_metadata?.source_database_name || source.databaseId;
            const targetDatabaseName = firstRule.mapping_rule_metadata?.target_database_name || target.databaseId;
            setRelationshipName(`${sourceDatabaseName}_to_${targetDatabaseName}`);
          }
          
          // Always auto-generate description (not shown in UI)
          if (source && target) {
            const sourceDatabaseName = firstRule.mapping_rule_metadata?.source_database_name || source.databaseId;
            const targetDatabaseName = firstRule.mapping_rule_metadata?.target_database_name || target.databaseId;
            const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
            setRelationshipDescription(
              `${relationshipType} relationship from ${sourceDatabaseName}.${source.tableName} to ${targetDatabaseName}.${target.tableName} using mapping ${mappingDetails.mapping.mapping_name} created on ${timestamp}`
            );
          }
        }
      } catch (error: any) {
        showToast({
          type: 'error',
          title: 'Failed to Load Mapping Details',
          message: error.message || 'Could not fetch mapping details.'
        });
        setSelectedMappingDetails(null);
        setSourceInfo(null);
        setTargetInfo(null);
      }
    };

    fetchMappingDetails();
  }, [selectedMapping, workspaceName, relationshipType, isManualName, showToast]);

  const loadMappings = async () => {
    setIsLoadingMappings(true);
    try {
      const response = await api.mappings.list(workspaceName);
      setMappings(response.mappings || []);
      if (response.mappings.length === 0) {
        showToast({
          type: 'info',
          title: 'No Mappings Found',
          message: 'You need to create a mapping first before creating a relationship.'
        });
      }
    } catch (error: any) {
      showToast({
        type: 'error',
        title: 'Failed to Load Mappings',
        message: error.message || 'Could not fetch mappings.'
      });
    } finally {
      setIsLoadingMappings(false);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!selectedMapping || !sourceInfo || !targetInfo) {
      showToast({
        type: 'error',
        title: 'Validation Error',
        message: 'Please select a valid mapping with source and target information.'
      });
      return;
    }
    
    // Get database names from metadata if available
    const firstRule = selectedMappingDetails?.mapping_rules?.[0];
    const sourceDatabaseName = firstRule?.mapping_rule_metadata?.source_database_name || sourceInfo.databaseId;
    const targetDatabaseName = firstRule?.mapping_rule_metadata?.target_database_name || targetInfo.databaseId;
    
    // Auto-generate name if blank
    let finalName = relationshipName.trim();
    if (!finalName) {
      finalName = `${sourceDatabaseName}_to_${targetDatabaseName}`;
    }
    
    // Always generate fresh description with timestamp
    const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
    const finalDescription = `${relationshipType} relationship from ${sourceDatabaseName}.${sourceInfo.tableName} to ${targetDatabaseName}.${targetInfo.tableName} using mapping ${selectedMapping.mapping_name} created on ${timestamp}`;
    
    setIsSubmitting(true);
    try {
      const request: CreateRelationshipRequest = {
        relationship_name: finalName,
        relationship_description: finalDescription,
        relationship_type: relationshipType,
        relationship_source_database_id: sourceInfo.databaseId,
        relationship_source_table_name: sourceInfo.tableName,
        relationship_target_database_id: targetInfo.databaseId,
        relationship_target_table_name: targetInfo.tableName,
        mapping_id: selectedMapping.mapping_id,
        policy_id: ''
      };
      
      await api.relationships.create(workspaceName, request);
      
      showToast({
        type: 'success',
        title: 'Relationship Created',
        message: `Successfully created relationship '${finalName}'.`
      });
      
      onSuccess();
      onClose();
    } catch (error: any) {
      showToast({
        type: 'error',
        title: 'Failed to Create Relationship',
        message: error.message || 'An error occurred while creating the relationship.'
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-lg w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <h2 className="text-2xl font-bold text-foreground">Create Relationship</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
            disabled={isSubmitting}
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Body */}
        <form onSubmit={handleSubmit} className="p-6 space-y-6">
          {/* Mapping Selection */}
          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Select Mapping <span className="text-red-500">*</span>
            </label>
            {isLoadingMappings ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : (
              <select
                value={selectedMapping?.mapping_id || ''}
                onChange={(e) => {
                  const mapping = mappings.find(m => m.mapping_id === e.target.value);
                  setSelectedMapping(mapping || null);
                }}
                className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
                required
                disabled={isSubmitting}
              >
                <option value="">Select a mapping...</option>
                {mappings.map((mapping) => (
                  <option key={mapping.mapping_id} value={mapping.mapping_id}>
                    {mapping.mapping_name} ({mapping.mapping_rule_count || 0} rules)
                  </option>
                ))}
              </select>
            )}
            {selectedMapping && (
              <p className="mt-2 text-sm text-muted-foreground">
                {selectedMapping.mapping_description || 'No description available'}
              </p>
            )}
          </div>

          {/* Source and Target Info */}
          {selectedMapping && sourceInfo && targetInfo && (
            <div className="grid grid-cols-2 gap-4 p-4 bg-muted/50 rounded-md">
              <div>
                <p className="text-sm font-medium text-foreground mb-1">Source</p>
                <p className="text-sm text-muted-foreground">
                  Database: {selectedMappingDetails?.mapping_rules?.[0]?.mapping_rule_metadata?.source_database_name || sourceInfo.databaseId}
                </p>
                <p className="text-sm text-muted-foreground">
                  Table: {sourceInfo.tableName}
                </p>
              </div>
              <div>
                <p className="text-sm font-medium text-foreground mb-1">Target</p>
                <p className="text-sm text-muted-foreground">
                  Database: {selectedMappingDetails?.mapping_rules?.[0]?.mapping_rule_metadata?.target_database_name || targetInfo.databaseId}
                </p>
                <p className="text-sm text-muted-foreground">
                  Table: {targetInfo.tableName}
                </p>
              </div>
            </div>
          )}

          {/* Relationship Type */}
          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Relationship Type <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={relationshipType}
              disabled
              className="w-full px-3 py-2 bg-muted border border-input rounded-md text-muted-foreground"
            />
            <p className="mt-1 text-sm text-muted-foreground">
              Currently only &apos;replication&apos; is supported
            </p>
          </div>

          {/* Relationship Name */}
          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Relationship Name <span className="text-muted-foreground text-xs">(optional - auto-generated if empty)</span>
            </label>
            <input
              type="text"
              value={relationshipName}
              onChange={(e) => {
                setRelationshipName(e.target.value);
                setIsManualName(e.target.value.length > 0);
              }}
              placeholder="Auto-generated based on source and target"
              className="w-full px-3 py-2 bg-background border border-input rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
              disabled={isSubmitting}
            />
            {isManualName ? (
              <p className="mt-1 text-sm text-muted-foreground">
                Using custom name. Clear the field to use auto-generated name.
              </p>
            ) : relationshipName ? (
              <p className="mt-1 text-sm text-muted-foreground">
                Auto-generated: <span className="font-mono">{relationshipName}</span>
              </p>
            ) : null}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end space-x-3 pt-4 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-foreground bg-background border border-input rounded-md hover:bg-accent transition-colors"
              disabled={isSubmitting}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting || !selectedMapping || !sourceInfo || !targetInfo}
              className="px-4 py-2 text-sm font-medium text-primary-foreground bg-primary rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center"
            >
              {isSubmitting && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              {isSubmitting ? 'Creating...' : 'Create Relationship'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

