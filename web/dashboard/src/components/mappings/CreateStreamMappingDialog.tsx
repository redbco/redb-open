'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { X, AlertCircle, Database, Waves, ArrowRight, Filter as FilterIcon } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { apiClient } from '@/lib/api/client';
import { CreateMappingResponse } from '@/lib/api/types';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';

interface CreateStreamMappingDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
  defaultMappingType?: 'stream-to-table' | 'table-to-stream' | 'stream-to-stream';
}

type MappingType = 'stream-to-table' | 'table-to-stream' | 'stream-to-stream';

interface StreamResource {
  integrationName: string;
  topicName: string;
}

interface TableResource {
  databaseName: string;
  tableName: string;
}

interface StreamFilter {
  filterType: string;
  filterExpression: Record<string, any>;
  filterOrder: number;
  filterOperator: string;
}

export function CreateStreamMappingDialog({ 
  workspaceId, 
  onClose, 
  onSuccess,
  defaultMappingType = 'stream-to-table'
}: CreateStreamMappingDialogProps) {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // Form state
  const [mappingType, setMappingType] = useState<MappingType>(defaultMappingType);
  const [mappingName, setMappingName] = useState('');
  const [mappingDescription, setMappingDescription] = useState('');
  
  // Stream-to-table state
  const [sourceIntegration, setSourceIntegration] = useState('');
  const [sourceTopic, setSourceTopic] = useState('');
  const [targetDatabase, setTargetDatabase] = useState('');
  const [targetTable, setTargetTable] = useState('');
  
  // Table-to-stream state
  const [sourceDatabase, setSourceDatabase] = useState('');
  const [sourceTable, setSourceTable] = useState('');
  const [targetIntegration, setTargetIntegration] = useState('');
  const [targetTopic, setTargetTopic] = useState('');
  
  // Stream-to-stream state
  const [sourceIntegration2, setSourceIntegration2] = useState('');
  const [sourceTopic2, setSourceTopic2] = useState('');
  const [targetIntegration2, setTargetIntegration2] = useState('');
  const [targetTopic2, setTargetTopic2] = useState('');
  
  // Filters
  const [filters, setFilters] = useState<StreamFilter[]>([]);
  const [showFilterForm, setShowFilterForm] = useState(false);

  // Available resources (would be fetched from API)
  const [availableIntegrations, setAvailableIntegrations] = useState<string[]>([]);
  const [availableDatabases, setAvailableDatabases] = useState<string[]>([]);
  const [availableTopics, setAvailableTopics] = useState<Record<string, string[]>>({});
  const [availableTables, setAvailableTables] = useState<Record<string, string[]>>({});

  // Load available resources
  useEffect(() => {
    const loadResources = async () => {
      try {
        // Load stream integrations
        const streamsResponse = await api.streams.list(workspaceId);
        // API returns stream_platform and stream_name, not integration_name and topic_name
        const integrations = streamsResponse.streams.map((s: any) => s.stream_platform || s.integration_name);
        setAvailableIntegrations([...new Set(integrations)]);
        
        // Build topics map
        const topicsMap: Record<string, string[]> = {};
        streamsResponse.streams.forEach((s: any) => {
          const integration = s.stream_platform || s.integration_name;
          const topicName = s.stream_name || s.topic_name;
          if (!topicsMap[integration]) {
            topicsMap[integration] = [];
          }
          if (topicName) {
            topicsMap[integration].push(topicName);
          }
        });
        setAvailableTopics(topicsMap);

        // Load databases
        const dbResponse = await api.databases.list(workspaceId);
        const dbNames = dbResponse.databases.map((db: any) => db.database_name);
        setAvailableDatabases(dbNames);
        
        // Load tables for each database (simplified - would need actual implementation)
        const tablesMap: Record<string, string[]> = {};
        for (const db of dbResponse.databases) {
          if (db.database_name) {
            try {
              const schemaResponse = await api.databases.getSchema(workspaceId, db.database_name);
              tablesMap[db.database_name] = schemaResponse.schema?.tables?.map((t: any) => t.name) || [];
            } catch (err) {
              console.error(`Failed to load tables for ${db.database_name}:`, err);
              tablesMap[db.database_name] = [];
            }
          }
        }
        setAvailableTables(tablesMap);
      } catch (err) {
        console.error('Failed to load resources:', err);
      }
    };

    loadResources();
  }, [workspaceId]);

  // Auto-generate mapping name and description
  useEffect(() => {
    if (mappingType === 'stream-to-table' && sourceIntegration && sourceTopic && targetDatabase && targetTable) {
      if (!mappingName) {
        setMappingName(`${sourceIntegration}-${sourceTopic}-to-${targetDatabase}-${targetTable}`);
      }
      if (!mappingDescription) {
        setMappingDescription(`Stream-to-table mapping from ${sourceIntegration}/${sourceTopic} to ${targetDatabase}.${targetTable}`);
      }
    } else if (mappingType === 'table-to-stream' && sourceDatabase && sourceTable && targetIntegration && targetTopic) {
      if (!mappingName) {
        setMappingName(`${sourceDatabase}-${sourceTable}-to-${targetIntegration}-${targetTopic}`);
      }
      if (!mappingDescription) {
        setMappingDescription(`Table-to-stream mapping from ${sourceDatabase}.${sourceTable} to ${targetIntegration}/${targetTopic}`);
      }
    } else if (mappingType === 'stream-to-stream' && sourceIntegration2 && sourceTopic2 && targetIntegration2 && targetTopic2) {
      if (!mappingName) {
        setMappingName(`${sourceIntegration2}-${sourceTopic2}-to-${targetIntegration2}-${targetTopic2}`);
      }
      if (!mappingDescription) {
        setMappingDescription(`Stream-to-stream mapping from ${sourceIntegration2}/${sourceTopic2} to ${targetIntegration2}/${targetTopic2}`);
      }
    }
  }, [mappingType, sourceIntegration, sourceTopic, targetDatabase, targetTable, sourceDatabase, sourceTable, targetIntegration, targetTopic, sourceIntegration2, sourceTopic2, targetIntegration2, targetTopic2]);

  const handleAddFilter = () => {
    setFilters([...filters, {
      filterType: 'time_window',
      filterExpression: { window_type: 'sliding', duration: '5m' },
      filterOrder: filters.length + 1,
      filterOperator: 'and'
    }]);
    setShowFilterForm(false);
  };

  const handleRemoveFilter = (index: number) => {
    setFilters(filters.filter((_, i) => i !== index));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    setError(null);
    setIsLoading(true);

    try {
      let endpoint = '';
      let request: any = {
        mapping_name: mappingName,
        mapping_description: mappingDescription,
        filters: filters.length > 0 ? filters : undefined
      };

      if (mappingType === 'stream-to-table') {
        endpoint = `/workspaces/${workspaceId}/mappings/stream-to-table`;
        request = {
          ...request,
          source_integration_name: sourceIntegration,
          source_topic_name: sourceTopic,
          target_database_name: targetDatabase,
          target_table_name: targetTable
        };
      } else if (mappingType === 'table-to-stream') {
        endpoint = `/workspaces/${workspaceId}/mappings/table-to-stream`;
        request = {
          ...request,
          source_database_name: sourceDatabase,
          source_table_name: sourceTable,
          target_integration_name: targetIntegration,
          target_topic_name: targetTopic
        };
      } else if (mappingType === 'stream-to-stream') {
        endpoint = `/workspaces/${workspaceId}/mappings/stream-to-stream`;
        request = {
          ...request,
          source_integration_name: sourceIntegration2,
          source_topic_name: sourceTopic2,
          target_integration_name: targetIntegration2,
          target_topic_name: targetTopic2
        };
      }

      const response = await apiClient.post<CreateMappingResponse>(endpoint, request);
      
      // Navigate to the newly created mapping
      router.push(`/workspaces/${workspaceId}/mappings/${encodeURIComponent(response.mapping.mapping_name)}`);
      
      onSuccess();
    } catch (err) {
      console.error('Failed to create stream mapping:', err);
      setError(err instanceof Error ? err.message : 'Failed to create stream mapping');
    } finally {
      setIsLoading(false);
    }
  };

  const canSubmit = () => {
    if (!mappingName || !mappingDescription) return false;
    
    if (mappingType === 'stream-to-table') {
      return sourceIntegration && sourceTopic && targetDatabase && targetTable;
    } else if (mappingType === 'table-to-stream') {
      return sourceDatabase && sourceTable && targetIntegration && targetTopic;
    } else if (mappingType === 'stream-to-stream') {
      return sourceIntegration2 && sourceTopic2 && targetIntegration2 && targetTopic2;
    }
    
    return false;
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border sticky top-0 bg-card z-10">
          <div>
            <h2 className="text-2xl font-bold text-foreground">Create Stream Mapping</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Map data streams to databases or other streams
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
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">
                Mapping Type
              </label>
              <div className="grid grid-cols-3 gap-4">
                <button
                  type="button"
                  onClick={() => setMappingType('stream-to-table')}
                  className={`p-4 border rounded-lg flex flex-col items-center gap-2 transition-colors ${
                    mappingType === 'stream-to-table'
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-border hover:border-primary/50'
                  }`}
                >
                  <Waves className="h-6 w-6" />
                  <span className="text-sm font-medium">Stream → Table</span>
                  <span className="text-xs text-muted-foreground text-center">Real-time ingestion</span>
                </button>

                <button
                  type="button"
                  onClick={() => setMappingType('table-to-stream')}
                  className={`p-4 border rounded-lg flex flex-col items-center gap-2 transition-colors ${
                    mappingType === 'table-to-stream'
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-border hover:border-primary/50'
                  }`}
                >
                  <Database className="h-6 w-6" />
                  <span className="text-sm font-medium">Table → Stream</span>
                  <span className="text-xs text-muted-foreground text-center">CDC publishing</span>
                </button>

                <button
                  type="button"
                  onClick={() => setMappingType('stream-to-stream')}
                  className={`p-4 border rounded-lg flex flex-col items-center gap-2 transition-colors ${
                    mappingType === 'stream-to-stream'
                      ? 'border-primary bg-primary/10 text-primary'
                      : 'border-border hover:border-primary/50'
                  }`}
                >
                  <Waves className="h-6 w-6" />
                  <span className="text-sm font-medium">Stream → Stream</span>
                  <span className="text-xs text-muted-foreground text-center">Stream routing</span>
                </button>
              </div>
            </div>

            {/* Stream-to-Table Form */}
            {mappingType === 'stream-to-table' && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  {/* Source */}
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                      <Waves className="h-4 w-4" />
                      Source Stream
                    </h3>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Integration
                      </label>
                      <select
                        value={sourceIntegration}
                        onChange={(e) => setSourceIntegration(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                      >
                        <option value="">Select integration...</option>
                        {availableIntegrations.map((int) => (
                          <option key={int} value={int}>{int}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Topic
                      </label>
                      <select
                        value={sourceTopic}
                        onChange={(e) => setSourceTopic(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                        disabled={!sourceIntegration}
                      >
                        <option value="">Select topic...</option>
                        {sourceIntegration && availableTopics[sourceIntegration]?.map((topic) => (
                          <option key={topic} value={topic}>{topic}</option>
                        ))}
                      </select>
                    </div>
                  </div>

                  {/* Arrow */}
                  <div className="flex items-center justify-center">
                    <ArrowRight className="h-6 w-6 text-muted-foreground" />
                  </div>

                  {/* Target */}
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                      <Database className="h-4 w-4" />
                      Target Table
                    </h3>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Database
                      </label>
                      <select
                        value={targetDatabase}
                        onChange={(e) => setTargetDatabase(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                      >
                        <option value="">Select database...</option>
                        {availableDatabases.map((db) => (
                          <option key={db} value={db}>{db}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Table
                      </label>
                      <select
                        value={targetTable}
                        onChange={(e) => setTargetTable(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                        disabled={!targetDatabase}
                      >
                        <option value="">Select table...</option>
                        {targetDatabase && availableTables[targetDatabase]?.map((table) => (
                          <option key={table} value={table}>{table}</option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Table-to-Stream Form */}
            {mappingType === 'table-to-stream' && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  {/* Source */}
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                      <Database className="h-4 w-4" />
                      Source Table
                    </h3>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Database
                      </label>
                      <select
                        value={sourceDatabase}
                        onChange={(e) => setSourceDatabase(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                      >
                        <option value="">Select database...</option>
                        {availableDatabases.map((db) => (
                          <option key={db} value={db}>{db}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Table
                      </label>
                      <select
                        value={sourceTable}
                        onChange={(e) => setSourceTable(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                        disabled={!sourceDatabase}
                      >
                        <option value="">Select table...</option>
                        {sourceDatabase && availableTables[sourceDatabase]?.map((table) => (
                          <option key={table} value={table}>{table}</option>
                        ))}
                      </select>
                    </div>
                  </div>

                  {/* Arrow */}
                  <div className="flex items-center justify-center">
                    <ArrowRight className="h-6 w-6 text-muted-foreground" />
                  </div>

                  {/* Target */}
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                      <Waves className="h-4 w-4" />
                      Target Stream
                    </h3>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Integration
                      </label>
                      <select
                        value={targetIntegration}
                        onChange={(e) => setTargetIntegration(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                      >
                        <option value="">Select integration...</option>
                        {availableIntegrations.map((int) => (
                          <option key={int} value={int}>{int}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Topic
                      </label>
                      <input
                        type="text"
                        value={targetTopic}
                        onChange={(e) => setTargetTopic(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        placeholder="Enter topic name..."
                        required
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Stream-to-Stream Form */}
            {mappingType === 'stream-to-stream' && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  {/* Source */}
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                      <Waves className="h-4 w-4" />
                      Source Stream
                    </h3>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Integration
                      </label>
                      <select
                        value={sourceIntegration2}
                        onChange={(e) => setSourceIntegration2(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                      >
                        <option value="">Select integration...</option>
                        {availableIntegrations.map((int) => (
                          <option key={int} value={int}>{int}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Topic
                      </label>
                      <select
                        value={sourceTopic2}
                        onChange={(e) => setSourceTopic2(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                        disabled={!sourceIntegration2}
                      >
                        <option value="">Select topic...</option>
                        {sourceIntegration2 && availableTopics[sourceIntegration2]?.map((topic) => (
                          <option key={topic} value={topic}>{topic}</option>
                        ))}
                      </select>
                    </div>
                  </div>

                  {/* Arrow */}
                  <div className="flex items-center justify-center">
                    <ArrowRight className="h-6 w-6 text-muted-foreground" />
                  </div>

                  {/* Target */}
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-foreground flex items-center gap-2">
                      <Waves className="h-4 w-4" />
                      Target Stream
                    </h3>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Integration
                      </label>
                      <select
                        value={targetIntegration2}
                        onChange={(e) => setTargetIntegration2(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        required
                      >
                        <option value="">Select integration...</option>
                        {availableIntegrations.map((int) => (
                          <option key={int} value={int}>{int}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-muted-foreground mb-1">
                        Topic
                      </label>
                      <input
                        type="text"
                        value={targetTopic2}
                        onChange={(e) => setTargetTopic2(e.target.value)}
                        className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                        placeholder="Enter topic name..."
                        required
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Mapping Details */}
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Mapping Name
                </label>
                <input
                  type="text"
                  value={mappingName}
                  onChange={(e) => setMappingName(e.target.value)}
                  className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                  placeholder="Auto-generated if left empty"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Description
                </label>
                <textarea
                  value={mappingDescription}
                  onChange={(e) => setMappingDescription(e.target.value)}
                  className="w-full px-3 py-2 border border-border rounded-lg bg-background text-foreground"
                  rows={3}
                  placeholder="Describe the purpose of this mapping..."
                  required
                />
              </div>
            </div>

            {/* Filters */}
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <label className="text-sm font-medium text-foreground flex items-center gap-2">
                  <FilterIcon className="h-4 w-4" />
                  Stream Filters (Optional)
                </label>
                <button
                  type="button"
                  onClick={handleAddFilter}
                  className="text-sm text-primary hover:text-primary/80 font-medium"
                >
                  + Add Filter
                </button>
              </div>

              {filters.length > 0 && (
                <div className="space-y-2">
                  {filters.map((filter, index) => (
                    <div key={index} className="flex items-center gap-2 p-3 bg-muted rounded-lg">
                      <div className="flex-1">
                        <span className="text-sm font-medium text-foreground">{filter.filterType}</span>
                        <span className="text-xs text-muted-foreground ml-2">
                          {JSON.stringify(filter.filterExpression)}
                        </span>
                      </div>
                      <button
                        type="button"
                        onClick={() => handleRemoveFilter(index)}
                        className="text-destructive hover:text-destructive/80"
                      >
                        <X className="h-4 w-4" />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between p-6 border-t border-border bg-muted/50 sticky bottom-0">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-foreground hover:text-foreground/80 transition-colors"
              disabled={isLoading}
            >
              Cancel
            </button>
            
            <button
              type="submit"
              className="px-6 py-2 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 font-medium disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
              disabled={!canSubmit() || isLoading}
            >
              {isLoading && <LoadingSpinner className="h-4 w-4" />}
              Create Mapping
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

