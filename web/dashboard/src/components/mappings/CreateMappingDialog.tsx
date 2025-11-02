'use client';

import { useState } from 'react';
import { X, AlertCircle } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { CreateDatabaseMappingRequest } from '@/lib/api/types';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';

interface CreateMappingDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function CreateMappingDialog({ workspaceId, onClose, onSuccess }: CreateMappingDialogProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mappingType, setMappingType] = useState<'database' | 'table'>('table');
  
  // Form state for database mapping
  const [formData, setFormData] = useState<CreateDatabaseMappingRequest>({
    mapping_name: '',
    mapping_description: '',
    mapping_source_database_name: '',
    mapping_target_database_name: '',
    policy_id: '',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsLoading(true);

    try {
      if (mappingType === 'database') {
        await api.mappings.createDatabaseMapping(workspaceId, formData);
        onSuccess();
      } else {
        // TODO: Implement table mapping creation
        setError('Table mapping creation not yet implemented');
        setIsLoading(false);
      }
    } catch (err) {
      console.error('Failed to create mapping:', err);
      setError(err instanceof Error ? err.message : 'Failed to create mapping');
      setIsLoading(false);
    }
  };

  const handleChange = (field: keyof CreateDatabaseMappingRequest, value: string) => {
    setFormData(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <h2 className="text-2xl font-bold text-foreground">Create Mapping</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
            disabled={isLoading}
          >
            <X className="h-6 w-6" />
          </button>
        </div>

        {/* Mapping Type Toggle */}
        <div className="p-6 border-b border-border">
          <div className="flex items-center space-x-4">
            <button
              type="button"
              onClick={() => setMappingType('database')}
              className={`px-4 py-2 rounded-md transition-colors ${
                mappingType === 'database'
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-muted-foreground hover:bg-muted/80'
              }`}
            >
              Database Mapping
            </button>
            <button
              type="button"
              onClick={() => setMappingType('table')}
              className={`px-4 py-2 rounded-md transition-colors ${
                mappingType === 'table'
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-muted text-muted-foreground hover:bg-muted/80'
              }`}
            >
              Table Mapping
            </button>
          </div>
          <p className="text-sm text-muted-foreground mt-2">
            {mappingType === 'database' 
              ? 'Map an entire database to another database'
              : 'Map specific tables and columns between databases'
            }
          </p>
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

            {mappingType === 'database' ? (
              <>
                {/* Database Mapping Form */}
                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Mapping Name <span className="text-destructive">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.mapping_name}
                    onChange={(e) => handleChange('mapping_name', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="my-database-mapping"
                    required
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Description
                  </label>
                  <textarea
                    value={formData.mapping_description}
                    onChange={(e) => handleChange('mapping_description', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="Describe what this mapping does"
                    rows={3}
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Source Database <span className="text-destructive">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.mapping_source_database_name}
                    onChange={(e) => handleChange('mapping_source_database_name', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="source-database"
                    required
                  />
                  <p className="text-sm text-muted-foreground mt-1">
                    The database to map from
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Target Database <span className="text-destructive">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.mapping_target_database_name}
                    onChange={(e) => handleChange('mapping_target_database_name', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="target-database"
                    required
                  />
                  <p className="text-sm text-muted-foreground mt-1">
                    The database to map to
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Policy ID (Optional)
                  </label>
                  <input
                    type="text"
                    value={formData.policy_id}
                    onChange={(e) => handleChange('policy_id', e.target.value)}
                    className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
                    placeholder="policy-id"
                  />
                </div>
              </>
            ) : (
              <div className="bg-muted/50 border border-border rounded-lg p-8 text-center">
                <p className="text-muted-foreground">
                  Table mapping creation will be implemented in a future update.
                  <br />
                  Please use database mapping for now.
                </p>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end space-x-3 p-6 border-t border-border bg-muted/50">
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
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
              disabled={isLoading || mappingType === 'table'}
            >
              {isLoading ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Creating...
                </>
              ) : (
                'Create Mapping'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

