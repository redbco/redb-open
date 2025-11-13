'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { X, AlertCircle } from 'lucide-react';
import { api } from '@/lib/api/endpoints';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { MappingCreationWizard, MappingCreationData } from './MappingCreationWizard';

interface CreateMappingDialogProps {
  workspaceId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export function CreateMappingDialog({ workspaceId, onClose, onSuccess }: CreateMappingDialogProps) {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mappingData, setMappingData] = useState<MappingCreationData | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!mappingData) {
      setError('Please complete all required fields');
      return;
    }

    setError(null);
    setIsLoading(true);

    try {
      // Create the mapping using the new unified endpoint
      const request = {
        mapping_name: mappingData.mappingName,
        mapping_description: mappingData.mappingDescription,
        scope: mappingData.source.type === 'database' ? 'database' : 'table',
        source: mappingData.source.uri,
        target: mappingData.target.uri,
        generate_rules: mappingData.generateRules,
      };

      const response = await api.mappings.create(workspaceId, request);
      
      // Navigate to the newly created mapping detail page
      router.push(`/workspaces/${workspaceId}/mappings/${encodeURIComponent(response.mapping.mapping_name)}`);
      
      onSuccess();
    } catch (err) {
      console.error('Failed to create mapping:', err);
      setError(err instanceof Error ? err.message : 'Failed to create mapping');
    } finally {
      setIsLoading(false);
    }
  };

  const canSubmit = mappingData !== null && !isLoading;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-background/80 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-lg max-w-6xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border sticky top-0 bg-card z-10">
          <div>
            <h2 className="text-2xl font-bold text-foreground">Create Mapping</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Map data between resources with flexible source and target selection
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

            {/* Wizard Component */}
            <MappingCreationWizard
              workspaceId={workspaceId}
              onMappingDataChange={setMappingData}
            />
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between p-6 border-t border-border bg-muted/50 sticky bottom-0">
            <div className="text-sm text-muted-foreground">
              {!canSubmit && 'Complete all required fields to create the mapping'}
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
                ) : (
                  'Create Mapping'
                )}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}

