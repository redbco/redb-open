'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useMappingRules } from '@/lib/hooks/useMappingRules';
import { useTransformations } from '@/lib/hooks/useTransformations';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { ArrowLeft, Plus, RefreshCw, Activity } from 'lucide-react';
import { MappingRulesTable } from '@/components/mappings/MappingRulesTable';
import { AddMappingRuleDialog } from '@/components/mappings/AddMappingRuleDialog';
import { api } from '@/lib/api/endpoints';

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
  const { showToast } = useToast();
  
  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wsId, mappingName: mName }) => {
      setWorkspaceId(wsId);
      setMappingName(decodeURIComponent(mName));
    });
  }, [params]);

  const { mappingRules, isLoading: rulesLoading, error: rulesError, refetch: refetchRules } = useMappingRules(workspaceId, mappingName);
  const { transformations, isLoading: transformationsLoading } = useTransformations();

  const handleRefresh = () => {
    refetchRules();
    showToast({
      type: 'info',
      title: 'Refreshing mapping rules...',
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
      refetchRules();
    } catch (error) {
      console.error('Error deleting rule:', error);
      showToast({
        type: 'error',
        title: 'Failed to delete rule',
      });
    }
  };

  if (!workspaceId || !mappingName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

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
              Mapping rules define how data is transformed from source to target
            </p>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={handleRefresh}
              className="inline-flex items-center px-4 py-2 bg-background border border-border text-foreground rounded-md hover:bg-accent transition-colors"
              disabled={rulesLoading}
            >
              <RefreshCw className={`h-4 w-4 mr-2 ${rulesLoading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button
              onClick={() => setShowAddRuleDialog(true)}
              className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
              disabled={transformationsLoading}
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Rule
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

      {/* Stats */}
      {!rulesLoading && mappingRules.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center space-x-6">
            <div>
              <p className="text-sm text-muted-foreground">Total Rules</p>
              <p className="text-2xl font-bold text-foreground">{mappingRules.length}</p>
            </div>
            <div className="border-l border-border pl-6">
              <p className="text-sm text-muted-foreground">Transformations Used</p>
              <p className="text-2xl font-bold text-foreground">
                {new Set(mappingRules.map(r => r.mapping_rule_transformation_name).filter(Boolean)).size}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Rules List */}
      <div>
        <h3 className="text-xl font-semibold text-foreground mb-4 flex items-center">
          <Activity className="h-5 w-5 mr-2" />
          Mapping Rules
        </h3>
        <MappingRulesTable
          rules={mappingRules}
          isLoading={rulesLoading}
          onDelete={handleDeleteRule}
        />
      </div>

      {/* Add Rule Dialog */}
      {showAddRuleDialog && !transformationsLoading && (
        <AddMappingRuleDialog
          workspaceId={workspaceId}
          mappingName={mappingName}
          transformations={transformations}
          onClose={() => setShowAddRuleDialog(false)}
          onSuccess={refetchRules}
        />
      )}
    </div>
  );
}

