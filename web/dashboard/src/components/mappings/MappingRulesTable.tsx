'use client';

import { useState } from 'react';
import { MappingRule } from '@/lib/api/types';
import { Activity, ArrowRight, Trash2, Hash } from 'lucide-react';

interface MappingRulesTableProps {
  rules: MappingRule[];
  isLoading: boolean;
  onDelete?: (ruleId: string) => void;
}

export function MappingRulesTable({ rules, isLoading, onDelete }: MappingRulesTableProps) {
  if (isLoading) {
    return (
      <div className="space-y-3">
        {[...Array(3)].map((_, i) => (
          <div key={i} className="bg-card border border-border rounded-lg p-4 animate-pulse">
            <div className="h-5 bg-muted rounded w-1/3 mb-2"></div>
            <div className="h-4 bg-muted rounded w-2/3"></div>
          </div>
        ))}
      </div>
    );
  }

  if (rules.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-12 text-center">
        <Activity className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-xl font-semibold text-foreground mb-2">No Mapping Rules</h3>
        <p className="text-muted-foreground">
          This mapping doesn't have any rules yet. Add rules to define column mappings.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {rules.map((rule) => (
        <div
          key={rule.mapping_rule_id}
          className="bg-card border border-border rounded-lg p-4 hover:border-primary/50 transition-all"
        >
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <div className="flex items-center space-x-2 mb-2">
                <h4 className="text-base font-semibold text-foreground">
                  {rule.mapping_rule_name}
                </h4>
                {rule.mapping_rule_transformation_name && (
                  <span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400">
                    {rule.mapping_rule_transformation_name}
                  </span>
                )}
              </div>
              
              {rule.mapping_rule_description && (
                <p className="text-sm text-muted-foreground mb-3">
                  {rule.mapping_rule_description}
                </p>
              )}

              <div className="flex items-center space-x-2 text-sm">
                <code className="px-2 py-1 bg-muted rounded font-mono text-foreground">
                  {rule.mapping_rule_source}
                </code>
                <ArrowRight className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                <code className="px-2 py-1 bg-muted rounded font-mono text-foreground">
                  {rule.mapping_rule_target}
                </code>
              </div>

              {rule.mapping_rule_metadata && (
                <div className="mt-3 flex flex-wrap gap-2 text-xs text-muted-foreground">
                  {rule.mapping_rule_metadata.match_score !== undefined && (
                    <span className="flex items-center">
                      <Hash className="h-3 w-3 mr-1" />
                      Match: {(rule.mapping_rule_metadata.match_score * 100).toFixed(0)}%
                    </span>
                  )}
                  {rule.mapping_rule_metadata.type_compatible !== undefined && (
                    <span>
                      {rule.mapping_rule_metadata.type_compatible ? '✓' : '✗'} Type Compatible
                    </span>
                  )}
                </div>
              )}
            </div>

            {onDelete && (
              <button
                onClick={() => onDelete(rule.mapping_rule_id)}
                className="p-2 text-muted-foreground hover:text-destructive hover:bg-destructive/10 rounded-md transition-colors"
                title="Delete rule"
              >
                <Trash2 className="h-4 w-4" />
              </button>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

