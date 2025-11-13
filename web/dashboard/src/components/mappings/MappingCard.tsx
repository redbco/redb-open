'use client';

import { useState } from 'react';
import { Mapping } from '@/lib/api/types';
import {
  ArrowRightLeft,
  Eye,
  MoreVertical,
  Trash2,
  CheckCircle,
  AlertCircle,
  Database,
  Table,
  Activity,
  Link as Link2
} from 'lucide-react';
import Link from 'next/link';
import { api } from '@/lib/api/endpoints';
import { useToast } from '@/components/ui/Toast';

interface MappingCardProps {
  mapping: Mapping;
  workspaceId: string;
  onUpdate: () => void;
}

export function MappingCard({ mapping, workspaceId, onUpdate }: MappingCardProps) {
  const [showMenu, setShowMenu] = useState(false);
  const { showToast } = useToast();

  const handleDelete = async () => {
    if (!confirm(`Are you sure you want to delete mapping "${mapping.mapping_name}"? This cannot be undone.`)) {
      return;
    }

    try {
      await api.mappings.delete(workspaceId, mapping.mapping_name);
      showToast({
        type: 'success',
        title: 'Mapping deleted successfully',
      });
      onUpdate(); // Refresh the list
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      showToast({
        type: 'error',
        title: 'Failed to delete mapping',
        message: error.message || 'An error occurred while deleting the mapping.',
      });
    }
  };

  const getMappingTypeIcon = (type?: string) => {
    switch (type?.toLowerCase()) {
      case 'table':
        return Table;
      case 'database':
        return Database;
      default:
        return ArrowRightLeft;
    }
  };

  const getValidationColor = (validated?: boolean) => {
    if (validated === undefined) return 'text-gray-600 dark:text-gray-400';
    return validated ? 'text-green-600 dark:text-green-400' : 'text-yellow-600 dark:text-yellow-400';
  };

  const getValidationBg = (validated?: boolean) => {
    if (validated === undefined) return 'bg-gray-100 dark:bg-gray-900/30';
    return validated ? 'bg-green-100 dark:bg-green-900/30' : 'bg-yellow-100 dark:bg-yellow-900/30';
  };

  const getRelationshipStatusColor = (status?: string) => {
    switch (status?.toLowerCase()) {
      case 'running':
        return 'text-green-600 dark:text-green-400';
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
      case 'running':
        return 'bg-green-100 dark:bg-green-900/30';
      case 'pending':
        return 'bg-yellow-100 dark:bg-yellow-900/30';
      case 'stopped':
        return 'bg-gray-100 dark:bg-gray-900/30';
      case 'error':
        return 'bg-red-100 dark:bg-red-900/30';
      default:
        return 'bg-gray-100 dark:bg-gray-900/30';
    }
  };

  const formatSourceTarget = (dbName?: string, tableName?: string, fallbackUri?: string) => {
    if (dbName && tableName) {
      return `${dbName}.${tableName}`;
    }
    return fallbackUri || 'N/A';
  };

  const TypeIcon = getMappingTypeIcon(mapping.mapping_type);

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200 hover:shadow-lg">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 bg-blue-100 dark:bg-blue-900/30 rounded-lg flex items-center justify-center">
            <TypeIcon className="h-5 w-5 text-blue-600 dark:text-blue-400" />
          </div>
          <div>
            <Link
              href={`/workspaces/${workspaceId}/mappings/${encodeURIComponent(mapping.mapping_name)}`}
              className="text-lg font-semibold text-foreground hover:text-primary transition-colors"
            >
              {mapping.mapping_name}
            </Link>
            <p className="text-sm text-muted-foreground capitalize">
              {mapping.mapping_type || 'table'} mapping
            </p>
          </div>
        </div>
        
        <div className="relative">
          <button
            onClick={() => setShowMenu(!showMenu)}
            className="p-1 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground"
          >
            <MoreVertical className="h-5 w-5" />
          </button>
          
          {showMenu && (
            <>
              <div
                className="fixed inset-0 z-10"
                onClick={() => setShowMenu(false)}
              />
              <div className="absolute right-0 mt-2 w-48 bg-popover border border-border rounded-md shadow-lg z-20 py-1">
                <Link
                  href={`/workspaces/${workspaceId}/mappings/${encodeURIComponent(mapping.mapping_name)}`}
                  className="flex items-center px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                  onClick={() => setShowMenu(false)}
                >
                  <Eye className="h-4 w-4 mr-2" />
                  View Details
                </Link>
                <div className="border-t border-border my-1" />
                <button
                  className="flex items-center w-full px-4 py-2 text-sm text-destructive hover:bg-accent"
                  onClick={() => {
                    setShowMenu(false);
                    handleDelete();
                  }}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  Delete
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Description */}
      {mapping.mapping_description && (
        <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
          {mapping.mapping_description}
        </p>
      )}

      {/* Source and Target */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <span className="text-muted-foreground w-16">Source:</span>
          <span className="text-foreground font-mono text-xs">
            {formatSourceTarget(mapping.source_database_name, mapping.source_table_name, mapping.mapping_source)}
          </span>
        </div>
        <div className="flex items-center text-sm">
          <span className="text-muted-foreground w-16">Target:</span>
          <span className="text-foreground font-mono text-xs">
            {formatSourceTarget(mapping.target_database_name, mapping.target_table_name, mapping.mapping_target)}
          </span>
        </div>
      </div>

      {/* Mapping Rules Count and Validation */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center text-sm text-muted-foreground">
          <Activity className="h-4 w-4 mr-2" />
          <span>{mapping.mapping_rule_count || 0} {mapping.mapping_rule_count === 1 ? 'rule' : 'rules'}</span>
        </div>
        <div className="flex items-center space-x-2">
          {mapping.validated !== undefined && (
            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getValidationBg(mapping.validated)}`}>
              {mapping.validated ? (
                <>
                  <CheckCircle className={`h-3 w-3 mr-1 ${getValidationColor(mapping.validated)}`} />
                  <span className={getValidationColor(mapping.validated)}>Validated</span>
                </>
              ) : (
                <>
                  <AlertCircle className={`h-3 w-3 mr-1 ${getValidationColor(mapping.validated)}`} />
                  <span className={getValidationColor(mapping.validated)}>Not Validated</span>
                </>
              )}
            </span>
          )}
        </div>
      </div>

      {/* Relationships Status and Validation Errors */}
      <div className="mt-4 pt-4 border-t border-border space-y-2">
        {/* Relationships */}
        {mapping.relationship_infos && mapping.relationship_infos.length > 0 ? (
          <>
            {mapping.relationship_infos.map((rel, idx) => (
              <div key={idx} className="flex items-center justify-between">
                <div className="flex items-center gap-2 flex-1 min-w-0">
                  <Link2 className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                  <span className="text-xs text-muted-foreground">Relationship:</span>
                  <span className="font-mono text-xs text-foreground truncate">
                    {rel.relationship_name}
                  </span>
                </div>
                <div className="flex-shrink-0 ml-2">
                  <span className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium ${getRelationshipStatusBg(rel.status)}`}>
                    <span className={`inline-flex items-center ${getRelationshipStatusColor(rel.status)}`}>
                      {rel.status?.toLowerCase() === 'running' && (
                        <span className="inline-flex h-2 w-2 rounded-full bg-green-600 dark:bg-green-400 animate-pulse mr-1"></span>
                      )}
                      <span className="capitalize">{rel.status}</span>
                    </span>
                  </span>
                </div>
              </div>
            ))}
          </>
        ) : (
          <div className="flex items-center gap-2">
            <Link2 className="h-4 w-4 text-muted-foreground flex-shrink-0" />
            <span className="text-xs text-muted-foreground">Relationship:</span>
            <span className="text-xs text-muted-foreground italic">None</span>
          </div>
        )}
        
        {/* Validation Errors */}
        {mapping.validation_errors && mapping.validation_errors.length > 0 && (
          <div className="text-xs text-destructive">
            {mapping.validation_errors.length} validation error{mapping.validation_errors.length !== 1 ? 's' : ''}
          </div>
        )}
      </div>
    </div>
  );
}

