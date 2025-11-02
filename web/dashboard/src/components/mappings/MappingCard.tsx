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
  Calendar,
  Activity,
} from 'lucide-react';
import Link from 'next/link';

interface MappingCardProps {
  mapping: Mapping;
  workspaceId: string;
  onUpdate: () => void;
}

export function MappingCard({ mapping, workspaceId, onUpdate }: MappingCardProps) {
  const [showMenu, setShowMenu] = useState(false);

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

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    } catch {
      return dateString;
    }
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
            <h3 className="text-lg font-semibold text-foreground">
              {mapping.mapping_name}
            </h3>
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
                    // TODO: Implement delete
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
          <span className="text-foreground font-mono text-xs truncate">
            {mapping.mapping_source || 'N/A'}
          </span>
        </div>
        <div className="flex items-center text-sm">
          <span className="text-muted-foreground w-16">Target:</span>
          <span className="text-foreground font-mono text-xs truncate">
            {mapping.mapping_target || 'N/A'}
          </span>
        </div>
      </div>

      {/* Mapping Rules Count */}
      <div className="flex items-center justify-between pt-4 border-t border-border">
        <div className="flex items-center text-sm text-muted-foreground">
          <Activity className="h-4 w-4 mr-2" />
          <span>{mapping.mapping_rule_count || 0} {mapping.mapping_rule_count === 1 ? 'rule' : 'rules'}</span>
        </div>
        <Link
          href={`/workspaces/${workspaceId}/mappings/${encodeURIComponent(mapping.mapping_name)}`}
          className="text-sm text-primary hover:text-primary/80 font-medium"
        >
          View Mapping →
        </Link>
      </div>

      {/* Status and Validation */}
      <div className="mt-4 pt-4 border-t border-border">
        <div className="flex items-center justify-between">
          <span className="text-xs text-muted-foreground flex items-center">
            <Calendar className="h-3 w-3 mr-1" />
            {formatDate(mapping.created)}
          </span>
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
        {mapping.validation_errors && mapping.validation_errors.length > 0 && (
          <div className="mt-2 text-xs text-destructive">
            {mapping.validation_errors.length} validation error{mapping.validation_errors.length !== 1 ? 's' : ''}
          </div>
        )}
      </div>
    </div>
  );
}

