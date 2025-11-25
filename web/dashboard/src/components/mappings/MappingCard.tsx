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
  Link as Link2,
  ChevronDown,
  ChevronUp,
  ExternalLink,
  Calendar
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
  const [isExpanded, setIsExpanded] = useState(false);
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

  const getStatusColor = (validated?: boolean) => {
    if (validated === undefined) return 'bg-slate-500';
    return validated ? 'bg-emerald-500' : 'bg-amber-500';
  };

  const formatSourceTarget = (dbName?: string, tableName?: string, fallbackUri?: string) => {
    if (dbName && tableName) {
      return `${dbName}.${tableName}`;
    }
    return fallbackUri || 'N/A';
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return '-';
    try {
      return new Date(dateString).toLocaleDateString(undefined, {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
      });
    } catch {
      return dateString;
    }
  };

  const TypeIcon = getMappingTypeIcon(mapping.mapping_type);

  return (
    <div className="group bg-card border border-border rounded-xl p-5 hover:shadow-lg hover:border-primary/20 transition-all duration-200 flex flex-col h-full relative">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-4">
          <div className="w-12 h-12 bg-gradient-to-br from-teal-50 to-teal-100 dark:from-teal-900/20 dark:to-teal-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
            <TypeIcon className="h-7 w-7 text-teal-600 dark:text-teal-400" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <Link
                href={`/workspaces/${workspaceId}/mappings/${encodeURIComponent(mapping.mapping_name)}`}
                className="font-bold text-lg text-foreground hover:text-primary hover:underline transition-colors flex items-center gap-1"
                title="View Mapping Details"
              >
                {mapping.mapping_name}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover:opacity-50 transition-opacity" />
              </Link>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
              <span className="font-medium capitalize truncate max-w-[200px]">
                {mapping.mapping_type || 'table'} mapping
              </span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div className="relative flex h-2.5 w-2.5">
              <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${getStatusColor(mapping.validated)}`}></span>
              <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${getStatusColor(mapping.validated)}`}></span>
            </div>
            <span className="text-xs font-medium text-muted-foreground capitalize">
              {mapping.validated === undefined ? 'Unknown' : mapping.validated ? 'Validated' : 'Not Validated'}
            </span>
          </div>

          <div className="relative">
            <button
              onClick={() => setShowMenu(!showMenu)}
              className="p-1 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"
            >
              <MoreVertical className="h-4 w-4" />
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
      </div>

      <div className="flex-grow">
        {/* Description */}
        <p className="text-sm text-muted-foreground mb-5 line-clamp-2 min-h-[2.5rem]">
          {mapping.mapping_description || <span className="text-muted-foreground/60 italic">No description provided</span>}
        </p>

        {/* Source and Target */}
        <div className="grid grid-cols-2 gap-y-3 gap-x-4 mb-4">
          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Database className="h-3.5 w-3.5" />
              Source
            </span>
            <span className="text-sm font-medium text-foreground font-mono truncate" title={formatSourceTarget(mapping.source_database_name, mapping.source_table_name, mapping.mapping_source)}>
              {formatSourceTarget(mapping.source_database_name, mapping.source_table_name, mapping.mapping_source)}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Database className="h-3.5 w-3.5" />
              Target
            </span>
            <span className="text-sm font-medium text-foreground font-mono truncate" title={formatSourceTarget(mapping.target_database_name, mapping.target_table_name, mapping.mapping_target)}>
              {formatSourceTarget(mapping.target_database_name, mapping.target_table_name, mapping.mapping_target)}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Activity className="h-3.5 w-3.5" />
              Rules
            </span>
            <span className="text-sm font-medium text-foreground font-mono">
              {mapping.mapping_rule_count || 0}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Calendar className="h-3.5 w-3.5" />
              Created
            </span>
            <span className="text-sm font-medium text-foreground">
              {formatDate(mapping.created)}
            </span>
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="mt-auto pt-4 border-t border-border/50 flex items-center justify-between">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-xs text-muted-foreground hover:text-foreground font-medium flex items-center gap-1 transition-colors"
        >
          {isExpanded ? (
            <>
              <ChevronUp className="h-3.5 w-3.5" />
              Hide Details
            </>
          ) : (
            <>
              <ChevronDown className="h-3.5 w-3.5" />
              View Details
            </>
          )}
        </button>

        <div className="flex items-center gap-2">
          {mapping.validated !== undefined && (
            mapping.validated ? (
              <CheckCircle className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
            ) : (
              <AlertCircle className="h-4 w-4 text-amber-600 dark:text-amber-400" />
            )
          )}
        </div>
      </div>

      {/* Expanded Details */}
      {isExpanded && (
        <div className="mt-3 pt-3 border-t border-border/50 space-y-2.5 text-xs animate-in slide-in-from-top-2 duration-200">
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Mapping ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={mapping.mapping_id}>{mapping.mapping_id}</span>
          </div>
          
          {/* Relationships */}
          {mapping.relationship_infos && mapping.relationship_infos.length > 0 ? (
            <>
              <div className="text-muted-foreground font-medium pt-2">Relationships:</div>
              {mapping.relationship_infos.map((rel, idx) => (
                <div key={idx} className="grid grid-cols-[1fr,auto] gap-2 pl-2">
                  <div className="flex items-center gap-2">
                    <Link2 className="h-3 w-3 text-muted-foreground" />
                    <span className="font-mono text-foreground truncate">
                      {rel.relationship_name}
                    </span>
                  </div>
                  <span className={`px-2 py-0.5 rounded text-xs capitalize ${
                    rel.status?.toLowerCase() === 'running' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                      : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
                  }`}>
                    {rel.status}
                  </span>
                </div>
              ))}
            </>
          ) : (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Relationships:</span>
              <span className="text-muted-foreground italic">None</span>
            </div>
          )}
          
          {/* Validation Errors */}
          {mapping.validation_errors && mapping.validation_errors.length > 0 && (
            <div className="pt-2">
              <div className="text-destructive font-medium mb-1">
                Validation Errors ({mapping.validation_errors.length}):
              </div>
              <ul className="list-disc list-inside text-destructive space-y-1 pl-2">
                {mapping.validation_errors.slice(0, 3).map((error, idx) => (
                  <li key={idx} className="truncate">{error}</li>
                ))}
                {mapping.validation_errors.length > 3 && (
                  <li className="text-muted-foreground italic">
                    +{mapping.validation_errors.length - 3} more...
                  </li>
                )}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

