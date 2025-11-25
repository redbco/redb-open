'use client';

import { useState } from 'react';
import Link from 'next/link';
import { Database as DatabaseType } from '@/lib/api/types';
import { Server, Activity, Unplug, Calendar, Shield, ExternalLink, ChevronDown, ChevronUp } from 'lucide-react';
import { DisconnectDatabaseDialog } from './DisconnectDatabaseDialog';
import { DatabaseIcon } from './DatabaseIcon';
import { formatDatabaseType } from '@/lib/formatters';

interface DatabaseCardProps {
  database: DatabaseType;
  workspaceId: string;
  onUpdate: () => void;
}

export function DatabaseCard({ database, workspaceId, onUpdate }: DatabaseCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [showDisconnectDialog, setShowDisconnectDialog] = useState(false);

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'healthy':
      case 'connected':
        return 'bg-emerald-500';
      case 'warning':
        return 'bg-amber-500';
      case 'error':
      case 'disconnected':
        return 'bg-rose-500';
      default:
        return 'bg-slate-500';
    }
  };

  const formatDate = (dateString: string) => {
    try {
      return new Date(dateString).toLocaleDateString(undefined, {
        year: 'numeric',
        month: 'short',
        day: 'numeric'
      });
    } catch (e) {
      return dateString;
    }
  };

  return (
    <div className="group bg-card border border-border rounded-xl p-5 hover:shadow-lg hover:border-primary/20 transition-all duration-200 flex flex-col h-full relative">
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-4">
          <div className="w-12 h-12 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
            <DatabaseIcon type={database.database_type} className="h-7 w-7" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <Link
                href={`/workspaces/${workspaceId}/databases/${database.database_name}/schema`}
                className="font-bold text-lg text-foreground hover:text-primary hover:underline transition-colors flex items-center gap-1"
                title="View Database Schema"
              >
                {database.database_name}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover:opacity-50 transition-opacity" />
              </Link>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
              <span className="font-medium truncate max-w-[200px]">
                {formatDatabaseType(database.database_type)}
              </span>
              <span>•</span>
              <Link
                href={`/workspaces/${workspaceId}/instances/${database.instance_name}`}
                className="truncate max-w-[120px] hover:underline hover:text-primary transition-colors"
                title={`View Instance: ${database.instance_name}`}
              >
                {database.instance_name}
              </Link>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <div className="relative flex h-2.5 w-2.5">
            <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${getStatusColor(database.status)}`}></span>
            <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${getStatusColor(database.status)}`}></span>
          </div>
          <span className="text-xs font-medium text-muted-foreground capitalize">{database.status}</span>
        </div>
      </div>

      <div className="flex-grow">
        <p className="text-sm text-muted-foreground mb-5 line-clamp-2 min-h-[2.5rem]">
          {database.database_description || <span className="text-muted-foreground/60 italic">No description provided</span>}
        </p>

        <div className="grid grid-cols-2 gap-y-3 gap-x-4 mb-4">
          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Server className="h-3.5 w-3.5" />
              Host
            </span>
            <span className="text-sm font-medium text-foreground font-mono truncate" title={`${database.instance_host}:${database.instance_port}`}>
              {database.instance_host}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Activity className="h-3.5 w-3.5" />
              Port
            </span>
            <span className="text-sm font-medium text-foreground font-mono">
              {database.instance_port}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Shield className="h-3.5 w-3.5" />
              SSL
            </span>
            <span className="text-sm font-medium text-foreground">
              {database.instance_ssl ? (database.instance_ssl_mode || 'Enabled') : 'Disabled'}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Calendar className="h-3.5 w-3.5" />
              Created
            </span>
            <span className="text-sm font-medium text-foreground">
              {database.created ? formatDate(database.created) : '-'}
            </span>
          </div>
        </div>
      </div>

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
        <button
          onClick={() => setShowDisconnectDialog(true)}
          className="text-muted-foreground hover:text-red-600 dark:hover:text-red-400 p-1.5 rounded-md hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
          title="Disconnect Database"
        >
          <Unplug className="h-4 w-4" />
        </button>
      </div>

      {isExpanded && (
        <div className="mt-3 pt-3 border-t border-border/50 space-y-2.5 text-xs animate-in slide-in-from-top-2 duration-200">
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Database ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={database.database_id}>{database.database_id}</span>
          </div>
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Instance ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={database.instance_id}>{database.instance_id}</span>
          </div>
          {database.database_version && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Version:</span>
              <span className="text-foreground truncate" title={database.database_version}>{database.database_version}</span>
            </div>
          )}
          {database.database_username && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Username:</span>
              <span className="font-mono text-foreground truncate">{database.database_username}</span>
            </div>
          )}
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">DB Name:</span>
            <span className="font-mono text-foreground truncate">{database.database_db_name}</span>
          </div>
        </div>
      )}

      {/* Disconnect Dialog */}
      {showDisconnectDialog && (
        <DisconnectDatabaseDialog
          database={database}
          workspaceName={workspaceId}
          onClose={() => setShowDisconnectDialog(false)}
          onSuccess={onUpdate}
        />
      )}
    </div>
  );
}

