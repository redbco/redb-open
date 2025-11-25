'use client';

import { useState } from 'react';
import { Instance } from '@/lib/api/types';
import {
  Server,
  Eye,
  MoreVertical,
  Power,
  PowerOff,
  RefreshCw,
  Settings,
  Trash2,
  Database,
  Calendar,
  MapPin,
  Shield,
  ExternalLink,
  ChevronDown,
  ChevronUp,
  Unplug
} from 'lucide-react';
import Link from 'next/link';
import { DatabaseIcon } from '../databases/DatabaseIcon';
import { formatVendor, formatDatabaseType } from '@/lib/formatters';

interface InstanceCardProps {
  instance: Instance;
  workspaceId: string;
  onUpdate: () => void;
}

export function InstanceCard({ instance, workspaceId, onUpdate }: InstanceCardProps) {
  const [showMenu, setShowMenu] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'healthy':
      case 'connected':
        return 'bg-emerald-500';
      case 'warning':
        return 'bg-amber-500';
      case 'error':
      case 'disconnected':
      case 'unhealthy':
        return 'bg-rose-500';
      default:
        return 'bg-slate-500';
    }
  };

  const formatDate = (dateString: string | undefined) => {
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

  return (
    <div className="group bg-card border border-border rounded-xl p-5 hover:shadow-lg hover:border-primary/20 transition-all duration-200 flex flex-col h-full relative">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-4">
          <div className="w-12 h-12 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
            <DatabaseIcon type={instance.instance_type} className="h-7 w-7" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <Link
                href={`/workspaces/${workspaceId}/instances/${instance.instance_name}`}
                className="font-bold text-lg text-foreground hover:text-primary hover:underline transition-colors flex items-center gap-1"
                title="View Instance Details"
              >
                {instance.instance_name}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover:opacity-50 transition-opacity" />
              </Link>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
              <span className="font-medium truncate max-w-[200px]" title={`${formatVendor(instance.instance_vendor)} - ${formatDatabaseType(instance.instance_type)}`}>
                {formatVendor(instance.instance_vendor)} • {formatDatabaseType(instance.instance_type)}
              </span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <div className="relative flex h-2.5 w-2.5">
              <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${getStatusColor(instance.status)}`}></span>
              <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${getStatusColor(instance.status)}`}></span>
            </div>
            <span className="text-xs font-medium text-muted-foreground capitalize">{instance.status || 'Unknown'}</span>
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
                    href={`/workspaces/${workspaceId}/instances/${instance.instance_name}`}
                    className="flex items-center px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                    onClick={() => setShowMenu(false)}
                  >
                    <Eye className="h-4 w-4 mr-2" />
                    View Details
                  </Link>
                  <button
                    className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                    onClick={() => {
                      setShowMenu(false);
                      // TODO: Implement reconnect
                    }}
                  >
                    <RefreshCw className="h-4 w-4 mr-2" />
                    Reconnect
                  </button>
                  <button
                    className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                    onClick={() => {
                      setShowMenu(false);
                      // TODO: Implement modify
                    }}
                  >
                    <Settings className="h-4 w-4 mr-2" />
                    Modify
                  </button>
                  <div className="border-t border-border my-1" />
                  {instance.instance_enabled ? (
                    <button
                      className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                      onClick={() => {
                        setShowMenu(false);
                        // TODO: Implement disable
                      }}
                    >
                      <PowerOff className="h-4 w-4 mr-2" />
                      Disable
                    </button>
                  ) : (
                    <button
                      className="flex items-center w-full px-4 py-2 text-sm text-popover-foreground hover:bg-accent"
                      onClick={() => {
                        setShowMenu(false);
                        // TODO: Implement enable
                      }}
                    >
                      <Power className="h-4 w-4 mr-2" />
                      Enable
                    </button>
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      <div className="flex-grow">
        <p className="text-sm text-muted-foreground mb-5 line-clamp-2 min-h-[2.5rem]">
          {instance.instance_description || <span className="text-muted-foreground/60 italic">No description provided</span>}
        </p>

        <div className="grid grid-cols-2 gap-y-3 gap-x-4 mb-4">
          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <MapPin className="h-3.5 w-3.5" />
              Host
            </span>
            <span className="text-sm font-medium text-foreground font-mono truncate" title={`${instance.instance_host}:${instance.instance_port}`}>
              {instance.instance_host}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Server className="h-3.5 w-3.5" />
              Port
            </span>
            <span className="text-sm font-medium text-foreground font-mono">
              {instance.instance_port}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Shield className="h-3.5 w-3.5" />
              SSL
            </span>
            <span className="text-sm font-medium text-foreground">
              {instance.instance_ssl ? (instance.instance_ssl_mode || 'Enabled') : 'Disabled'}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Calendar className="h-3.5 w-3.5" />
              Created
            </span>
            <span className="text-sm font-medium text-foreground">
              {formatDate(instance.created)}
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

        <div className="flex items-center gap-2">
          {instance.database_count !== undefined && (
            <div className="flex items-center text-xs text-muted-foreground bg-muted/50 px-2 py-1 rounded">
              <Database className="h-3 w-3 mr-1.5" />
              <span>{instance.database_count} DBs</span>
            </div>
          )}
          <button
            className="text-muted-foreground hover:text-red-600 dark:hover:text-red-400 p-1.5 rounded-md hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
            title="Disconnect Instance"
            onClick={() => {
              // TODO: Implement disconnect
            }}
          >
            <Unplug className="h-4 w-4" />
          </button>
        </div>
      </div>

      {isExpanded && (
        <div className="mt-3 pt-3 border-t border-border/50 space-y-2.5 text-xs animate-in slide-in-from-top-2 duration-200">
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Instance ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={instance.instance_id}>{instance.instance_id}</span>
          </div>
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Unique ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={instance.instance_unique_identifier}>{instance.instance_unique_identifier}</span>
          </div>
          {instance.instance_version && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Version:</span>
              <span className="text-foreground truncate" title={instance.instance_version}>{instance.instance_version}</span>
            </div>
          )}
          {instance.instance_username && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Username:</span>
              <span className="font-mono text-foreground truncate">{instance.instance_username}</span>
            </div>
          )}
          {instance.instance_system_db_name && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">System DB:</span>
              <span className="font-mono text-foreground truncate">{instance.instance_system_db_name}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

