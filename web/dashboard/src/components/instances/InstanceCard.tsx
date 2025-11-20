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
} from 'lucide-react';
import Link from 'next/link';

interface InstanceCardProps {
  instance: Instance;
  workspaceId: string;
  onUpdate: () => void;
}

export function InstanceCard({ instance, workspaceId, onUpdate }: InstanceCardProps) {
  const [showMenu, setShowMenu] = useState(false);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'healthy':
        return 'bg-green-500';
      case 'unhealthy':
        return 'bg-red-500';
      case 'unknown':
        return 'bg-gray-500';
      default:
        return 'bg-yellow-500';
    }
  };

  const getTypeIcon = (type: string) => {
    switch (type.toLowerCase()) {
      case 'postgresql':
      case 'postgres':
        return '🐘';
      case 'mysql':
      case 'mariadb':
        return '🐬';
      case 'mongodb':
        return '🍃';
      case 'redis':
        return '🔴';
      case 'mssql':
      case 'sqlserver':
        return '🗄️';
      case 'oracle':
        return '🔶';
      default:
        return '💾';
    }
  };

  const formatDate = (dateString: string | undefined) => {
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

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:border-primary/50 transition-all duration-200 hover:shadow-lg">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="text-3xl">{getTypeIcon(instance.instance_type)}</div>
          <div>
            <h3 className="text-lg font-semibold text-foreground flex items-center">
              {instance.instance_name}
              <span className={`ml-2 w-2 h-2 rounded-full ${getStatusColor(instance.status)}`} />
            </h3>
            <p className="text-sm text-muted-foreground">{instance.instance_type}</p>
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
                <button
                  className="flex items-center w-full px-4 py-2 text-sm text-destructive hover:bg-accent"
                  onClick={() => {
                    setShowMenu(false);
                    // TODO: Implement disconnect
                  }}
                >
                  <Trash2 className="h-4 w-4 mr-2" />
                  Disconnect
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Description */}
      {instance.instance_description && (
        <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
          {instance.instance_description}
        </p>
      )}

      {/* Connection Info */}
      <div className="space-y-2 mb-4">
        <div className="flex items-center text-sm">
          <MapPin className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-foreground font-mono">
            {instance.instance_host}:{instance.instance_port}
          </span>
        </div>
        {instance.instance_version && (
          <div className="flex items-center text-sm">
            <Server className="h-4 w-4 mr-2 text-muted-foreground flex-shrink-0" />
            <span className="text-muted-foreground truncate">
              Version {instance.instance_version}
            </span>
          </div>
        )}
        <div className="flex items-center text-sm">
          <Calendar className="h-4 w-4 mr-2 text-muted-foreground" />
          <span className="text-muted-foreground">
            Added {formatDate(instance.created)}
          </span>
        </div>
      </div>

      {/* Database Count */}
      {instance.database_count !== undefined && (
        <div className="flex items-center justify-between pt-4 border-t border-border">
          <div className="flex items-center text-sm text-muted-foreground">
            <Database className="h-4 w-4 mr-2" />
            <span>{instance.database_count} {instance.database_count === 1 ? 'database' : 'databases'}</span>
          </div>
          <Link
            href={`/workspaces/${workspaceId}/instances/${instance.instance_name}`}
            className="text-sm text-primary hover:text-primary/80 font-medium"
          >
            View Details →
          </Link>
        </div>
      )}

      {/* Status Badge */}
      <div className="mt-4 pt-4 border-t border-border">
        <div className="flex items-center justify-between">
          <span
            className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
              instance.instance_enabled
                ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                : 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400'
            }`}
          >
            {instance.instance_enabled ? 'Enabled' : 'Disabled'}
          </span>
          {instance.instance_ssl && (
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400">
              SSL
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

