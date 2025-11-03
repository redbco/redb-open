'use client';

import { useState } from 'react';
import Link from 'next/link';
import { Database as DatabaseType } from '@/lib/api/types';
import { Database, Server, Activity, Settings, Trash2, Power, Table } from 'lucide-react';

interface DatabaseCardProps {
  database: DatabaseType;
  workspaceId: string;
  onUpdate: () => void;
}

export function DatabaseCard({ database, workspaceId, onUpdate }: DatabaseCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'healthy':
      case 'connected':
        return 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400';
      case 'warning':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400';
      case 'error':
      case 'disconnected':
        return 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-400';
    }
  };

  const getVendorIcon = (vendor: string) => {
    // In the future, we can add specific icons for different vendors
    return Database;
  };

  const VendorIcon = getVendorIcon(database.database_vendor);

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 bg-primary/10 rounded-lg flex items-center justify-center">
            <VendorIcon className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h3 className="font-semibold text-foreground">{database.database_name}</h3>
            <p className="text-sm text-muted-foreground">{database.database_vendor}</p>
          </div>
        </div>
        <div className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(database.status)}`}>
          {database.status}
        </div>
      </div>
      <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
        {database.database_description || <span className="text-muted-foreground italic">No description</span>}
      </p>
      <div className="space-y-3">
        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground flex items-center">
            <Server className="h-4 w-4 mr-1" />
            Instance
          </span>
          <span className="font-medium text-foreground">{database.instance_name}</span>
        </div>
        
        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground flex items-center">
            <Database className="h-4 w-4 mr-1" />
            DB Name
          </span>
          <span className="font-medium text-foreground font-mono text-xs">{database.database_db_name}</span>
        </div>

        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground flex items-center">
            <Activity className="h-4 w-4 mr-1" />
            Status
          </span>
          <span className="font-medium text-foreground">{database.database_enabled ? 'Enabled' : 'Disabled'}</span>
        </div>

        {database.instance_host && (
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Host</span>
            <span className="font-medium text-foreground font-mono text-xs">
              {database.instance_host}:{database.instance_port}
            </span>
          </div>
        )}
      </div>

      <div className="mt-4 pt-4 border-t border-border flex items-center justify-between">
        <div className="flex items-center gap-2">
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="text-sm text-primary hover:text-primary/80 font-medium"
          >
            {isExpanded ? 'Hide Details' : 'View Details'}
          </button>
          <Link
            href={`/workspaces/${workspaceId}/databases/${database.database_name}/schema`}
            className="inline-flex items-center gap-1 text-sm px-3 py-1.5 bg-primary/10 text-primary hover:bg-primary/20 rounded-md transition-colors font-medium"
            title="View Database Schema"
          >
            <Table className="h-3.5 w-3.5" />
            View Schema
          </Link>
        </div>
        <div className="flex items-center space-x-2">
          <button
            className="p-2 rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            title="Settings"
          >
            <Settings className="h-4 w-4" />
          </button>
          <button
            className="p-2 rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            title={database.database_enabled ? 'Disable' : 'Enable'}
          >
            <Power className="h-4 w-4" />
          </button>
          <button
            className="p-2 rounded-md hover:bg-red-100 dark:hover:bg-red-900/20 hover:text-red-600 dark:hover:text-red-400 transition-colors"
            title="Disconnect"
          >
            <Trash2 className="h-4 w-4" />
          </button>
        </div>
      </div>

      {isExpanded && (
        <div className="mt-4 pt-4 border-t border-border space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-muted-foreground">Database ID:</span>
            <span className="font-mono text-xs text-foreground">{database.database_id}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Instance ID:</span>
            <span className="font-mono text-xs text-foreground">{database.instance_id}</span>
          </div>
          {database.database_version && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">Version:</span>
              <span className="font-medium text-foreground">{database.database_version}</span>
            </div>
          )}
          {database.database_username && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">Username:</span>
              <span className="font-mono text-xs text-foreground">{database.database_username}</span>
            </div>
          )}
          {database.instance_ssl && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">SSL:</span>
              <span className="font-medium text-foreground">{database.instance_ssl_mode || 'Enabled'}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

