'use client';

import { useState, useEffect } from 'react';
import { useInstance } from '@/lib/hooks/useInstances';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { formatDatabaseType } from '@/lib/formatters';
import { DatabaseIcon } from '@/components/databases/DatabaseIcon';
import { ConnectLogicalDatabaseDialog } from '@/components/instances/ConnectLogicalDatabaseDialog';
import {
  ArrowLeft,
  Server,
  RefreshCw,
  Settings,
  Trash2,
  Database,
  MapPin,
  Shield,
  Key,
  Activity,
  HardDrive,
} from 'lucide-react';
import Link from 'next/link';
import type { LogicalDatabase } from '@/lib/api/types';

interface InstanceDetailPageProps {
  params: Promise<{
    workspaceId: string;
    instanceName: string;
  }>;
}

export default function InstanceDetailPage({ params }: InstanceDetailPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [instanceName, setInstanceName] = useState<string>('');
  const [visibleDatabasesCount, setVisibleDatabasesCount] = useState<number>(10);
  const [showConnectDialog, setShowConnectDialog] = useState(false);
  const [selectedLogicalDatabase, setSelectedLogicalDatabase] = useState<LogicalDatabase | null>(null);

  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wsId, instanceName: instName }) => {
      setWorkspaceId(wsId);
      setInstanceName(instName);
    });
  }, [params]);

  const { instance, isLoading, error, refetch } = useInstance(workspaceId, instanceName);

  if (!workspaceId || !instanceName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <div className="flex items-center">
          <Link
            href={`/workspaces/${workspaceId}/instances`}
            className="inline-flex items-center text-muted-foreground hover:text-foreground"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Instances
          </Link>
        </div>
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Instance</h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <button
            onClick={() => refetch()}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (isLoading || !instance) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'healthy':
      case 'connected':
        return 'text-green-600 dark:text-green-400';
      case 'warning':
        return 'text-amber-600 dark:text-amber-400';
      case 'error':
      case 'unhealthy':
      case 'disconnected':
        return 'text-red-600 dark:text-red-400';
      case 'unknown':
        return 'text-gray-600 dark:text-gray-400';
      default:
        return 'text-gray-600 dark:text-gray-400';
    }
  };

  const getStatusBg = (status: string) => {
    switch (status.toLowerCase()) {
      case 'healthy':
      case 'connected':
        return 'bg-green-100 dark:bg-green-900/30';
      case 'warning':
        return 'bg-amber-100 dark:bg-amber-900/30';
      case 'error':
      case 'unhealthy':
      case 'disconnected':
        return 'bg-red-100 dark:bg-red-900/30';
      case 'unknown':
        return 'bg-gray-100 dark:bg-gray-900/30';
      default:
        return 'bg-gray-100 dark:bg-gray-900/30';
    }
  };

  const formatDate = (dateString: string | undefined) => {
    if (!dateString) return 'N/A';
    try {
      return new Date(dateString).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });
    } catch {
      return dateString;
    }
  };

  const formatUptime = (seconds: number | undefined) => {
    if (!seconds) return 'N/A';
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${days}d ${hours}h ${minutes}m`;
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const extractVersion = (versionString: string | undefined) => {
    if (!versionString) return undefined;
    
    // Try to extract version number from verbose strings
    // Pattern: Match version numbers like "16.3", "8.0.32", "15.0.2000.5"
    // First, remove common database name prefixes
    const cleaned = versionString
      .replace(/^(PostgreSQL|MySQL|MariaDB|MongoDB|Redis|SQL Server|Oracle)\s+/i, '');
    
    // Extract the version number (numbers and dots) before any other text
    const versionMatch = cleaned.match(/^(\d+\.\d+(?:\.\d+)?(?:\.\d+)?)/);
    if (versionMatch) {
      return versionMatch[1];
    }
    
    // Fallback: try to find any version-like pattern in the string
    const anyVersionMatch = versionString.match(/(\d+\.\d+(?:\.\d+)?(?:\.\d+)?)/);
    if (anyVersionMatch) {
      return anyVersionMatch[1];
    }
    
    // If no pattern found, return the original (truncated if too long)
    return versionString.length > 20 ? versionString.substring(0, 20) + '...' : versionString;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between bg-gradient-to-r from-blue-50/50 to-transparent dark:from-blue-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/instances`}
            className="p-2 hover:bg-accent rounded-md transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-900 rounded-xl flex items-center justify-center border border-border shadow-sm">
              <DatabaseIcon type={instance.instance_type} className="h-7 w-7" />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-foreground">{instance.instance_name}</h2>
              <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
                <span className="font-medium">
                  {formatDatabaseType(instance.instance_type)}
                </span>
                {instance.instance_description && (
                  <>
                    <span>•</span>
                    <span className="truncate max-w-md">{instance.instance_description}</span>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={() => refetch()}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>
          <button
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
          >
            <Settings className="h-4 w-4" />
          </button>
          <button
            className="inline-flex items-center px-3 py-2 border border-destructive text-destructive rounded-md hover:bg-destructive/10 transition-colors"
          >
            <Trash2 className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Status Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Status</p>
              <p className={`text-2xl font-bold mt-1 ${getStatusColor(instance.status)}`}>
                {instance.status.charAt(0).toUpperCase() + instance.status.slice(1)}
              </p>
              {instance.instance_status_message && (
                <p className="text-sm text-muted-foreground mt-1">{instance.instance_status_message}</p>
              )}
            </div>
            <div className={`w-12 h-12 rounded-lg ${getStatusBg(instance.status)} flex items-center justify-center ${getStatusColor(instance.status)}`}>
              <Activity className="h-6 w-6" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Type</p>
              <p className="text-2xl font-bold text-foreground mt-1">{formatDatabaseType(instance.instance_type)}</p>
              {instance.instance_version && extractVersion(instance.instance_version) && (
                <p className="text-sm text-muted-foreground mt-1">v{extractVersion(instance.instance_version)}</p>
              )}
            </div>
            <div className="w-12 h-12 rounded-lg bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center text-blue-600 dark:text-blue-400">
              <Server className="h-6 w-6" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Total Databases</p>
              <p className="text-2xl font-bold text-foreground mt-1">
                {instance.instance_metadata?.total_databases || 0}
              </p>
              <p className="text-sm text-muted-foreground mt-1">
                {instance.connected_databases?.length || 0} Connected, {instance.instance_metadata?.logical_databases?.length || 0} Logical
              </p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center text-purple-600 dark:text-purple-400">
              <Database className="h-6 w-6" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Connections</p>
              <p className="text-2xl font-bold text-foreground mt-1">
                {instance.instance_metadata?.total_connections || 0}
              </p>
              <p className="text-sm text-muted-foreground mt-1">
                of {instance.instance_metadata?.max_connections || 0} max
              </p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-cyan-100 dark:bg-cyan-900/30 flex items-center justify-center text-cyan-600 dark:text-cyan-400">
              <Activity className="h-6 w-6" />
            </div>
          </div>
        </div>
      </div>

      {/* Two Column Layout: Logical Databases + Metrics/Config */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Left Column: Logical Databases */}
        {instance.instance_metadata?.logical_databases && instance.instance_metadata.logical_databases.length > 0 && (
          <div className="bg-card border border-border rounded-lg p-6">
            <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
              <Database className="h-5 w-5 mr-2" />
              Logical Databases ({instance.instance_metadata.logical_databases.length})
            </h3>
            <div className="space-y-1.5">
              {instance.instance_metadata.logical_databases.slice(0, visibleDatabasesCount).map((db, idx) => {
                // Find if this logical database is connected
                const connectedDb = instance.connected_databases?.find(
                  (conn) => conn.database_db_name === db.name
                );
                const isConnected = !!connectedDb;
                const isOffline = connectedDb?.status === 'offline' || connectedDb?.status === 'disconnected';

                return (
                  <div
                    key={idx}
                    className={`flex items-center justify-between p-2 rounded-md transition-colors border ${
                      isConnected
                        ? isOffline
                          ? 'bg-amber-50/50 dark:bg-amber-900/10 border-amber-200 dark:border-amber-800/50'
                          : 'bg-green-50/50 dark:bg-green-900/10 border-green-200 dark:border-green-800/50'
                        : 'bg-muted/30 hover:bg-muted/50 border-border/50'
                    }`}
                  >
                    <div className="flex-1 min-w-0 pr-2">
                      <div className="flex items-center gap-2 mb-1">
                        <Database className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
                        <p className="font-medium text-sm text-foreground truncate">
                          {db.name}
                          {isConnected && connectedDb && (
                            <span className="text-xs text-muted-foreground font-normal ml-1.5">
                              connected as {connectedDb.database_name}
                            </span>
                          )}
                        </p>
                        {isConnected && (
                          <span
                            className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                              isOffline
                                ? 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-400'
                                : 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                            }`}
                          >
                            {isOffline ? 'Offline' : 'Connected'}
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-3 text-xs text-muted-foreground">
                        <span className="truncate">Owner: {db.owner}</span>
                        <span className="whitespace-nowrap">Size: {formatBytes(db.size_bytes)}</span>
                        <span className="truncate">{db.encoding}</span>
                      </div>
                    </div>
                    {isConnected && connectedDb ? (
                      <Link
                        href={`/workspaces/${workspaceId}/databases/${connectedDb.database_name}/schema`}
                        className="text-xs text-primary hover:text-primary/80 font-medium flex-shrink-0 whitespace-nowrap transition-colors"
                      >
                        Open &gt;
                      </Link>
                    ) : (
                      <button
                        className="text-xs text-primary hover:text-primary/80 font-medium flex-shrink-0 transition-colors cursor-pointer"
                        onClick={() => {
                          setSelectedLogicalDatabase(db);
                          setShowConnectDialog(true);
                        }}
                      >
                        Connect
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
            {instance.instance_metadata.logical_databases.length > visibleDatabasesCount && (
              <div className="mt-3 text-center">
                <button
                  onClick={() => setVisibleDatabasesCount(prev => prev + 10)}
                  className="text-sm text-primary hover:text-primary/80 font-medium flex items-center justify-center gap-1 mx-auto"
                >
                  <span>Load More ({instance.instance_metadata.logical_databases.length - visibleDatabasesCount} remaining)</span>
                  <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
              </div>
            )}
          </div>
        )}

        {/* Right Column: Performance Metrics and Server Configuration */}
        <div className="space-y-6">
          {/* Performance Metrics */}
          <div className="bg-card border border-border rounded-lg p-6">
            <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
              <Activity className="h-5 w-5 mr-2" />
              Performance Metrics
            </h3>
            <div className="grid grid-cols-2 gap-4">
              {/* Uptime */}
              <div>
                <p className="text-sm text-muted-foreground">Uptime</p>
                <p className="text-lg font-semibold text-foreground mt-1">
                  {formatUptime(instance.instance_metadata?.uptime_seconds)}
                </p>
              </div>
              {/* Replica Status */}
              <div>
                <p className="text-sm text-muted-foreground">Replication</p>
                <p className="text-lg font-semibold text-foreground mt-1">
                  {instance.instance_metadata?.replication_enabled ? 'Enabled' : 'Disabled'}
                </p>
              </div>
              {/* Platform */}
              <div>
                <p className="text-sm text-muted-foreground">Platform</p>
                <p className="text-lg font-semibold text-foreground mt-1">
                  {instance.instance_metadata?.platform || 'N/A'} / {instance.instance_metadata?.architecture || 'N/A'}
                </p>
              </div>
              {/* Edition */}
              <div>
                <p className="text-sm text-muted-foreground">Edition</p>
                <p className="text-lg font-semibold text-foreground mt-1">
                  {instance.instance_metadata?.edition || 'N/A'}
                </p>
              </div>
            </div>
          </div>

          {/* Server Configuration */}
          {instance.instance_metadata?.server_settings && Object.keys(instance.instance_metadata.server_settings).length > 0 && (
            <div className="bg-card border border-border rounded-lg p-6">
              <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
                <Settings className="h-5 w-5 mr-2" />
                Server Configuration
              </h3>
              <div className="grid grid-cols-1 gap-3">
                {Object.entries(instance.instance_metadata.server_settings).map(([key, value]) => (
                  <div key={key} className="flex justify-between items-center p-2 bg-muted/20 rounded">
                    <span className="text-sm text-muted-foreground">{key}</span>
                    <span className="text-sm font-mono text-foreground">{value}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Details Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Connection Details */}
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
            <MapPin className="h-5 w-5 mr-2" />
            Connection Details
          </h3>
          <div className="space-y-3">
            <div>
              <p className="text-sm text-muted-foreground">Host</p>
              <p className="text-sm font-mono text-foreground mt-1">{instance.instance_host}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Port</p>
              <p className="text-sm font-mono text-foreground mt-1">{instance.instance_port}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Username</p>
              <p className="text-sm font-mono text-foreground mt-1">{instance.instance_username || 'Not specified'}</p>
            </div>
            {instance.instance_system_db_name && (
              <div>
                <p className="text-sm text-muted-foreground">System Database</p>
                <p className="text-sm font-mono text-foreground mt-1">{instance.instance_system_db_name}</p>
              </div>
            )}
          </div>
        </div>

        {/* Security Details */}
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
            <Shield className="h-5 w-5 mr-2" />
            Security
          </h3>
          <div className="space-y-3">
            <div>
              <p className="text-sm text-muted-foreground">SSL Enabled (Instance Config)</p>
              <p className="text-sm text-foreground mt-1">
                {instance.instance_ssl ? (
                  <span className="inline-flex items-center text-green-600 dark:text-green-400">
                    <span className="w-2 h-2 bg-green-500 rounded-full mr-2" />
                    Yes
                  </span>
                ) : (
                  <span className="inline-flex items-center text-red-600 dark:text-red-400">
                    <span className="w-2 h-2 bg-red-500 rounded-full mr-2" />
                    No
                  </span>
                )}
              </p>
            </div>
            {instance.instance_metadata?.ssl_enabled !== undefined && (
              <div>
                <p className="text-sm text-muted-foreground">SSL Enabled (Server)</p>
                <p className="text-sm text-foreground mt-1">
                  {instance.instance_metadata.ssl_enabled ? (
                    <span className="inline-flex items-center text-green-600 dark:text-green-400">
                      <span className="w-2 h-2 bg-green-500 rounded-full mr-2" />
                      Yes
                    </span>
                  ) : (
                    <span className="inline-flex items-center text-red-600 dark:text-red-400">
                      <span className="w-2 h-2 bg-red-500 rounded-full mr-2" />
                      No
                    </span>
                  )}
                </p>
              </div>
            )}
            {instance.instance_ssl && instance.instance_ssl_mode && (
              <div>
                <p className="text-sm text-muted-foreground">SSL Mode</p>
                <p className="text-sm font-mono text-foreground mt-1">{instance.instance_ssl_mode}</p>
              </div>
            )}
            {instance.owner_id && (
              <div>
                <p className="text-sm text-muted-foreground">Owner ID</p>
                <p className="text-sm font-mono text-foreground mt-1 break-all">{instance.owner_id}</p>
              </div>
            )}
          </div>
        </div>

        {/* Extensions & Features */}
        {((instance.instance_metadata?.extensions && instance.instance_metadata.extensions.length > 0) ||
          (instance.instance_metadata?.features && instance.instance_metadata.features.length > 0)) && (
          <div className="bg-card border border-border rounded-lg p-6">
            <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
              <HardDrive className="h-5 w-5 mr-2" />
              Extensions & Features
            </h3>
            <div className="space-y-4">
              {instance.instance_metadata.extensions && instance.instance_metadata.extensions.length > 0 && (
                <div>
                  <p className="text-sm text-muted-foreground mb-2">Extensions</p>
                  <div className="flex flex-wrap gap-2">
                    {instance.instance_metadata.extensions.map((ext, idx) => (
                      <span
                        key={idx}
                        className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400"
                      >
                        {ext}
                      </span>
                    ))}
                  </div>
                </div>
              )}
              {instance.instance_metadata.features && instance.instance_metadata.features.length > 0 && (
                <div>
                  <p className="text-sm text-muted-foreground mb-2">Features</p>
                  <div className="flex flex-wrap gap-2">
                    {instance.instance_metadata.features.map((feature, idx) => (
                      <span
                        key={idx}
                        className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400"
                      >
                        {feature}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* System Information */}
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
            <Key className="h-5 w-5 mr-2" />
            System Information
          </h3>
          <div className="space-y-3">
            <div>
              <p className="text-sm text-muted-foreground">Instance ID</p>
              <p className="text-sm font-mono text-foreground mt-1 break-all">{instance.instance_id}</p>
            </div>
            {instance.environment_id && (
              <div>
                <p className="text-sm text-muted-foreground">Environment ID</p>
                <p className="text-sm font-mono text-foreground mt-1 break-all">{instance.environment_id}</p>
              </div>
            )}
            {instance.connected_to_node_id && (
              <div>
                <p className="text-sm text-muted-foreground">Connected Node ID</p>
                <p className="text-sm font-mono text-foreground mt-1 break-all">{instance.connected_to_node_id}</p>
              </div>
            )}
            <div>
              <p className="text-sm text-muted-foreground">Created</p>
              <p className="text-sm text-foreground mt-1">{formatDate(instance.created)}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Last Updated</p>
              <p className="text-sm text-foreground mt-1">{formatDate(instance.updated)}</p>
            </div>
          </div>
        </div>
      </div>

      {/* Connect Logical Database Dialog */}
      {showConnectDialog && selectedLogicalDatabase && (
        <ConnectLogicalDatabaseDialog
          workspaceId={workspaceId}
          instance={instance}
          logicalDatabase={selectedLogicalDatabase}
          onClose={() => {
            setShowConnectDialog(false);
            setSelectedLogicalDatabase(null);
          }}
          onSuccess={() => {
            setShowConnectDialog(false);
            setSelectedLogicalDatabase(null);
            refetch(); // Refresh the instance data to update connected databases
          }}
        />
      )}
    </div>
  );
}

