'use client';

import { useState, useEffect } from 'react';
import { useInstance } from '@/lib/hooks/useInstances';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import {
  ArrowLeft,
  Server,
  RefreshCw,
  Settings,
  Power,
  PowerOff,
  Trash2,
  Database,
  MapPin,
  Calendar,
  Shield,
  Key,
  Activity,
  HardDrive,
} from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

interface InstanceDetailPageProps {
  params: Promise<{
    workspaceId: string;
    instanceName: string;
  }>;
}

export default function InstanceDetailPage({ params }: InstanceDetailPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [instanceName, setInstanceName] = useState<string>('');
  const router = useRouter();
  const { showToast } = useToast();

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
    switch (status) {
      case 'healthy':
        return 'text-green-600 dark:text-green-400';
      case 'unhealthy':
        return 'text-red-600 dark:text-red-400';
      case 'unknown':
        return 'text-gray-600 dark:text-gray-400';
      default:
        return 'text-yellow-600 dark:text-yellow-400';
    }
  };

  const getStatusBg = (status: string) => {
    switch (status) {
      case 'healthy':
        return 'bg-green-100 dark:bg-green-900/30';
      case 'unhealthy':
        return 'bg-red-100 dark:bg-red-900/30';
      case 'unknown':
        return 'bg-gray-100 dark:bg-gray-900/30';
      default:
        return 'bg-yellow-100 dark:bg-yellow-900/30';
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

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <Link
            href={`/workspaces/${workspaceId}/instances`}
            className="inline-flex items-center text-muted-foreground hover:text-foreground mb-4"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Instances
          </Link>
          <h2 className="text-3xl font-bold text-foreground">{instance.instance_name}</h2>
          <p className="text-muted-foreground mt-2">{instance.instance_description || 'No description provided'}</p>
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
            className="inline-flex items-center px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
          >
            <Settings className="h-4 w-4 mr-2" />
            Modify
          </button>
          {instance.instance_enabled ? (
            <button
              className="inline-flex items-center px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              <PowerOff className="h-4 w-4 mr-2" />
              Disable
            </button>
          ) : (
            <button
              className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              <Power className="h-4 w-4 mr-2" />
              Enable
            </button>
          )}
          <button
            className="inline-flex items-center px-4 py-2 border border-destructive text-destructive rounded-md hover:bg-destructive/10 transition-colors"
          >
            <Trash2 className="h-4 w-4 mr-2" />
            Disconnect
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
              <p className="text-2xl font-bold text-foreground mt-1">{instance.instance_type}</p>
              {instance.instance_version && (
                <p className="text-sm text-muted-foreground mt-1">v{instance.instance_version}</p>
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
              <p className="text-sm font-medium text-muted-foreground">Databases</p>
              <p className="text-2xl font-bold text-foreground mt-1">
                {instance.database_count !== undefined ? instance.database_count : '-'}
              </p>
              <p className="text-sm text-muted-foreground mt-1">Connected</p>
            </div>
            <div className="w-12 h-12 rounded-lg bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center text-purple-600 dark:text-purple-400">
              <Database className="h-6 w-6" />
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Enabled</p>
              <p className="text-2xl font-bold text-foreground mt-1">
                {instance.instance_enabled ? 'Yes' : 'No'}
              </p>
              <p className="text-sm text-muted-foreground mt-1">Instance state</p>
            </div>
            <div className={`w-12 h-12 rounded-lg ${instance.instance_enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-900/30 text-gray-600 dark:text-gray-400'} flex items-center justify-center`}>
              <HardDrive className="h-6 w-6" />
            </div>
          </div>
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
            {instance.instance_unique_identifier && (
              <div>
                <p className="text-sm text-muted-foreground">Unique Identifier</p>
                <p className="text-sm font-mono text-foreground mt-1 break-all">{instance.instance_unique_identifier}</p>
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
              <p className="text-sm text-muted-foreground">SSL Enabled</p>
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
            {instance.policy_ids && instance.policy_ids.length > 0 && (
              <div>
                <p className="text-sm text-muted-foreground">Policies</p>
                <div className="flex flex-wrap gap-2 mt-1">
                  {instance.policy_ids.map((policyId, idx) => (
                    <span
                      key={idx}
                      className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400"
                    >
                      {policyId}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

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
            {instance.workspace_id && (
              <div>
                <p className="text-sm text-muted-foreground">Workspace ID</p>
                <p className="text-sm font-mono text-foreground mt-1 break-all">{instance.workspace_id}</p>
              </div>
            )}
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
          </div>
        </div>

        {/* Timestamps */}
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center">
            <Calendar className="h-5 w-5 mr-2" />
            Timestamps
          </h3>
          <div className="space-y-3">
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
    </div>
  );
}

