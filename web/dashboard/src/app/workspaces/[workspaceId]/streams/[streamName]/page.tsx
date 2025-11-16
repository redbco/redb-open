'use client';

import { useState, useEffect } from 'react';
import { useStream, useStreamTopics, useReconnectStream } from '@/lib/hooks/useStreams';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { ArrowLeft, RefreshCw, Power, Edit } from 'lucide-react';
import Link from 'next/link';
import { DisconnectStreamDialog } from '@/components/streams/DisconnectStreamDialog';
import { StatusBadge } from '@/components/ui/StatusBadge';

interface StreamDetailsPageProps {
  params: Promise<{
    workspaceId: string;
    streamName: string;
  }>;
}

export default function StreamDetailsPage({ params }: StreamDetailsPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [streamName, setStreamName] = useState<string>('');
  const [showDisconnectDialog, setShowDisconnectDialog] = useState(false);
  const { showToast } = useToast();

  useEffect(() => {
    params.then(({ workspaceId: id, streamName: name }) => {
      setWorkspaceId(id);
      setStreamName(decodeURIComponent(name));
    });
  }, [params]);

  const { stream, isLoading, error, refetch } = useStream(workspaceId, streamName);
  const { topics, isLoading: topicsLoading, refetch: refetchTopics } = useStreamTopics(workspaceId, streamName);
  const { reconnect, isLoading: reconnecting } = useReconnectStream(workspaceId, streamName);

  const handleReconnect = async () => {
    try {
      await reconnect();
      showToast({
        type: 'success',
        title: 'Reconnecting',
        message: 'Stream is reconnecting...',
      });
      setTimeout(() => refetch(), 1000);
    } catch (error) {
      showToast({
        type: 'error',
        title: 'Reconnection Failed',
        message: error instanceof Error ? error.message : 'Failed to reconnect stream',
      });
    }
  };

  const getStatusColor = (status: string) => {
    const statusLower = status?.toLowerCase() || '';
    if (statusLower.includes('connected') || statusLower.includes('online')) {
      return 'success';
    } else if (statusLower.includes('pending') || statusLower.includes('connecting')) {
      return 'warning';
    } else {
      return 'error';
    }
  };

  if (!workspaceId || !streamName) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Stream</h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <Link
            href={`/workspaces/${workspaceId}/streams`}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Streams
          </Link>
        </div>
      </div>
    );
  }

  if (isLoading || !stream) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link
            href={`/workspaces/${workspaceId}/streams`}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h2 className="text-3xl font-bold text-foreground">{stream.stream_name}</h2>
            <p className="text-muted-foreground mt-1">{stream.stream_description || 'No description'}</p>
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <StatusBadge status={getStatusColor(stream.status)} />
          <button
            onClick={handleReconnect}
            disabled={reconnecting}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${reconnecting ? 'animate-spin' : ''}`} />
            Reconnect
          </button>
          <button
            onClick={() => setShowDisconnectDialog(true)}
            className="inline-flex items-center px-3 py-2 border border-destructive bg-background text-destructive rounded-md hover:bg-destructive hover:text-destructive-foreground transition-colors"
          >
            <Power className="h-4 w-4 mr-2" />
            Disconnect
          </button>
        </div>
      </div>

      {/* Stream Details */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4">Stream Information</h3>
          <dl className="space-y-3">
            <div className="flex justify-between">
              <dt className="text-sm text-muted-foreground">Platform</dt>
              <dd className="text-sm font-medium text-foreground capitalize">{stream.stream_platform}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-sm text-muted-foreground">Status</dt>
              <dd className="text-sm font-medium text-foreground">{stream.status}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-sm text-muted-foreground">Node ID</dt>
              <dd className="text-sm font-medium text-foreground">{stream.connected_to_node_id}</dd>
            </div>
            <div className="flex justify-between">
              <dt className="text-sm text-muted-foreground">Stream ID</dt>
              <dd className="text-sm font-mono text-foreground text-xs">{stream.stream_id}</dd>
            </div>
            {stream.created && (
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Created</dt>
                <dd className="text-sm font-medium text-foreground">{new Date(stream.created).toLocaleString()}</dd>
              </div>
            )}
          </dl>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4">Connection Configuration</h3>
          <dl className="space-y-3">
            {stream.connection_config && Object.entries(stream.connection_config).map(([key, value]) => {
              // Mask sensitive fields
              const isSensitive = key.toLowerCase().includes('password') ||
                key.toLowerCase().includes('secret') ||
                key.toLowerCase().includes('key');

              return (
                <div key={key} className="flex justify-between">
                  <dt className="text-sm text-muted-foreground">{key}</dt>
                  <dd className="text-sm font-medium text-foreground truncate max-w-[200px]">
                    {isSensitive ? '********' : Array.isArray(value) ? value.join(', ') : String(value)}
                  </dd>
                </div>
              );
            })}
          </dl>
        </div>
      </div>

      {/* Monitored Topics */}
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-foreground">Monitored Topics ({stream.monitored_topics?.length || 0})</h3>
          <button
            onClick={refetchTopics}
            className="text-sm text-primary hover:text-primary/80 transition-colors"
          >
            Refresh
          </button>
        </div>
        {stream.monitored_topics && stream.monitored_topics.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {stream.monitored_topics.map((topic) => (
              <span
                key={topic}
                className="px-3 py-1 bg-primary/10 text-primary rounded-full text-sm"
              >
                {topic}
              </span>
            ))}
          </div>
        ) : (
          <p className="text-muted-foreground">No topics configured</p>
        )}
      </div>

      {/* Available Topics */}
      {topics && topics.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4">Available Topics</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-sm font-medium text-muted-foreground">Name</th>
                  <th className="text-left py-3 px-4 text-sm font-medium text-muted-foreground">Partitions</th>
                  <th className="text-left py-3 px-4 text-sm font-medium text-muted-foreground">Replicas</th>
                  <th className="text-right py-3 px-4 text-sm font-medium text-muted-foreground">Actions</th>
                </tr>
              </thead>
              <tbody>
                {topics.map((topic) => (
                  <tr key={topic.name} className="border-b border-border hover:bg-accent/50">
                    <td className="py-3 px-4 text-sm font-medium text-foreground">{topic.name}</td>
                    <td className="py-3 px-4 text-sm text-muted-foreground">{topic.partitions}</td>
                    <td className="py-3 px-4 text-sm text-muted-foreground">{topic.replicas}</td>
                    <td className="py-3 px-4 text-sm text-right">
                      <Link
                        href={`/workspaces/${workspaceId}/streams/${streamName}/topics/${topic.name}`}
                        className="text-primary hover:text-primary/80 transition-colors"
                      >
                        View Schema
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {showDisconnectDialog && (
        <DisconnectStreamDialog
          workspaceName={workspaceId}
          streamName={streamName}
          onClose={() => setShowDisconnectDialog(false)}
          onSuccess={() => {
            showToast({
              type: 'success',
              title: 'Stream Disconnected',
              message: 'Redirecting...',
            });
            window.location.href = `/${workspaceId}/streams`;
          }}
        />
      )}
    </div>
  );
}

