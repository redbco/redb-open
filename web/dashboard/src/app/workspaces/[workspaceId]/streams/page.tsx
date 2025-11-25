'use client';

import { useState, useEffect } from 'react';
import { useStreams, useReconnectStream, useDisconnectStream } from '@/lib/hooks/useStreams';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Plus, RefreshCw, Activity, CheckCircle2, AlertCircle, Waves } from 'lucide-react';
import { StreamCard } from '@/components/streams/StreamCard';
import { ConnectStreamDialog } from '@/components/streams/ConnectStreamDialog';
import { DisconnectStreamDialog } from '@/components/streams/DisconnectStreamDialog';
import { CreateStreamMappingDialog } from '@/components/mappings/CreateStreamMappingDialog';

interface StreamsPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function StreamsPage({ params }: StreamsPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [showConnectDialog, setShowConnectDialog] = useState(false);
  const [showStreamMappingDialog, setShowStreamMappingDialog] = useState(false);
  const [disconnectStreamName, setDisconnectStreamName] = useState<string | null>(null);
  const { showToast } = useToast();

  // Initialize workspace ID from params
  useEffect(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  }, [params]);

  const { streams, isLoading, error, refetch } = useStreams(workspaceId);

  const connectedStreams = streams.filter(s => s.status.toLowerCase().includes('connected') || s.status.toLowerCase().includes('online')).length;
  const disconnectedStreams = streams.length - connectedStreams;

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing streams...',
    });
  };

  const handleReconnect = async (streamName: string) => {
    try {
      showToast({
        type: 'info',
        title: 'Reconnecting...',
        message: `Reconnecting stream ${streamName}`,
      });

      // Trigger refetch to show updated status
      setTimeout(() => {
        refetch();
        showToast({
          type: 'success',
          title: 'Reconnected',
          message: `Stream ${streamName} is reconnecting`,
        });
      }, 1000);
    } catch (error) {
      showToast({
        type: 'error',
        title: 'Reconnection Failed',
        message: error instanceof Error ? error.message : 'Failed to reconnect stream',
      });
    }
  };

  const handleDisconnect = (streamName: string) => {
    setDisconnectStreamName(streamName);
  };

  const handleDisconnectSuccess = () => {
    refetch();
  };

  if (!workspaceId) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold text-foreground">Streams</h2>
            <p className="text-muted-foreground mt-2">
              Manage your stream connections
            </p>
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-8 text-center">
          <div className="text-red-600 dark:text-red-400 mb-4">
            <svg className="h-12 w-12 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Streams</h3>
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

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between bg-gradient-to-r from-orange-50/50 to-transparent dark:from-orange-900/10 p-6 -mx-6 rounded-lg mb-6">
        <div>
          <div className="flex items-center gap-3">
            <Waves className="w-8 h-8 text-orange-600 dark:text-orange-400" />
            <h2 className="text-3xl font-bold text-foreground">Streams</h2>
          </div>
          <div className="flex items-center gap-2 mt-2">
            <p className="text-muted-foreground">
              Manage stream connections to messaging platforms
            </p>
            {!isLoading && streams.length > 0 && (
              <>
                <span className="text-muted-foreground/30">•</span>
                {disconnectedStreams > 0 ? (
                  <div className="flex items-center gap-1.5 text-rose-600 dark:text-rose-400 text-sm font-medium animate-in fade-in duration-300">
                    <AlertCircle className="w-4 h-4" />
                    {disconnectedStreams} disconnected
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5 text-emerald-600 dark:text-emerald-400 text-sm font-medium animate-in fade-in duration-300">
                    <CheckCircle2 className="w-4 h-4" />
                    All streams connected
                  </div>
                )}
              </>
            )}
          </div>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </button>

          <button
            onClick={() => setShowConnectDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Connect Stream
          </button>
        </div>
      </div>



      {/* Streams Grid */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <LoadingSpinner size="lg" />
        </div>
      ) : streams.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Activity className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-lg font-semibold text-foreground mb-2">No Streams Connected</h3>
          <p className="text-muted-foreground mb-6">
            Connect to a messaging platform to start streaming data.
          </p>
          <button
            onClick={() => setShowConnectDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Connect Your First Stream
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {streams.map((stream) => (
            <StreamCard
              key={stream.stream_id}
              stream={stream}
              workspaceName={workspaceId}
              onReconnect={handleReconnect}
              onDisconnect={handleDisconnect}
            />
          ))}
        </div>
      )}

      {/* Dialogs */}
      {showConnectDialog && (
        <ConnectStreamDialog
          workspaceName={workspaceId}
          onClose={() => setShowConnectDialog(false)}
          onSuccess={() => {
            setShowConnectDialog(false);
            refetch();
          }}
        />
      )}

      {disconnectStreamName && (
        <DisconnectStreamDialog
          workspaceName={workspaceId}
          streamName={disconnectStreamName}
          onClose={() => setDisconnectStreamName(null)}
          onSuccess={() => {
            setDisconnectStreamName(null);
            handleDisconnectSuccess();
          }}
        />
      )}

      {showStreamMappingDialog && (
        <CreateStreamMappingDialog
          workspaceId={workspaceId}
          onClose={() => setShowStreamMappingDialog(false)}
          onSuccess={() => {
            setShowStreamMappingDialog(false);
            showToast({
              type: 'success',
              title: 'Stream Mapping Created',
              message: 'Your stream mapping has been created successfully',
            });
          }}
        />
      )}
    </div>
  );
}

