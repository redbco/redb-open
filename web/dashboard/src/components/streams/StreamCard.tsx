'use client';

import Link from 'next/link';
import { Stream } from '@/lib/api/types';
import { StatusBadge } from '@/components/ui/StatusBadge';
import { Activity, RefreshCw, Power, Trash2 } from 'lucide-react';

interface StreamCardProps {
  stream: Stream;
  workspaceName: string;
  onReconnect?: (streamName: string) => void;
  onDisconnect?: (streamName: string) => void;
}

export function StreamCard({ stream, workspaceName, onReconnect, onDisconnect }: StreamCardProps) {
  const getPlatformIcon = (platform: string) => {
    // Return appropriate icon based on platform
    return <Activity className="h-5 w-5" />;
  };

  const getStatusColor = (status: string) => {
    const statusLower = status.toLowerCase();
    if (statusLower.includes('connected') || statusLower.includes('online')) {
      return 'success';
    } else if (statusLower.includes('pending') || statusLower.includes('connecting')) {
      return 'warning';
    } else {
      return 'error';
    }
  };

  return (
    <div className="bg-card border border-border rounded-lg p-6 hover:shadow-lg transition-shadow">
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            {getPlatformIcon(stream.stream_platform)}
          </div>
          <div>
            <Link
              href={`/workspaces/${workspaceName}/streams/${stream.stream_name}`}
              className="text-lg font-semibold text-foreground hover:text-primary transition-colors"
            >
              {stream.stream_name}
            </Link>
            <p className="text-sm text-muted-foreground mt-1">
              {stream.stream_description || 'No description'}
            </p>
          </div>
        </div>
        <StatusBadge status={getStatusColor(stream.status)} />
      </div>

      <div className="space-y-2">
        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground">Platform:</span>
          <span className="font-medium text-foreground capitalize">{stream.stream_platform}</span>
        </div>
        
        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground">Topics:</span>
          <span className="font-medium text-foreground">{stream.monitored_topics?.length || 0}</span>
        </div>

        <div className="flex items-center justify-between text-sm">
          <span className="text-muted-foreground">Node:</span>
          <span className="font-medium text-foreground">{stream.connected_to_node_id}</span>
        </div>

        {stream.connection_config?.brokers && (
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Brokers:</span>
            <span className="font-medium text-foreground text-xs truncate max-w-[150px]">
              {Array.isArray(stream.connection_config.brokers) 
                ? stream.connection_config.brokers.length 
                : '1'} broker(s)
            </span>
          </div>
        )}
      </div>

      <div className="flex items-center space-x-2 mt-4 pt-4 border-t border-border">
        <Link
          href={`/workspaces/${workspaceName}/streams/${stream.stream_name}`}
          className="flex-1 px-3 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors text-center"
        >
          View Details
        </Link>
        
        {onReconnect && (
          <button
            onClick={() => onReconnect(stream.stream_name)}
            className="px-3 py-2 text-sm border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            title="Reconnect Stream"
          >
            <RefreshCw className="h-4 w-4" />
          </button>
        )}
        
        {onDisconnect && (
          <button
            onClick={() => onDisconnect(stream.stream_name)}
            className="px-3 py-2 text-sm border border-input bg-background rounded-md hover:bg-destructive hover:text-destructive-foreground transition-colors"
            title="Disconnect Stream"
          >
            <Power className="h-4 w-4" />
          </button>
        )}
      </div>
    </div>
  );
}

