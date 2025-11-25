'use client';

import Link from 'next/link';
import { Stream } from '@/lib/api/types';

import { RefreshCw, Power, ChevronDown, ChevronUp, ExternalLink, Calendar, Server, Activity } from 'lucide-react';
import { StreamIcon } from './StreamIcon';
import { formatStreamPlatform } from '@/lib/formatters';
import { useState } from 'react';

interface StreamCardProps {
  stream: Stream;
  workspaceName: string;
  onReconnect?: (streamName: string) => void;
  onDisconnect?: (streamName: string) => void;
}

export function StreamCard({ stream, workspaceName, onReconnect, onDisconnect }: StreamCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  const getStatusColor = (status: string) => {
    const statusLower = status.toLowerCase();
    if (statusLower.includes('connected') || statusLower.includes('online')) {
      return 'bg-emerald-500';
    } else if (statusLower.includes('pending') || statusLower.includes('connecting')) {
      return 'bg-amber-500';
    } else {
      return 'bg-rose-500';
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
          <div className="w-12 h-12 bg-gradient-to-br from-orange-50 to-orange-100 dark:from-orange-900/20 dark:to-orange-800/20 rounded-xl flex items-center justify-center border border-border shadow-sm group-hover:scale-105 transition-transform duration-200">
            <StreamIcon platform={stream.stream_platform} className="h-7 w-7 text-orange-600 dark:text-orange-400" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <Link
                href={`/workspaces/${workspaceName}/streams/${stream.stream_name}`}
                className="font-bold text-lg text-foreground hover:text-primary hover:underline transition-colors flex items-center gap-1"
                title="View Stream Details"
              >
                {stream.stream_name}
                <ExternalLink className="h-3 w-3 opacity-0 group-hover:opacity-50 transition-opacity" />
              </Link>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground mt-0.5">
              <span className="font-medium truncate max-w-[200px]">
                {formatStreamPlatform(stream.stream_platform)}
              </span>
              <span>•</span>
              <span>{stream.monitored_topics?.length || 0} topics</span>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <div className="relative flex h-2.5 w-2.5">
            <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${getStatusColor(stream.status)}`}></span>
            <span className={`relative inline-flex rounded-full h-2.5 w-2.5 ${getStatusColor(stream.status)}`}></span>
          </div>
          <span className="text-xs font-medium text-muted-foreground capitalize">
            {stream.status.replace('STATUS_', '').toLowerCase()}
          </span>
        </div>
      </div>

      <div className="flex-grow">
        <p className="text-sm text-muted-foreground mb-5 line-clamp-2 min-h-[2.5rem]">
          {stream.stream_description || <span className="text-muted-foreground/60 italic">No description provided</span>}
        </p>

        <div className="grid grid-cols-2 gap-y-3 gap-x-4 mb-4">
          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Server className="h-3.5 w-3.5" />
              Node
            </span>
            <span className="text-sm font-medium text-foreground font-mono truncate">
              {stream.connected_to_node_id}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Activity className="h-3.5 w-3.5" />
              Brokers
            </span>
            <span className="text-sm font-medium text-foreground font-mono">
              {Array.isArray(stream.connection_config?.brokers)
                ? stream.connection_config.brokers.length
                : (stream.connection_config?.brokers ? '1' : '-')}
            </span>
          </div>

          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted-foreground flex items-center gap-1.5">
              <Calendar className="h-3.5 w-3.5" />
              Created
            </span>
            <span className="text-sm font-medium text-foreground">
              {stream.created ? formatDate(stream.created) : '-'}
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
        <div className="flex gap-1">
          {onReconnect && (
            <button
              onClick={() => onReconnect(stream.stream_name)}
              className="text-muted-foreground hover:text-primary p-1.5 rounded-md hover:bg-primary/10 transition-colors"
              title="Reconnect Stream"
            >
              <RefreshCw className="h-4 w-4" />
            </button>
          )}

          {onDisconnect && (
            <button
              onClick={() => onDisconnect(stream.stream_name)}
              className="text-muted-foreground hover:text-red-600 dark:hover:text-red-400 p-1.5 rounded-md hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
              title="Disconnect Stream"
            >
              <Power className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>

      {isExpanded && (
        <div className="mt-3 pt-3 border-t border-border/50 space-y-2.5 text-xs animate-in slide-in-from-top-2 duration-200">
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Stream ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={stream.stream_id}>{stream.stream_id}</span>
          </div>
          <div className="grid grid-cols-[1fr,2fr] gap-2">
            <span className="text-muted-foreground">Tenant ID:</span>
            <span className="font-mono text-foreground truncate select-all" title={stream.tenant_id}>{stream.tenant_id}</span>
          </div>
          {stream.stream_version && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Version:</span>
              <span className="text-foreground truncate" title={stream.stream_version}>{stream.stream_version}</span>
            </div>
          )}
          {stream.owner_id && (
            <div className="grid grid-cols-[1fr,2fr] gap-2">
              <span className="text-muted-foreground">Owner:</span>
              <span className="font-mono text-foreground truncate">{stream.owner_id}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
