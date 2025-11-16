'use client';

import { useState, useEffect } from 'react';
import { useTopicSchema } from '@/lib/hooks/useStreams';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ArrowLeft, RefreshCw } from 'lucide-react';
import Link from 'next/link';

interface TopicSchemaPageProps {
  params: Promise<{
    workspaceId: string;
    streamName: string;
    topicName: string;
  }>;
}

export default function TopicSchemaPage({ params }: TopicSchemaPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [streamName, setStreamName] = useState<string>('');
  const [topicName, setTopicName] = useState<string>('');

  useEffect(() => {
    params.then(({ workspaceId: id, streamName: stream, topicName: topic }) => {
      setWorkspaceId(id);
      setStreamName(decodeURIComponent(stream));
      setTopicName(decodeURIComponent(topic));
    });
  }, [params]);

  const { schema, isLoading, error, refetch } = useTopicSchema(workspaceId, streamName, topicName);

  if (!workspaceId || !streamName || !topicName) {
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
          <h3 className="text-xl font-semibold text-foreground mb-2">Failed to Load Schema</h3>
          <p className="text-muted-foreground mb-4">{error.message}</p>
          <Link
            href={`/${workspaceId}/streams/${streamName}`}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Stream
          </Link>
        </div>
      </div>
    );
  }

  if (isLoading || !schema) {
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
            href={`/${workspaceId}/streams/${streamName}`}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div>
            <h2 className="text-3xl font-bold text-foreground">{topicName}</h2>
            <p className="text-muted-foreground mt-1">Topic schema from {streamName}</p>
          </div>
        </div>
        <button
          onClick={refetch}
          className="inline-flex items-center px-3 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
        >
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </button>
      </div>

      {/* Schema Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-card border border-border rounded-lg p-4">
          <p className="text-sm text-muted-foreground">Messages Sampled</p>
          <p className="text-2xl font-bold text-foreground">{schema.messages_sampled}</p>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <p className="text-sm text-muted-foreground">Confidence Score</p>
          <p className="text-2xl font-bold text-foreground">{(schema.confidence_score * 100).toFixed(1)}%</p>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <p className="text-sm text-muted-foreground">Schema Status</p>
          <p className="text-2xl font-bold text-foreground">
            {schema.confidence_score > 0.8 ? 'High' : schema.confidence_score > 0.5 ? 'Medium' : 'Low'}
          </p>
        </div>
      </div>

      {/* Schema Display */}
      <div className="bg-card border border-border rounded-lg p-6">
        <h3 className="text-lg font-semibold text-foreground mb-4">Discovered Schema</h3>
        {schema.schema ? (
          <div className="bg-muted/30 rounded-lg p-4 overflow-x-auto">
            <pre className="text-sm text-foreground font-mono">
              {JSON.stringify(schema.schema, null, 2)}
            </pre>
          </div>
        ) : (
          <div className="text-center py-8">
            <p className="text-muted-foreground">
              No schema available yet. Schema discovery may still be in progress.
            </p>
            <p className="text-sm text-muted-foreground mt-2">
              Try refreshing in a few moments once messages have been sampled.
            </p>
          </div>
        )}
      </div>

      {/* Schema Help */}
      <div className="bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
        <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100 mb-2">About Schema Discovery</h4>
        <p className="text-sm text-blue-800 dark:text-blue-200">
          Schemas are automatically discovered by sampling messages from the topic. The confidence score indicates 
          how reliable the discovered schema is based on the number of messages sampled and consistency of the data structure.
        </p>
      </div>
    </div>
  );
}

