'use client';

import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import { ApiClientError } from '@/lib/api/client';
import type { 
  Stream, 
  ConnectStreamRequest,
  ModifyStreamRequest,
  DisconnectStreamRequest,
  TopicInfo,
  TopicSchema
} from '@/lib/api/types';

export function useStreams(workspaceName: string) {
  const [streams, setStreams] = useState<Stream[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchStreams = useCallback(async () => {
    if (!workspaceName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      const response = await api.streams.list(workspaceName);
      setStreams(response.streams);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch streams'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName]);

  useEffect(() => {
    fetchStreams();
  }, [fetchStreams]);

  return { streams, isLoading, error, refetch: fetchStreams };
}

export function useStream(workspaceName: string, streamName: string) {
  const [stream, setStream] = useState<Stream | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchStream = useCallback(async () => {
    if (!workspaceName || !streamName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      const response = await api.streams.show(workspaceName, streamName);
      setStream(response.stream);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch stream'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName, streamName]);

  useEffect(() => {
    fetchStream();
  }, [fetchStream]);

  return { stream, isLoading, error, refetch: fetchStream };
}

export function useConnectStream(workspaceName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const connect = async (request: ConnectStreamRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.streams.connect(workspaceName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to connect stream');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { connect, isLoading, error };
}

export function useModifyStream(workspaceName: string, streamName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const modify = async (request: ModifyStreamRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.streams.modify(workspaceName, streamName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to modify stream');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { modify, isLoading, error };
}

export function useReconnectStream(workspaceName: string, streamName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const reconnect = async () => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.streams.reconnect(workspaceName, streamName);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to reconnect stream');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { reconnect, isLoading, error };
}

export function useDisconnectStream(workspaceName: string, streamName: string) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<ApiClientError | Error | null>(null);

  const disconnect = async (request?: DisconnectStreamRequest) => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await api.streams.disconnect(workspaceName, streamName, request);
      return response;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to disconnect stream');
      setError(error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  };

  return { disconnect, isLoading, error };
}

export function useStreamTopics(workspaceName: string, streamName: string) {
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchTopics = useCallback(async () => {
    if (!workspaceName || !streamName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      const response = await api.streams.listTopics(workspaceName, streamName);
      setTopics(response.topics);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch topics'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName, streamName]);

  useEffect(() => {
    fetchTopics();
  }, [fetchTopics]);

  return { topics, isLoading, error, refetch: fetchTopics };
}

export function useTopicSchema(workspaceName: string, streamName: string, topicName: string) {
  const [schema, setSchema] = useState<TopicSchema | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchSchema = useCallback(async () => {
    if (!workspaceName || !streamName || !topicName) {
      setIsLoading(false);
      return;
    }
    
    try {
      setIsLoading(true);
      const response = await api.streams.getTopicSchema(workspaceName, streamName, topicName);
      setSchema({
        topic_name: response.topic_name,
        schema: response.schema,
        messages_sampled: response.messages_sampled,
        confidence_score: response.confidence_score,
      });
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch topic schema'));
    } finally {
      setIsLoading(false);
    }
  }, [workspaceName, streamName, topicName]);

  useEffect(() => {
    fetchSchema();
  }, [fetchSchema]);

  return { schema, isLoading, error, refetch: fetchSchema };
}

