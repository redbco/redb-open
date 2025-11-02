import { useState, useEffect, useCallback } from 'react';
import { api } from '@/lib/api/endpoints';
import type { Session } from '@/lib/api/types';

export function useSessions() {
  const [sessions, setSessions] = useState<Session[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchSessions = useCallback(async () => {
    try {
      setIsLoading(true);
      const response = await api.users.listSessions();
      setSessions(response.sessions || []);
      setError(null);
    } catch (err) {
      console.error('[useSessions] Error fetching sessions:', err);
      setError(err instanceof Error ? err : new Error('Failed to fetch sessions'));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSessions();
  }, [fetchSessions]);

  return {
    sessions,
    isLoading,
    error,
    refetch: fetchSessions,
  };
}

