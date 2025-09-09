import { AuthSession, UserProfile } from '@/types/auth';

// Session storage keys
const STORAGE_KEYS = {
  SESSION: 'redb_session',
  USER: 'redb_user',
  ACCESS_TOKEN: 'redb_access_token',
  REFRESH_TOKEN: 'redb_refresh_token',
  SESSION_ID: 'redb_session_id',
  TENANT_ID: 'redb_tenant_id',
  EXPIRES_AT: 'redb_expires_at',
} as const;

// Session storage utility class
export class SessionStorage {
  // Store complete session
  static storeSession(session: AuthSession): void {
    try {
      localStorage.setItem(STORAGE_KEYS.SESSION, JSON.stringify(session));
      localStorage.setItem(STORAGE_KEYS.USER, JSON.stringify(session.user));
      localStorage.setItem(STORAGE_KEYS.ACCESS_TOKEN, session.accessToken);
      localStorage.setItem(STORAGE_KEYS.REFRESH_TOKEN, session.refreshToken);
      localStorage.setItem(STORAGE_KEYS.SESSION_ID, session.sessionId);
      localStorage.setItem(STORAGE_KEYS.TENANT_ID, session.tenantId);
      localStorage.setItem(STORAGE_KEYS.EXPIRES_AT, session.expiresAt.toISOString());
    } catch (error) {
      console.error('Failed to store session:', error);
    }
  }

  // Get complete session
  static getSession(): AuthSession | null {
    try {
      const sessionData = localStorage.getItem(STORAGE_KEYS.SESSION);
      if (!sessionData) return null;

      const session: AuthSession = JSON.parse(sessionData);
      // Convert expiresAt back to Date object
      session.expiresAt = new Date(session.expiresAt);
      
      return session;
    } catch (error) {
      console.error('Failed to get session:', error);
      return null;
    }
  }

  // Get user profile
  static getUser(): UserProfile | null {
    try {
      const userData = localStorage.getItem(STORAGE_KEYS.USER);
      return userData ? JSON.parse(userData) : null;
    } catch (error) {
      console.error('Failed to get user:', error);
      return null;
    }
  }

  // Get access token
  static getAccessToken(): string | null {
    return localStorage.getItem(STORAGE_KEYS.ACCESS_TOKEN);
  }

  // Get refresh token
  static getRefreshToken(): string | null {
    return localStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN);
  }

  // Get session ID
  static getSessionId(): string | null {
    return localStorage.getItem(STORAGE_KEYS.SESSION_ID);
  }

  // Get tenant ID
  static getTenantId(): string | null {
    return localStorage.getItem(STORAGE_KEYS.TENANT_ID);
  }

  // Get expiration date
  static getExpiresAt(): Date | null {
    try {
      const expiresAtStr = localStorage.getItem(STORAGE_KEYS.EXPIRES_AT);
      return expiresAtStr ? new Date(expiresAtStr) : null;
    } catch (error) {
      console.error('Failed to get expires at:', error);
      return null;
    }
  }

  // Check if session is expired
  static isSessionExpired(): boolean {
    const expiresAt = this.getExpiresAt();
    if (!expiresAt) return true;
    
    return new Date() >= expiresAt;
  }

  // Check if session exists and is valid
  static hasValidSession(): boolean {
    const session = this.getSession();
    return session !== null && !this.isSessionExpired();
  }

  // Update access token (for refresh)
  static updateAccessToken(accessToken: string, expiresAt: Date): void {
    try {
      localStorage.setItem(STORAGE_KEYS.ACCESS_TOKEN, accessToken);
      localStorage.setItem(STORAGE_KEYS.EXPIRES_AT, expiresAt.toISOString());
      
      // Update the full session object if it exists
      const session = this.getSession();
      if (session) {
        session.accessToken = accessToken;
        session.expiresAt = expiresAt;
        localStorage.setItem(STORAGE_KEYS.SESSION, JSON.stringify(session));
      }
    } catch (error) {
      console.error('Failed to update access token:', error);
    }
  }

  // Clear all session data
  static clearSession(): void {
    try {
      Object.values(STORAGE_KEYS).forEach(key => {
        localStorage.removeItem(key);
      });
    } catch (error) {
      console.error('Failed to clear session:', error);
    }
  }

  // Store tenant-specific data (for multi-tenant support)
  static storeTenantData(tenantId: string, data: Record<string, unknown>): void {
    try {
      const key = `redb_tenant_${tenantId}`;
      localStorage.setItem(key, JSON.stringify(data));
    } catch (error) {
      console.error('Failed to store tenant data:', error);
    }
  }

  // Get tenant-specific data
  static getTenantData(tenantId: string): Record<string, unknown> | null {
    try {
      const key = `redb_tenant_${tenantId}`;
      const data = localStorage.getItem(key);
      return data ? JSON.parse(data) : null;
    } catch (error) {
      console.error('Failed to get tenant data:', error);
      return null;
    }
  }

  // Clear tenant-specific data
  static clearTenantData(tenantId: string): void {
    try {
      const key = `redb_tenant_${tenantId}`;
      localStorage.removeItem(key);
    } catch (error) {
      console.error('Failed to clear tenant data:', error);
    }
  }
}
