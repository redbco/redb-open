'use client';

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { useRouter } from 'next/navigation';
import { apiClient } from '@/lib/api/client';
import { api } from '@/lib/api/endpoints';
import type { Profile, LoginRequest } from '@/lib/api/types';

interface AuthContextType {
  profile: Profile | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (credentials: LoginRequest) => Promise<void>;
  logout: () => Promise<void>;
  refreshProfile: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [profile, setProfile] = useState<Profile | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const router = useRouter();

  // Check for existing session on mount
  useEffect(() => {
    const checkAuth = async () => {
      console.log('[Auth] Checking existing auth on mount...');
      const token = apiClient.getToken();
      console.log('[Auth] Existing token found:', token ? 'YES' : 'NO');
      
      if (token) {
        // Try to load profile from localStorage
        const storedProfile = localStorage.getItem('user_profile');
        console.log('[Auth] Stored profile found:', storedProfile ? 'YES' : 'NO');
        if (storedProfile) {
          try {
            const parsedProfile = JSON.parse(storedProfile);
            console.log('[Auth] Profile loaded:', parsedProfile.username);
            setProfile(parsedProfile);
          } catch (e) {
            console.error('[Auth] Failed to parse stored profile:', e);
            // Clear invalid data
            localStorage.removeItem('user_profile');
            apiClient.clearToken();
          }
        } else {
          // Token exists but no profile - clear token
          console.warn('[Auth] Token exists but no profile, clearing token');
          apiClient.clearToken();
        }
      }
      
      setIsLoading(false);
      console.log('[Auth] Auth check complete');
    };

    checkAuth();
  }, []);

  const login = async (credentials: LoginRequest) => {
    try {
      console.log('[Auth] Attempting login...');
      const response = await api.auth.login(credentials);
      console.log('[Auth] Login successful, received token:', response.access_token ? 'YES' : 'NO');
      
      // Store token and profile
      apiClient.setToken(response.access_token);
      setProfile(response.profile);
      
      // Persist profile to localStorage
      localStorage.setItem('user_profile', JSON.stringify(response.profile));
      
      // Verify token is set
      const storedToken = apiClient.getToken();
      console.log('[Auth] Token stored successfully:', storedToken ? 'YES' : 'NO');
      console.log('[Auth] Token starts with:', storedToken?.substring(0, 20) + '...');
      
      // Redirect to workspaces page or default workspace
      const defaultWorkspace = process.env.NEXT_PUBLIC_DEFAULT_WORKSPACE;
      if (defaultWorkspace) {
        router.push(`/workspaces/${defaultWorkspace}`);
      } else if (response.profile.workspace_ids && response.profile.workspace_ids.length > 0) {
        router.push(`/workspaces/${response.profile.workspace_ids[0]}`);
      } else {
        router.push('/workspaces');
      }
    } catch (error) {
      console.error('[Auth] Login failed:', error);
      // Clear any existing auth state on failed login
      apiClient.clearToken();
      setProfile(null);
      localStorage.removeItem('user_profile');
      throw error;
    }
  };

  const logout = async () => {
    try {
      // Try to call logout endpoint (best effort)
      await api.auth.logout().catch(() => {
        // Ignore errors from logout endpoint
      });
    } finally {
      // Always clear local state
      apiClient.clearToken();
      setProfile(null);
      localStorage.removeItem('user_profile');
      router.push('/auth/login');
    }
  };

  const refreshProfile = async () => {
    // This would typically call a "me" or "profile" endpoint
    // For now, we'll just keep the existing profile
    // TODO: Implement when profile endpoint is available
    console.warn('refreshProfile not yet implemented');
  };

  const value: AuthContextType = {
    profile,
    isAuthenticated: !!profile && !!apiClient.getToken(),
    isLoading,
    login,
    logout,
    refreshProfile,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}

