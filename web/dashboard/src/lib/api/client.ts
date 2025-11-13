import { ApiError } from './types';

// Get configuration from environment variables
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8080';
const DEFAULT_TENANT = process.env.NEXT_PUBLIC_DEFAULT_TENANT || 'default';

export class ApiClient {
  private baseUrl: string;
  private tenant: string;
  private token: string | null = null;

  constructor(baseUrl: string = API_BASE_URL, tenant: string = DEFAULT_TENANT) {
    this.baseUrl = baseUrl;
    this.tenant = tenant;
    
    // Try to load token from localStorage on client side
    if (typeof window !== 'undefined') {
      this.token = localStorage.getItem('auth_token');
    }
  }

  // Set authentication token
  setToken(token: string) {
    this.token = token;
    if (typeof window !== 'undefined') {
      localStorage.setItem('auth_token', token);
    }
  }

  // Clear authentication token
  clearToken() {
    this.token = null;
    if (typeof window !== 'undefined') {
      localStorage.removeItem('auth_token');
    }
  }

  // Get current token
  getToken(): string | null {
    // Always read from localStorage on client side to ensure we have the latest token
    if (typeof window !== 'undefined') {
      this.token = localStorage.getItem('auth_token');
    }
    return this.token;
  }

  // Build full URL with tenant
  private buildUrl(path: string): string {
    // Remove leading slash if present
    const cleanPath = path.startsWith('/') ? path.slice(1) : path;
    return `${this.baseUrl}/${this.tenant}/${cleanPath}`;
  }

  // Generic request method
  async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = this.buildUrl(path);
    
    // Build headers
    const headers: HeadersInit = {
      'Content-Type': 'application/json',
      ...options.headers,
    };

    // Get the current token (reads from localStorage)
    const token = this.getToken();

    // Add authorization header if token exists
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
      console.log('[API Client] Request with auth token:', url, '(token length:', token.length, 'first 20 chars:', token.substring(0, 20) + '...)');
    } else {
      console.warn('[API Client] Request WITHOUT auth token:', url);
    }

    // Make request
    try {
      const response = await fetch(url, {
        ...options,
        headers,
      });

      // Parse response
      let data: any;
      const contentType = response.headers.get('content-type');
      
      if (contentType && contentType.includes('application/json')) {
        data = await response.json();
      } else {
        data = await response.text();
      }

      // Handle errors
      if (!response.ok) {
        const error: ApiError = {
          error: data.error || 'Request failed',
          message: data.message || `HTTP ${response.status}: ${response.statusText}`,
          status: data.status || 'error',
        };
        throw new ApiClientError(error, response.status);
      }

      return data as T;
    } catch (error) {
      // Re-throw ApiClientError as-is
      if (error instanceof ApiClientError) {
        throw error;
      }

      // Handle network errors
      if (error instanceof TypeError && error.message === 'Failed to fetch') {
        throw new ApiClientError(
          {
            error: 'Network Error',
            message: 'Unable to connect to the API server. Please check your connection.',
            status: 'error',
          },
          0
        );
      }

      // Handle other errors
      throw new ApiClientError(
        {
          error: 'Unknown Error',
          message: error instanceof Error ? error.message : 'An unexpected error occurred',
          status: 'error',
        },
        0
      );
    }
  }

  // Convenience methods
  async get<T>(path: string, options?: RequestInit): Promise<T> {
    return this.request<T>(path, { ...options, method: 'GET' });
  }

  async post<T>(path: string, body?: any, options?: RequestInit): Promise<T> {
    return this.request<T>(path, {
      ...options,
      method: 'POST',
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async put<T>(path: string, body?: any, options?: RequestInit): Promise<T> {
    return this.request<T>(path, {
      ...options,
      method: 'PUT',
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async patch<T>(path: string, body?: any, options?: RequestInit): Promise<T> {
    return this.request<T>(path, {
      ...options,
      method: 'PATCH',
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async delete<T>(path: string, options?: RequestInit): Promise<T> {
    return this.request<T>(path, { ...options, method: 'DELETE' });
  }
}

// Custom error class for API errors
export class ApiClientError extends Error {
  public apiError: ApiError;
  public statusCode: number;

  constructor(apiError: ApiError, statusCode: number) {
    super(apiError.message);
    this.name = 'ApiClientError';
    this.apiError = apiError;
    this.statusCode = statusCode;
  }

  isUnauthorized(): boolean {
    return this.statusCode === 401;
  }

  isForbidden(): boolean {
    return this.statusCode === 403;
  }

  isNotFound(): boolean {
    return this.statusCode === 404;
  }

  isConflict(): boolean {
    return this.statusCode === 409;
  }

  isServerError(): boolean {
    return this.statusCode >= 500;
  }
}

// Create a singleton instance
export const apiClient = new ApiClient();

// Export a function to get the client (useful for SSR)
export function getApiClient(): ApiClient {
  return apiClient;
}

