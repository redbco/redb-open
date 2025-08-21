import { LoginRequest, LoginResponse, LogoutRequest, LogoutResponse, ProfileResponse } from '@/types/auth';

// Get environment variables with defaults
const getClientApiUrl = () => {
  return process.env.NEXT_PUBLIC_CLIENT_API_URL || 'http://localhost:8080';
};

const getSessionName = () => {
  return process.env.NEXT_PUBLIC_SESSION_NAME || 'reDB Client Dashboard';
};

const getUserAgent = () => {
  return process.env.NEXT_PUBLIC_USER_AGENT || 'redb-client-dashboard/1.0.0';
};

// Get system information for session metadata
const getSystemInfo = () => {
  const userAgent = navigator.userAgent;
  let platform = 'Web';
  let operatingSystem = 'Unknown';
  let deviceType = 'Desktop';
  let browser = 'Unknown';

  // Detect operating system
  if (userAgent.includes('Windows')) {
    operatingSystem = 'Windows';
    platform = 'Windows';
  } else if (userAgent.includes('Mac')) {
    operatingSystem = 'macOS';
    platform = 'macOS';
  } else if (userAgent.includes('Linux')) {
    operatingSystem = 'Linux';
    platform = 'Linux';
  } else if (userAgent.includes('Android')) {
    operatingSystem = 'Android';
    platform = 'Android';
    deviceType = 'Mobile';
  } else if (userAgent.includes('iOS')) {
    operatingSystem = 'iOS';
    platform = 'iOS';
    deviceType = 'Mobile';
  }

  // Detect browser
  if (userAgent.includes('Chrome')) {
    browser = 'Chrome';
  } else if (userAgent.includes('Firefox')) {
    browser = 'Firefox';
  } else if (userAgent.includes('Safari')) {
    browser = 'Safari';
  } else if (userAgent.includes('Edge')) {
    browser = 'Edge';
  }

  // Detect device type
  if (userAgent.includes('Mobile') || userAgent.includes('Android') || userAgent.includes('iPhone')) {
    deviceType = 'Mobile';
  } else if (userAgent.includes('Tablet') || userAgent.includes('iPad')) {
    deviceType = 'Tablet';
  }

  return { platform, operatingSystem, deviceType, browser };
};

// Authentication API client
export class AuthAPI {
  private baseUrl: string;

  constructor() {
    this.baseUrl = getClientApiUrl();
  }

  // Login user with tenant context
  async login(username: string, password: string, tenantId: string): Promise<LoginResponse> {
    const { platform, operatingSystem, deviceType, browser } = getSystemInfo();
    
    const loginRequest: LoginRequest = {
      username,
      password,
      session_name: getSessionName(),
      user_agent: getUserAgent(),
      platform,
      operating_system: operatingSystem,
      device_type: deviceType,
      browser,
      // Optional: Add IP address and location if available
      // ip_address: await this.getClientIP(),
      // location: await this.getClientLocation(),
    };

    const url = `${this.baseUrl}/${tenantId}/api/v1/auth/login`;
    
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(loginRequest),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.message || `Login failed: ${response.status} ${response.statusText}`);
    }

    const loginResponse: LoginResponse = await response.json();
    return loginResponse;
  }

  // Logout user
  async logout(refreshToken: string, tenantId: string): Promise<LogoutResponse> {
    const logoutRequest: LogoutRequest = {
      refresh_token: refreshToken,
    };

    const url = `${this.baseUrl}/${tenantId}/api/v1/auth/logout`;
    
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(logoutRequest),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.message || `Logout failed: ${response.status} ${response.statusText}`);
    }

    const logoutResponse: LogoutResponse = await response.json();
    return logoutResponse;
  }

  // Get user profile
  async getProfile(accessToken: string, tenantId: string): Promise<ProfileResponse> {
    const url = `${this.baseUrl}/${tenantId}/api/v1/auth/profile`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.message || `Profile fetch failed: ${response.status} ${response.statusText}`);
    }

    const profileResponse: ProfileResponse = await response.json();
    return profileResponse;
  }

  // Refresh access token
  async refreshToken(refreshToken: string, tenantId: string): Promise<LoginResponse> {
    const url = `${this.baseUrl}/${tenantId}/api/v1/auth/refresh`;
    
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.message || `Token refresh failed: ${response.status} ${response.statusText}`);
    }

    const refreshResponse: LoginResponse = await response.json();
    return refreshResponse;
  }

  // Validate token
  async validateToken(accessToken: string, tenantId: string): Promise<boolean> {
    try {
      await this.getProfile(accessToken, tenantId);
      return true;
    } catch (error) {
      return false;
    }
  }

  // Optional: Get client IP address
  private async getClientIP(): Promise<string | undefined> {
    try {
      const response = await fetch('https://api.ipify.org?format=json');
      const data = await response.json();
      return data.ip;
    } catch (error) {
      console.warn('Failed to get client IP:', error);
      return undefined;
    }
  }

  // Optional: Get client location (requires IP)
  private async getClientLocation(): Promise<string | undefined> {
    try {
      const ip = await this.getClientIP();
      if (!ip) return undefined;
      
      const response = await fetch(`https://ipapi.co/${ip}/json/`);
      const data = await response.json();
      return `${data.city}, ${data.country_name}`;
    } catch (error) {
      console.warn('Failed to get client location:', error);
      return undefined;
    }
  }
}

// Export singleton instance
export const authAPI = new AuthAPI();
