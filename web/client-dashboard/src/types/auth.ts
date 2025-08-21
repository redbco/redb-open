// Authentication types based on CLI implementation

export interface LoginRequest {
  username: string;
  password: string;
  expiry_time_hours?: string;
  session_name?: string;
  user_agent?: string;
  ip_address?: string;
  platform?: string;
  browser?: string;
  operating_system?: string;
  device_type?: string;
  location?: string;
}

export interface LoginProfile {
  tenant_id: string;
  user_id: string;
  username: string;
  email: string;
  name: string;
}

export interface LoginResponse {
  profile: LoginProfile;
  access_token: string;
  refresh_token: string;
  session_id: string;
  status: string;
}

export interface LogoutRequest {
  refresh_token: string;
}

export interface LogoutResponse {
  message: string;
  success: boolean;
  status: string;
}

export interface UserProfile {
  tenant_id: string;
  user_id: string;
  username: string;
  email: string;
  name: string;
}

export interface ProfileResponse {
  profile: UserProfile;
}

export interface AuthSession {
  user: UserProfile;
  accessToken: string;
  refreshToken: string;
  sessionId: string;
  tenantId: string;
  expiresAt: Date;
}

export interface AuthError {
  message: string;
  code?: string;
  status?: number;
}
