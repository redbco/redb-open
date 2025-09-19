'use client';

import { useState } from 'react';
import { 
  User, 
  Mail, 
  Phone, 
  MapPin, 
  Building, 
  Calendar, 
  Shield, 
  Key, 
  Bell, 
  Globe, 
  Save, 
  Edit, 
  Camera,
  Settings,
  Activity,
  Clock,
  CheckCircle,
  AlertCircle,
  Smartphone,
  Monitor,
  Eye,
  EyeOff
} from 'lucide-react';
import { SessionStorage } from '@/lib/auth/storage';

export default function UserProfilePage() {
  const user = SessionStorage.getUser();
  const [isEditing, setIsEditing] = useState(false);
  const [showCurrentPassword, setShowCurrentPassword] = useState(false);
  const [showNewPassword, setShowNewPassword] = useState(false);
  const [activeTab, setActiveTab] = useState('profile');

  // Mock user data - in real app, this would come from API
  const [profileData, setProfileData] = useState({
    name: user?.name || 'John Doe',
    email: user?.email || 'john.doe@company.com',
    phone: '+1 (555) 123-4567',
    location: 'San Francisco, CA',
    company: 'Acme Corporation',
    department: 'Engineering',
    role: 'Senior Database Administrator',
    joinDate: '2023-01-15',
    timezone: 'America/Los_Angeles',
    avatar: null
  });

  const [securityData, setSecurityData] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: '',
    twoFactorEnabled: true,
    lastPasswordChange: '2024-01-15',
    activeSessions: 3
  });

  const [preferencesData, setPreferencesData] = useState({
    theme: 'system',
    language: 'en',
    notifications: {
      email: true,
      push: true,
      sms: false,
      desktop: true
    },
    dashboard: {
      defaultView: 'overview',
      refreshInterval: 30,
      showMetrics: true
    }
  });

  const handleSaveProfile = () => {
    // In real app, this would make an API call
    console.log('Saving profile:', profileData);
    setIsEditing(false);
  };

  const handlePasswordChange = () => {
    // In real app, this would make an API call
    console.log('Changing password');
    setSecurityData(prev => ({
      ...prev,
      currentPassword: '',
      newPassword: '',
      confirmPassword: '',
      lastPasswordChange: new Date().toISOString().split('T')[0]
    }));
  };

  const tabs = [
    { id: 'profile', label: 'Profile', icon: User },
    { id: 'security', label: 'Security', icon: Shield },
    { id: 'preferences', label: 'Preferences', icon: Settings },
    { id: 'activity', label: 'Activity', icon: Activity }
  ];

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-foreground">User Profile</h2>
        <p className="text-muted-foreground mt-2">
          Manage your account settings, security preferences, and personal information.
        </p>
      </div>

      {/* Profile Header */}
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="flex items-center space-x-6">
          <div className="relative">
            <div className="w-24 h-24 bg-primary rounded-full flex items-center justify-center">
              <span className="text-primary-foreground font-bold text-2xl">
                {profileData.name.split(' ').map(n => n[0]).join('')}
              </span>
            </div>
            <button className="absolute bottom-0 right-0 w-8 h-8 bg-background border border-border rounded-full flex items-center justify-center hover:bg-muted">
              <Camera className="h-4 w-4 text-muted-foreground" />
            </button>
          </div>
          <div className="flex-1">
            <h3 className="text-2xl font-bold text-foreground">{profileData.name}</h3>
            <p className="text-muted-foreground">{profileData.role}</p>
            <p className="text-sm text-muted-foreground">{profileData.company} • {profileData.department}</p>
            <div className="flex items-center space-x-4 mt-2">
              <div className="flex items-center space-x-1 text-sm text-muted-foreground">
                <MapPin className="h-4 w-4" />
                <span>{profileData.location}</span>
              </div>
              <div className="flex items-center space-x-1 text-sm text-muted-foreground">
                <Calendar className="h-4 w-4" />
                <span>Joined {new Date(profileData.joinDate).toLocaleDateString()}</span>
              </div>
            </div>
          </div>
          <div className="flex space-x-2">
            <button 
              onClick={() => setIsEditing(!isEditing)}
              className="flex items-center space-x-2 px-4 py-2 border border-border rounded-lg hover:bg-muted transition-colors"
            >
              <Edit className="h-4 w-4" />
              <span>{isEditing ? 'Cancel' : 'Edit Profile'}</span>
            </button>
          </div>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="border-b border-border">
        <nav className="flex space-x-8">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center space-x-2 py-2 px-1 border-b-2 font-medium text-sm transition-colors ${
                activeTab === tab.id
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground hover:border-muted-foreground'
              }`}
            >
              <tab.icon className="h-4 w-4" />
              <span>{tab.label}</span>
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      {activeTab === 'profile' && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Personal Information */}
          <div className="bg-card border border-border rounded-lg">
            <div className="px-6 py-4 border-b border-border">
              <h3 className="text-lg font-semibold text-foreground">Personal Information</h3>
            </div>
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Full Name</label>
                <div className="flex items-center space-x-2">
                  <User className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="text"
                      value={profileData.name}
                      onChange={(e) => setProfileData(prev => ({ ...prev, name: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.name}</span>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Email Address</label>
                <div className="flex items-center space-x-2">
                  <Mail className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="email"
                      value={profileData.email}
                      onChange={(e) => setProfileData(prev => ({ ...prev, email: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.email}</span>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Phone Number</label>
                <div className="flex items-center space-x-2">
                  <Phone className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="tel"
                      value={profileData.phone}
                      onChange={(e) => setProfileData(prev => ({ ...prev, phone: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.phone}</span>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Location</label>
                <div className="flex items-center space-x-2">
                  <MapPin className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="text"
                      value={profileData.location}
                      onChange={(e) => setProfileData(prev => ({ ...prev, location: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.location}</span>
                  )}
                </div>
              </div>

              {isEditing && (
                <div className="flex space-x-3 pt-4">
                  <button
                    onClick={handleSaveProfile}
                    className="flex items-center space-x-2 bg-primary text-primary-foreground px-4 py-2 rounded-lg hover:bg-primary/90 transition-colors"
                  >
                    <Save className="h-4 w-4" />
                    <span>Save Changes</span>
                  </button>
                  <button
                    onClick={() => setIsEditing(false)}
                    className="px-4 py-2 border border-border rounded-lg hover:bg-muted transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Work Information */}
          <div className="bg-card border border-border rounded-lg">
            <div className="px-6 py-4 border-b border-border">
              <h3 className="text-lg font-semibold text-foreground">Work Information</h3>
            </div>
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Company</label>
                <div className="flex items-center space-x-2">
                  <Building className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="text"
                      value={profileData.company}
                      onChange={(e) => setProfileData(prev => ({ ...prev, company: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.company}</span>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Department</label>
                <div className="flex items-center space-x-2">
                  <User className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="text"
                      value={profileData.department}
                      onChange={(e) => setProfileData(prev => ({ ...prev, department: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.department}</span>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Role</label>
                <div className="flex items-center space-x-2">
                  <Shield className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <input
                      type="text"
                      value={profileData.role}
                      onChange={(e) => setProfileData(prev => ({ ...prev, role: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    />
                  ) : (
                    <span className="text-foreground">{profileData.role}</span>
                  )}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Timezone</label>
                <div className="flex items-center space-x-2">
                  <Globe className="h-4 w-4 text-muted-foreground" />
                  {isEditing ? (
                    <select
                      value={profileData.timezone}
                      onChange={(e) => setProfileData(prev => ({ ...prev, timezone: e.target.value }))}
                      className="flex-1 px-3 py-2 border border-border rounded-md bg-background text-foreground"
                    >
                      <option value="America/Los_Angeles">Pacific Time (PT)</option>
                      <option value="America/Denver">Mountain Time (MT)</option>
                      <option value="America/Chicago">Central Time (CT)</option>
                      <option value="America/New_York">Eastern Time (ET)</option>
                      <option value="Europe/London">London (GMT)</option>
                      <option value="Europe/Berlin">Berlin (CET)</option>
                      <option value="Asia/Tokyo">Tokyo (JST)</option>
                    </select>
                  ) : (
                    <span className="text-foreground">{profileData.timezone}</span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'security' && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Password */}
          <div className="bg-card border border-border rounded-lg">
            <div className="px-6 py-4 border-b border-border">
              <h3 className="text-lg font-semibold text-foreground">Password</h3>
            </div>
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Current Password</label>
                <div className="relative">
                  <input
                    type={showCurrentPassword ? "text" : "password"}
                    value={securityData.currentPassword}
                    onChange={(e) => setSecurityData(prev => ({ ...prev, currentPassword: e.target.value }))}
                    className="w-full px-3 py-2 pr-10 border border-border rounded-md bg-background text-foreground"
                    placeholder="Enter current password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowCurrentPassword(!showCurrentPassword)}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2"
                  >
                    {showCurrentPassword ? (
                      <EyeOff className="h-4 w-4 text-muted-foreground" />
                    ) : (
                      <Eye className="h-4 w-4 text-muted-foreground" />
                    )}
                  </button>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">New Password</label>
                <div className="relative">
                  <input
                    type={showNewPassword ? "text" : "password"}
                    value={securityData.newPassword}
                    onChange={(e) => setSecurityData(prev => ({ ...prev, newPassword: e.target.value }))}
                    className="w-full px-3 py-2 pr-10 border border-border rounded-md bg-background text-foreground"
                    placeholder="Enter new password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowNewPassword(!showNewPassword)}
                    className="absolute right-3 top-1/2 transform -translate-y-1/2"
                  >
                    {showNewPassword ? (
                      <EyeOff className="h-4 w-4 text-muted-foreground" />
                    ) : (
                      <Eye className="h-4 w-4 text-muted-foreground" />
                    )}
                  </button>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">Confirm New Password</label>
                <input
                  type="password"
                  value={securityData.confirmPassword}
                  onChange={(e) => setSecurityData(prev => ({ ...prev, confirmPassword: e.target.value }))}
                  className="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground"
                  placeholder="Confirm new password"
                />
              </div>

              <div className="text-sm text-muted-foreground">
                <p>Last password change: {new Date(securityData.lastPasswordChange).toLocaleDateString()}</p>
              </div>

              <button
                onClick={handlePasswordChange}
                className="flex items-center space-x-2 bg-primary text-primary-foreground px-4 py-2 rounded-lg hover:bg-primary/90 transition-colors"
              >
                <Key className="h-4 w-4" />
                <span>Update Password</span>
              </button>
            </div>
          </div>

          {/* Two-Factor Authentication */}
          <div className="bg-card border border-border rounded-lg">
            <div className="px-6 py-4 border-b border-border">
              <h3 className="text-lg font-semibold text-foreground">Two-Factor Authentication</h3>
            </div>
            <div className="p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="font-medium text-foreground">2FA Status</p>
                  <p className="text-sm text-muted-foreground">
                    {securityData.twoFactorEnabled ? 'Two-factor authentication is enabled' : 'Two-factor authentication is disabled'}
                  </p>
                </div>
                <div className="flex items-center space-x-2">
                  {securityData.twoFactorEnabled ? (
                    <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400" />
                  ) : (
                    <AlertCircle className="h-5 w-5 text-yellow-600 dark:text-yellow-400" />
                  )}
                  <button
                    onClick={() => setSecurityData(prev => ({ ...prev, twoFactorEnabled: !prev.twoFactorEnabled }))}
                    className={`px-3 py-1 rounded-md text-sm font-medium transition-colors ${
                      securityData.twoFactorEnabled
                        ? 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400 hover:bg-red-200 dark:hover:bg-red-900/30'
                        : 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400 hover:bg-green-200 dark:hover:bg-green-900/30'
                    }`}
                  >
                    {securityData.twoFactorEnabled ? 'Disable' : 'Enable'}
                  </button>
                </div>
              </div>

              <div className="border-t border-border pt-4">
                <h4 className="font-medium text-foreground mb-2">Active Sessions</h4>
                <div className="space-y-2">
                  {[
                    { device: 'MacBook Pro', location: 'San Francisco, CA', current: true, lastActive: 'Now' },
                    { device: 'iPhone 15', location: 'San Francisco, CA', current: false, lastActive: '2 hours ago' },
                    { device: 'Chrome Browser', location: 'San Francisco, CA', current: false, lastActive: '1 day ago' }
                  ].map((session, index) => (
                    <div key={index} className="flex items-center justify-between p-3 border border-border rounded-md">
                      <div className="flex items-center space-x-3">
                        {session.device.includes('MacBook') ? (
                          <Monitor className="h-4 w-4 text-muted-foreground" />
                        ) : session.device.includes('iPhone') ? (
                          <Smartphone className="h-4 w-4 text-muted-foreground" />
                        ) : (
                          <Globe className="h-4 w-4 text-muted-foreground" />
                        )}
                        <div>
                          <p className="font-medium text-foreground">
                            {session.device}
                            {session.current && <span className="ml-2 text-xs bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400 px-2 py-0.5 rounded-full">Current</span>}
                          </p>
                          <p className="text-sm text-muted-foreground">{session.location} • {session.lastActive}</p>
                        </div>
                      </div>
                      {!session.current && (
                        <button className="text-sm text-red-600 dark:text-red-400 hover:text-red-700 dark:hover:text-red-300">
                          Revoke
                        </button>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'preferences' && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Appearance */}
          <div className="bg-card border border-border rounded-lg">
            <div className="px-6 py-4 border-b border-border">
              <h3 className="text-lg font-semibold text-foreground">Appearance</h3>
            </div>
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">Theme</label>
                <div className="grid grid-cols-3 gap-2">
                  {[
                    { value: 'light', label: 'Light' },
                    { value: 'dark', label: 'Dark' },
                    { value: 'system', label: 'System' }
                  ].map((theme) => (
                    <button
                      key={theme.value}
                      onClick={() => setPreferencesData(prev => ({ ...prev, theme: theme.value }))}
                      className={`p-3 border rounded-lg text-sm font-medium transition-colors ${
                        preferencesData.theme === theme.value
                          ? 'border-primary bg-primary/10 text-primary'
                          : 'border-border hover:bg-muted'
                      }`}
                    >
                      {theme.label}
                    </button>
                  ))}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-2">Language</label>
                <select
                  value={preferencesData.language}
                  onChange={(e) => setPreferencesData(prev => ({ ...prev, language: e.target.value }))}
                  className="w-full px-3 py-2 border border-border rounded-md bg-background text-foreground"
                >
                  <option value="en">English</option>
                  <option value="es">Spanish</option>
                  <option value="fr">French</option>
                  <option value="de">German</option>
                  <option value="ja">Japanese</option>
                </select>
              </div>
            </div>
          </div>

          {/* Notifications */}
          <div className="bg-card border border-border rounded-lg">
            <div className="px-6 py-4 border-b border-border">
              <h3 className="text-lg font-semibold text-foreground">Notifications</h3>
            </div>
            <div className="p-6 space-y-4">
              {[
                { key: 'email', label: 'Email Notifications', icon: Mail },
                { key: 'push', label: 'Push Notifications', icon: Bell },
                { key: 'sms', label: 'SMS Notifications', icon: Phone },
                { key: 'desktop', label: 'Desktop Notifications', icon: Monitor }
              ].map((notification) => (
                <div key={notification.key} className="flex items-center justify-between">
                  <div className="flex items-center space-x-3">
                    <notification.icon className="h-4 w-4 text-muted-foreground" />
                    <span className="text-foreground">{notification.label}</span>
                  </div>
                  <button
                    onClick={() => setPreferencesData(prev => ({
                      ...prev,
                      notifications: {
                        ...prev.notifications,
                        [notification.key]: !prev.notifications[notification.key as keyof typeof prev.notifications]
                      }
                    }))}
                    className={`w-12 h-6 rounded-full transition-colors ${
                      preferencesData.notifications[notification.key as keyof typeof preferencesData.notifications]
                        ? 'bg-primary'
                        : 'bg-muted'
                    }`}
                  >
                    <div
                      className={`w-5 h-5 bg-white rounded-full shadow transition-transform ${
                        preferencesData.notifications[notification.key as keyof typeof preferencesData.notifications]
                          ? 'translate-x-6'
                          : 'translate-x-0.5'
                      }`}
                    />
                  </button>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {activeTab === 'activity' && (
        <div className="bg-card border border-border rounded-lg">
          <div className="px-6 py-4 border-b border-border">
            <h3 className="text-lg font-semibold text-foreground">Recent Activity</h3>
          </div>
          <div className="p-6">
            <div className="space-y-4">
              {[
                {
                  action: 'Logged in from new device',
                  details: 'MacBook Pro • San Francisco, CA',
                  time: '2 hours ago',
                  type: 'security',
                  icon: Shield
                },
                {
                  action: 'Updated workspace settings',
                  details: 'Production workspace • Database configuration',
                  time: '1 day ago',
                  type: 'settings',
                  icon: Settings
                },
                {
                  action: 'Password changed',
                  details: 'Security settings updated',
                  time: '3 days ago',
                  type: 'security',
                  icon: Key
                },
                {
                  action: 'Profile information updated',
                  details: 'Phone number and location changed',
                  time: '1 week ago',
                  type: 'profile',
                  icon: User
                },
                {
                  action: 'Two-factor authentication enabled',
                  details: 'Security enhancement activated',
                  time: '2 weeks ago',
                  type: 'security',
                  icon: Shield
                }
              ].map((activity, index) => (
                <div key={index} className="flex items-start space-x-3 p-3 border border-border rounded-lg">
                  <div className={`w-8 h-8 rounded-full flex items-center justify-center ${
                    activity.type === 'security' ? 'bg-red-100 dark:bg-red-900/20' :
                    activity.type === 'settings' ? 'bg-blue-100 dark:bg-blue-900/20' :
                    'bg-green-100 dark:bg-green-900/20'
                  }`}>
                    <activity.icon className={`h-4 w-4 ${
                      activity.type === 'security' ? 'text-red-600 dark:text-red-400' :
                      activity.type === 'settings' ? 'text-blue-600 dark:text-blue-400' :
                      'text-green-600 dark:text-green-400'
                    }`} />
                  </div>
                  <div className="flex-1">
                    <p className="font-medium text-foreground">{activity.action}</p>
                    <p className="text-sm text-muted-foreground">{activity.details}</p>
                    <div className="flex items-center space-x-1 mt-1">
                      <Clock className="h-3 w-3 text-muted-foreground" />
                      <span className="text-xs text-muted-foreground">{activity.time}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}