'use client';

import { useState } from 'react';
import { useAuth } from '@/lib/auth/auth-context';
import { useToast } from '@/components/ui/Toast';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ApiClientError } from '@/lib/api/client';
import { LogIn } from 'lucide-react';

export default function LoginPage() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { login } = useAuth();
  const { showToast } = useToast();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!username || !password) {
      showToast({
        type: 'error',
        title: 'Validation Error',
        message: 'Please enter both username and password',
      });
      return;
    }

    setIsLoading(true);
    
    try {
      await login({
        username,
        password,
      });
      
      showToast({
        type: 'success',
        title: 'Login Successful',
        message: 'Welcome back!',
      });
    } catch (error) {
      console.error('Login error:', error);
      
      if (error instanceof ApiClientError) {
        if (error.isUnauthorized()) {
          showToast({
            type: 'error',
            title: 'Invalid Credentials',
            message: 'The username or password you entered is incorrect.',
          });
        } else {
          showToast({
            type: 'error',
            title: 'Login Failed',
            message: error.apiError.message || 'An error occurred during login.',
          });
        }
      } else {
        showToast({
          type: 'error',
          title: 'Login Failed',
          message: 'An unexpected error occurred. Please try again.',
        });
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex items-center justify-center min-h-screen p-4">
      <div className="w-full max-w-md">
        <div className="bg-card rounded-lg shadow-lg border border-border p-8">
          {/* Logo/Title */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center justify-center w-16 h-16 bg-primary/10 rounded-full mb-4">
              <LogIn className="h-8 w-8 text-primary" />
            </div>
            <h1 className="text-3xl font-bold text-foreground mb-2">
              Welcome to reDB
            </h1>
            <p className="text-muted-foreground">
              Sign in to access your databases
            </p>
          </div>

          {/* Login Form */}
          <form onSubmit={handleSubmit} className="space-y-6">
            <div>
              <label 
                htmlFor="username" 
                className="block text-sm font-medium text-foreground mb-2"
              >
                Username
              </label>
              <input
                id="username"
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                placeholder="Enter your username"
                disabled={isLoading}
                autoComplete="username"
              />
            </div>

            <div>
              <label 
                htmlFor="password" 
                className="block text-sm font-medium text-foreground mb-2"
              >
                Password
              </label>
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full px-4 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent text-foreground"
                placeholder="Enter your password"
                disabled={isLoading}
                autoComplete="current-password"
              />
            </div>

            <button
              type="submit"
              disabled={isLoading}
              className="w-full py-2 px-4 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center"
            >
              {isLoading ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Signing in...
                </>
              ) : (
                'Sign In'
              )}
            </button>
          </form>
        </div>

        {/* Footer */}
        <div className="mt-6 text-center">
          <p className="text-xs text-muted-foreground">
            Powered by{' '}
            <span className="font-semibold text-primary">reDB</span>
          </p>
        </div>
      </div>
    </div>
  );
}

