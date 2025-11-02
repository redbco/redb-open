'use client';

import { AlertCircle, RefreshCw } from 'lucide-react';

interface ErrorStateProps {
  title?: string;
  message: string;
  onRetry?: () => void;
  showIcon?: boolean;
}

export function ErrorState({ 
  title = 'Something went wrong', 
  message, 
  onRetry,
  showIcon = true 
}: ErrorStateProps) {
  return (
    <div className="bg-card border border-border rounded-lg p-8 text-center">
      {showIcon && (
        <div className="text-red-600 dark:text-red-400 mb-4">
          <AlertCircle className="h-12 w-12 mx-auto" />
        </div>
      )}
      <h3 className="text-xl font-semibold text-foreground mb-2">{title}</h3>
      <p className="text-muted-foreground mb-4">{message}</p>
      {onRetry && (
        <button
          onClick={onRetry}
          className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors inline-flex items-center"
        >
          <RefreshCw className="h-4 w-4 mr-2" />
          Retry
        </button>
      )}
    </div>
  );
}

