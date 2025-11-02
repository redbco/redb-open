'use client';

interface StatusBadgeProps {
  status: string;
  variant?: 'default' | 'success' | 'warning' | 'danger' | 'info';
  children?: React.ReactNode;
}

export function StatusBadge({ status, variant, children }: StatusBadgeProps) {
  const getVariantStyles = () => {
    // Auto-detect variant from status if not provided
    const detectedVariant = variant || detectVariant(status);
    
    switch (detectedVariant) {
      case 'success':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400';
      case 'warning':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400';
      case 'danger':
        return 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400';
      case 'info':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-400';
    }
  };

  const detectVariant = (status: string): string => {
    const statusLower = status.toLowerCase();
    
    if (statusLower.includes('healthy') || statusLower.includes('active') || 
        statusLower.includes('enabled') || statusLower.includes('success') ||
        statusLower.includes('connected') || statusLower.includes('validated')) {
      return 'success';
    }
    
    if (statusLower.includes('warning') || statusLower.includes('paused') ||
        statusLower.includes('pending') || statusLower.includes('not validated')) {
      return 'warning';
    }
    
    if (statusLower.includes('error') || statusLower.includes('failed') ||
        statusLower.includes('unhealthy') || statusLower.includes('disconnected') ||
        statusLower.includes('disabled')) {
      return 'danger';
    }
    
    if (statusLower.includes('info') || statusLower.includes('running')) {
      return 'info';
    }
    
    return 'default';
  };

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getVariantStyles()}`}>
      {children || status}
    </span>
  );
}

