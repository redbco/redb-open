'use client';

import { ReactNode, ButtonHTMLAttributes } from 'react';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  variant?: 'default' | 'outline' | 'destructive' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

export function Button({ 
  children, 
  variant = 'default', 
  size = 'md',
  className = '',
  disabled,
  ...props 
}: ButtonProps) {
  const getVariantStyles = () => {
    switch (variant) {
      case 'outline':
        return 'border border-input bg-background hover:bg-accent hover:text-accent-foreground';
      case 'destructive':
        return 'bg-red-600 text-white hover:bg-red-700 dark:bg-red-700 dark:hover:bg-red-800';
      case 'ghost':
        return 'hover:bg-accent hover:text-accent-foreground';
      default:
        return 'bg-primary text-primary-foreground hover:bg-primary/90';
    }
  };

  const getSizeStyles = () => {
    switch (size) {
      case 'sm':
        return 'px-3 py-1.5 text-sm';
      case 'lg':
        return 'px-6 py-3 text-lg';
      default:
        return 'px-4 py-2 text-base';
    }
  };

  const disabledStyles = disabled 
    ? 'opacity-50 cursor-not-allowed' 
    : '';

  return (
    <button
      className={`inline-flex items-center justify-center rounded-md font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 ${getVariantStyles()} ${getSizeStyles()} ${disabledStyles} ${className}`}
      disabled={disabled}
      {...props}
    >
      {children}
    </button>
  );
}

