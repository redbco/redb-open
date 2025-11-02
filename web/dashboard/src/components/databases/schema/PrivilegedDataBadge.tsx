'use client';

import { Shield, ShieldAlert, ShieldCheck } from 'lucide-react';

interface PrivilegedDataBadgeProps {
  dataCategory?: string;
  isPrivileged?: boolean;
  confidence?: number;
  description?: string;
  showTooltip?: boolean;
}

export function PrivilegedDataBadge({
  dataCategory = 'standard',
  isPrivileged = false,
  confidence = 0,
  description,
  showTooltip = true,
}: PrivilegedDataBadgeProps) {
  if (!isPrivileged || confidence === 0) {
    return null;
  }

  // Determine color based on confidence level
  const getConfidenceColor = () => {
    if (confidence > 0.7) {
      return {
        bg: 'bg-red-100 dark:bg-red-900/20',
        text: 'text-red-800 dark:text-red-400',
        border: 'border-red-200 dark:border-red-800',
        icon: ShieldAlert,
      };
    } else if (confidence >= 0.4) {
      return {
        bg: 'bg-yellow-100 dark:bg-yellow-900/20',
        text: 'text-yellow-800 dark:text-yellow-400',
        border: 'border-yellow-200 dark:border-yellow-800',
        icon: ShieldCheck,
      };
    } else {
      return {
        bg: 'bg-gray-100 dark:bg-gray-900/20',
        text: 'text-gray-800 dark:text-gray-400',
        border: 'border-gray-200 dark:border-gray-800',
        icon: Shield,
      };
    }
  };

  // Get category-specific styling
  const getCategoryColor = () => {
    const category = dataCategory.toLowerCase();
    if (category.includes('pii') || category.includes('personal')) {
      return 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400';
    } else if (category.includes('identity') || category.includes('auth')) {
      return 'bg-orange-100 text-orange-800 dark:bg-orange-900/20 dark:text-orange-400';
    } else if (category.includes('financial') || category.includes('payment')) {
      return 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400';
    } else if (category.includes('health') || category.includes('medical')) {
      return 'bg-purple-100 text-purple-800 dark:bg-purple-900/20 dark:text-purple-400';
    } else if (category.includes('secret') || category.includes('credential')) {
      return 'bg-pink-100 text-pink-800 dark:bg-pink-900/20 dark:text-pink-400';
    }
    return 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400';
  };

  const confidenceStyle = getConfidenceColor();
  const Icon = confidenceStyle.icon;

  return (
    <div className="inline-flex items-center gap-1">
      {/* Category Badge */}
      <span
        className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getCategoryColor()}`}
        title={showTooltip ? `Data Category: ${dataCategory}` : undefined}
      >
        {dataCategory}
      </span>

      {/* Confidence Badge */}
      <span
        className={`inline-flex items-center gap-1 px-2 py-0.5 rounded border ${confidenceStyle.bg} ${confidenceStyle.text} ${confidenceStyle.border} text-xs font-medium`}
        title={
          showTooltip
            ? `Privileged Data Confidence: ${(confidence * 100).toFixed(0)}%${
                description ? `\n${description}` : ''
              }`
            : undefined
        }
      >
        <Icon className="h-3 w-3" />
        {(confidence * 100).toFixed(0)}%
      </span>
    </div>
  );
}

