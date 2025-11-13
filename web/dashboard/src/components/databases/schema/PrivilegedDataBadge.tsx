'use client';

import { Shield, ShieldAlert, ShieldCheck } from 'lucide-react';

interface PrivilegedDataBadgeProps {
  dataCategory?: string;
  isPrivileged?: boolean;
  confidence?: number;
  description?: string;
  classification?: string;
  detectionMethod?: string;
  showTooltip?: boolean;
}

export function PrivilegedDataBadge({
  dataCategory = 'standard',
  isPrivileged = false,
  confidence = 0,
  description,
  classification,
  detectionMethod,
  showTooltip = true,
}: PrivilegedDataBadgeProps) {
  if (!isPrivileged || confidence === 0) {
    return null;
  }

  // Determine color based on confidence level (data from enriched schema endpoint)
  const getConfidenceColor = () => {
    if (confidence > 0.7) {
      return {
        bg: 'bg-red-100 dark:bg-red-900/20',
        text: 'text-red-800 dark:text-red-400',
        border: 'border-red-200 dark:border-red-800',
        icon: ShieldAlert,
        label: 'High',
      };
    } else if (confidence >= 0.4) {
      return {
        bg: 'bg-yellow-100 dark:bg-yellow-900/20',
        text: 'text-yellow-800 dark:text-yellow-400',
        border: 'border-yellow-200 dark:border-yellow-800',
        icon: ShieldCheck,
        label: 'Medium',
      };
    } else {
      return {
        bg: 'bg-gray-100 dark:bg-gray-900/20',
        text: 'text-gray-800 dark:text-gray-400',
        border: 'border-gray-200 dark:border-gray-800',
        icon: Shield,
        label: 'Low',
      };
    }
  };

  // Get category-specific styling based on privileged classification
  const getCategoryColor = () => {
    if (!classification) {
      return 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400';
    }
    
    const classLower = classification.toLowerCase();
    
    // Map privileged classifications to colors
    if (classLower.includes('pii') || classLower.includes('personal')) {
      return 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-400';
    } else if (classLower.includes('identity') || classLower.includes('auth')) {
      return 'bg-orange-100 text-orange-800 dark:bg-orange-900/20 dark:text-orange-400';
    } else if (classLower.includes('financial') || classLower.includes('payment')) {
      return 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400';
    } else if (classLower.includes('health') || classLower.includes('medical')) {
      return 'bg-purple-100 text-purple-800 dark:bg-purple-900/20 dark:text-purple-400';
    } else if (classLower.includes('secret') || classLower.includes('credential')) {
      return 'bg-pink-100 text-pink-800 dark:bg-pink-900/20 dark:text-pink-400';
    } else if (classLower.includes('text')) {
      return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400';
    }
    return 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-400';
  };

  const confidenceStyle = getConfidenceColor();
  const Icon = confidenceStyle.icon;
  
  // Format classification for display (convert snake_case to Title Case)
  const formatClassification = (cls?: string) => {
    if (!cls) return null;
    return cls
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  };
  
  // Build comprehensive tooltip text with all enriched schema endpoint data
  const tooltipText = showTooltip
    ? [
        `Privileged Data (${confidenceStyle.label} Confidence: ${(confidence * 100).toFixed(0)}%)`,
        classification ? `Classification: ${formatClassification(classification)}` : null,
        detectionMethod ? `Detection: ${detectionMethod}` : null,
        dataCategory && dataCategory !== 'standard' ? `Category: ${dataCategory}` : null,
        description,
      ]
        .filter(Boolean)
        .join('\n')
    : undefined;

  return (
    <div className="inline-flex items-center gap-1">
      {/* Classification Badge */}
      {classification && (
        <span
          className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getCategoryColor()}`}
          title={tooltipText}
        >
          {formatClassification(classification)}
        </span>
      )}

      {/* Confidence Badge with enriched data indicator */}
      <span
        className={`inline-flex items-center gap-1 px-2 py-0.5 rounded border ${confidenceStyle.bg} ${confidenceStyle.text} ${confidenceStyle.border} text-xs font-medium`}
        title={tooltipText}
      >
        <Icon className="h-3 w-3" />
        {(confidence * 100).toFixed(0)}%
      </span>
    </div>
  );
}

