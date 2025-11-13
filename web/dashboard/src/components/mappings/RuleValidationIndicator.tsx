'use client';

import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { CheckCircle, AlertTriangle, XCircle, Info } from 'lucide-react';
import type { ValidationResult } from '@/lib/utils/type-validator';

interface RuleValidationIndicatorProps {
  validation: ValidationResult;
  showLabel?: boolean;
  size?: 'sm' | 'md' | 'lg';
}

export function RuleValidationIndicator({
  validation,
  showLabel = false,
  size = 'md',
}: RuleValidationIndicatorProps) {
  const [isHovered, setIsHovered] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState<{ top: number; left: number } | null>(null);
  const badgeRef = useRef<HTMLDivElement>(null);

  // Calculate tooltip position
  useEffect(() => {
    if (isHovered && badgeRef.current) {
      const rect = badgeRef.current.getBoundingClientRect();
      setTooltipPosition({
        top: rect.top + window.scrollY - 8, // Position above the badge
        left: rect.left + window.scrollX + rect.width / 2, // Center horizontally
      });
    }
  }, [isHovered]);

  const getIcon = () => {
    const iconSizes = {
      sm: 'h-3 w-3',
      md: 'h-4 w-4',
      lg: 'h-5 w-5',
    };

    const iconSize = iconSizes[size];

    switch (validation.level) {
      case 'success':
        return <CheckCircle className={`${iconSize} text-green-600 dark:text-green-400`} />;
      case 'warning':
        return <AlertTriangle className={`${iconSize} text-yellow-600 dark:text-yellow-400`} />;
      case 'error':
        return <XCircle className={`${iconSize} text-red-600 dark:text-red-400`} />;
      case 'info':
        return <Info className={`${iconSize} text-blue-600 dark:text-blue-400`} />;
      default:
        return <Info className={`${iconSize} text-gray-600 dark:text-gray-400`} />;
    }
  };

  const getBackgroundColor = () => {
    switch (validation.level) {
      case 'success':
        return 'bg-green-100 dark:bg-green-900/20 border-green-200 dark:border-green-800/30';
      case 'warning':
        return 'bg-yellow-100 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800/30';
      case 'error':
        return 'bg-red-100 dark:bg-red-900/20 border-red-200 dark:border-red-800/30';
      case 'info':
        return 'bg-blue-100 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800/30';
      default:
        return 'bg-gray-100 dark:bg-gray-900/20 border-gray-200 dark:border-gray-800/30';
    }
  };

  const getTextColor = () => {
    switch (validation.level) {
      case 'success':
        return 'text-green-800 dark:text-green-200';
      case 'warning':
        return 'text-yellow-800 dark:text-yellow-200';
      case 'error':
        return 'text-red-800 dark:text-red-200';
      case 'info':
        return 'text-blue-800 dark:text-blue-200';
      default:
        return 'text-gray-800 dark:text-gray-200';
    }
  };

  const getLabel = () => {
    switch (validation.level) {
      case 'success':
        return 'Valid';
      case 'warning':
        return 'Warning';
      case 'error':
        return 'Error';
      case 'info':
        return 'Info';
      default:
        return 'Unknown';
    }
  };

  return (
    <>
      <div 
        ref={badgeRef}
        className="inline-flex items-center"
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      >
        <div
          className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-md border ${getBackgroundColor()} ${getTextColor()} text-xs font-medium`}
        >
          {getIcon()}
          {showLabel && <span>{getLabel()}</span>}
        </div>
      </div>

      {/* Tooltip Portal */}
      {isHovered && tooltipPosition && typeof document !== 'undefined' && createPortal(
        <div 
          className="fixed z-[100] w-max max-w-sm"
          style={{
            top: `${tooltipPosition.top}px`,
            left: `${tooltipPosition.left}px`,
            transform: 'translate(-50%, -100%)',
          }}
        >
          <div className="bg-gray-900 dark:bg-gray-100 text-white dark:text-gray-900 rounded-lg shadow-lg p-3 text-xs">
            <div className="font-semibold mb-1">{validation.message}</div>
            
            {validation.details && (
              <div className="space-y-1 text-gray-200 dark:text-gray-700">
                <div>
                  Source: <span className="font-mono">{validation.details.sourceType}</span>
                </div>
                <div>
                  Target: <span className="font-mono">{validation.details.targetType}</span>
                </div>
                
                {validation.details.issues.length > 0 && (
                  <div className="mt-2 pt-2 border-t border-gray-700 dark:border-gray-300">
                    <div className="font-semibold mb-1">Issues:</div>
                    <ul className="list-disc list-inside space-y-0.5">
                      {validation.details.issues.map((issue, idx) => (
                        <li key={idx}>{issue}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
            
            {/* Tooltip arrow */}
            <div 
              className="absolute top-full left-1/2 -translate-x-1/2 -mt-1 w-2 h-2 bg-gray-900 dark:bg-gray-100 transform rotate-45"
            ></div>
          </div>
        </div>,
        document.body
      )}
    </>
  );
}

/**
 * Compact version showing just the icon with tooltip
 */
export function RuleValidationIcon({
  validation,
  size = 'md',
}: {
  validation: ValidationResult;
  size?: 'sm' | 'md' | 'lg';
}) {
  return <RuleValidationIndicator validation={validation} showLabel={false} size={size} />;
}

/**
 * Badge version with label
 */
export function RuleValidationBadge({
  validation,
  size = 'md',
}: {
  validation: ValidationResult;
  size?: 'sm' | 'md' | 'lg';
}) {
  return <RuleValidationIndicator validation={validation} showLabel={true} size={size} />;
}

