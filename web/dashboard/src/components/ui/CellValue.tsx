/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import { JSX, useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Copy, ChevronDown, ChevronRight, Check, X } from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/Tooltip';

interface CellValueProps {
  value: any;
  className?: string;
  dataType?: string; // Column data type from schema (e.g., 'jsonb', 'json', etc.)
  maxStringLength?: number; // Maximum length before truncating strings (default: 70)
}

export function CellValue({ value, className = '', dataType, maxStringLength = 70 }: CellValueProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);
  const [popupPosition, setPopupPosition] = useState({ top: 0, left: 0 });
  const buttonRef = useRef<HTMLButtonElement>(null);
  const [isMounted, setIsMounted] = useState(false);

  // Set mounted state for portal
  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Calculate popup position when expanded
  useEffect(() => {
    if (isExpanded && buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      // Use fixed positioning relative to viewport (no scroll offset needed)
      setPopupPosition({
        top: rect.bottom + 4,
        left: rect.left,
      });
    }
  }, [isExpanded]);

  // Parse stringified JSON if the column type is json/jsonb
  const parseValueIfNeeded = (val: any): any => {
    // Handle boolean type with string values
    if (dataType && dataType.toLowerCase() === 'boolean') {
      if (typeof val === 'string') {
        const lowerVal = val.toLowerCase().trim();
        if (lowerVal === 'true' || lowerVal === '1' || lowerVal === 't' || lowerVal === 'yes' || lowerVal === 'y') {
          return true;
        }
        if (lowerVal === 'false' || lowerVal === '0' || lowerVal === 'f' || lowerVal === 'no' || lowerVal === 'n') {
          return false;
        }
      }
      // If it's already a boolean or a number, convert appropriately
      if (typeof val === 'number') {
        return val !== 0;
      }
      return val;
    }
    
    // If dataType indicates JSON and value is a string, try to parse it
    if (dataType && typeof val === 'string') {
      const normalizedType = dataType.toLowerCase();
      if (normalizedType === 'jsonb' || normalizedType === 'json' || normalizedType.includes('json')) {
        try {
          return JSON.parse(val);
        } catch (e) {
          // If parsing fails, return the original string
          console.warn('Failed to parse JSON string:', e);
          return val;
        }
      }
      
      // Check if it's an array type (e.g., 'integer[]', 'text[]', 'varchar[]')
      if (normalizedType.includes('[]') || normalizedType.includes('array')) {
        try {
          // Try to parse PostgreSQL array format: {value1,value2,value3}
          if (val.startsWith('{') && val.endsWith('}')) {
            const arrayContent = val.slice(1, -1);
            // Handle empty array
            if (arrayContent === '') return [];
            
            // Split by comma, but be careful with nested structures
            const items = arrayContent.split(',').map((item: string) => {
              const trimmed = item.trim();
              // Remove quotes if present
              if ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
                  (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
                return trimmed.slice(1, -1);
              }
              // Try to parse as number if it looks like one
              if (!isNaN(Number(trimmed)) && trimmed !== '') {
                return Number(trimmed);
              }
              return trimmed;
            });
            return items;
          }
          
          // Try standard JSON array format
          return JSON.parse(val);
        } catch (e) {
          console.warn('Failed to parse array string:', e);
          return val;
        }
      }
    }
    return val;
  };

  // Get the actual value to render (parsed if needed)
  const processedValue = parseValueIfNeeded(value);

  // Check if this is a date/time type and format accordingly
  const isDateTimeType = (type: string | undefined): boolean => {
    if (!type) return false;
    const normalized = type.toLowerCase();
    return (
      normalized.includes('timestamp') ||
      normalized.includes('datetime') ||
      normalized.includes('date') ||
      normalized.includes('time')
    );
  };

  // Format date/time values
  const formatDateTime = (val: any, type: string | undefined): { formatted: string; original: string; tooltip: string } | null => {
    if (!type || !isDateTimeType(type)) return null;
    if (val === null || val === undefined) return null;

    const normalized = type.toLowerCase();
    const valStr = String(val);
    
    try {
      // Try to parse the date
      let date: Date;
      
      // Handle Unix timestamps (numbers or numeric strings)
      if (!isNaN(Number(val)) && String(val).length >= 10) {
        // If it looks like a Unix timestamp in milliseconds
        if (String(val).length === 13) {
          date = new Date(Number(val));
        } else {
          // Assume seconds
          date = new Date(Number(val) * 1000);
        }
      } else {
        date = new Date(valStr);
      }

      // Check if date is valid
      if (isNaN(date.getTime())) {
        return null;
      }

      // Format based on the type
      const hasTimezone = normalized.includes('timestamptz') || 
                          normalized.includes('with time zone') ||
                          valStr.includes('Z') ||
                          /[+-]\d{2}:\d{2}$/.test(valStr);

      let formatted = '';
      let tooltip = '';

      // Date only
      if (normalized === 'date' || (!normalized.includes('time') && !normalized.includes('stamp'))) {
        formatted = date.toLocaleDateString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
        });
        tooltip = `Full date: ${date.toLocaleDateString('en-US', { 
          weekday: 'long', 
          year: 'numeric', 
          month: 'long', 
          day: 'numeric' 
        })}`;
      }
      // Time only
      else if (normalized.includes('time') && !normalized.includes('stamp') && !normalized.includes('date')) {
        formatted = date.toLocaleTimeString('en-US', {
          hour: 'numeric',
          minute: '2-digit',
          second: '2-digit',
        });
        tooltip = `Original: ${valStr}`;
      }
      // DateTime / Timestamp
      else {
        const dateOptions: Intl.DateTimeFormatOptions = {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: 'numeric',
          minute: '2-digit',
        };

        if (hasTimezone) {
          dateOptions.timeZoneName = 'short';
        }

        formatted = date.toLocaleString('en-US', dateOptions);

        // Calculate relative time
        const now = new Date();
        const diffMs = now.getTime() - date.getTime();
        const diffSecs = Math.floor(diffMs / 1000);
        const diffMins = Math.floor(diffSecs / 60);
        const diffHours = Math.floor(diffMins / 60);
        const diffDays = Math.floor(diffHours / 24);
        
        let relativeTime = '';
        if (diffDays > 365) {
          relativeTime = `${Math.floor(diffDays / 365)} year${Math.floor(diffDays / 365) !== 1 ? 's' : ''} ago`;
        } else if (diffDays > 30) {
          relativeTime = `${Math.floor(diffDays / 30)} month${Math.floor(diffDays / 30) !== 1 ? 's' : ''} ago`;
        } else if (diffDays > 0) {
          relativeTime = `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
        } else if (diffHours > 0) {
          relativeTime = `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
        } else if (diffMins > 0) {
          relativeTime = `${diffMins} minute${diffMins !== 1 ? 's' : ''} ago`;
        } else if (diffSecs > 0) {
          relativeTime = `${diffSecs} second${diffSecs !== 1 ? 's' : ''} ago`;
        } else {
          relativeTime = 'just now';
        }

        tooltip = `${relativeTime}${hasTimezone ? ' (with timezone)' : ' (no timezone)'}\nOriginal: ${valStr}`;
      }

      return {
        formatted,
        original: valStr,
        tooltip,
      };
    } catch (e) {
      console.warn('Failed to parse date/time value:', e);
      return null;
    }
  };

  const dateTimeFormatted = formatDateTime(processedValue, dataType);

  // Handle null and undefined
  if (processedValue === null || processedValue === undefined) {
    return <span className={`text-muted-foreground italic ${className}`}>NULL</span>;
  }

  // Handle date/time formatting
  if (dateTimeFormatted) {
    return (
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <span className={`cursor-help ${className}`}>
              {dateTimeFormatted.formatted}
            </span>
          </TooltipTrigger>
          <TooltipContent>
            <div className="text-xs whitespace-pre-line">
              {dateTimeFormatted.tooltip}
            </div>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    );
  }

  // Get the type of value
  const valueType = Array.isArray(processedValue) ? 'array' : typeof processedValue;

  // Check if this is a text-based column type that should always show character info
  const isTextColumn = (type: string | undefined): boolean => {
    if (!type) return false;
    const normalized = type.toLowerCase();
    return (
      normalized === 'text' ||
      normalized === 'longtext' ||
      normalized === 'mediumtext' ||
      normalized.startsWith('varchar') ||
      normalized.startsWith('char') ||
      normalized === 'string' ||
      normalized === 'clob'
    );
  };

  // Handle strings - with consistent formatting for text columns
  if (valueType === 'string') {
    const stringValue = String(processedValue);
    const isLongString = stringValue.length > maxStringLength;
    const shouldShowTextUI = isTextColumn(dataType);
    
    // For text columns, always show consistent UI (copy button)
    if (shouldShowTextUI) {
      const handleCopy = async () => {
        try {
          await navigator.clipboard.writeText(stringValue);
          setCopySuccess(true);
          setTimeout(() => setCopySuccess(false), 2000);
        } catch (err) {
          console.error('Failed to copy:', err);
        }
      };

      return (
        <>
          <div className={`inline-flex items-center gap-1 ${className}`}>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <button
                    onClick={handleCopy}
                    className="p-1 hover:bg-muted/50 rounded transition-colors"
                  >
                    <Copy className="h-3 w-3 text-muted-foreground hover:text-foreground" />
                  </button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>{copySuccess ? 'Copied!' : 'Copy text'}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>

            <span className="text-sm">
              {isLongString ? `${stringValue.substring(0, maxStringLength)}...` : stringValue}
            </span>

            {isLongString && (
              <button
                ref={buttonRef}
                onClick={() => setIsExpanded(!isExpanded)}
                className="inline-flex items-center gap-1 hover:bg-muted/50 px-1.5 py-1 rounded transition-colors ml-1"
                title={isExpanded ? 'Collapse' : 'Expand full text'}
              >
                {isExpanded ? (
                  <ChevronDown className="h-3 w-3 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-3 w-3 text-muted-foreground" />
                )}
              </button>
            )}
          </div>

          {/* Render popup in a portal for full text */}
          {isExpanded && isMounted && createPortal(
            <>
              {/* Backdrop to close on click */}
              <div
                className="fixed inset-0 z-40"
                onClick={() => setIsExpanded(false)}
              />
              {/* Popover content */}
              <div 
                className="fixed z-50 p-3 bg-card border border-border rounded-lg shadow-lg max-h-96 overflow-auto min-w-[300px] max-w-[600px]"
                style={{
                  top: `${popupPosition.top}px`,
                  left: `${popupPosition.left}px`,
                }}
              >
                <div className="flex items-center justify-between mb-2 pb-2 border-b border-border">
                  <span className="text-xs font-semibold text-foreground">
                    Full Text ({stringValue.length} characters)
                  </span>
                  <button
                    onClick={handleCopy}
                    className="flex items-center gap-1 px-2 py-1 text-xs hover:bg-muted rounded transition-colors"
                  >
                    <Copy className="h-3 w-3" />
                    {copySuccess ? 'Copied!' : 'Copy'}
                  </button>
                </div>
                <div className="text-sm whitespace-pre-wrap break-words">
                  {stringValue}
                </div>
              </div>
            </>,
            document.body
          )}
        </>
      );
    }
    
    // For non-text columns (e.g., uuid, enum), show plain string
    return <span className={className}>{stringValue}</span>;
  }

  // Handle numbers
  if (valueType === 'number') {
    return <span className={className}>{String(processedValue)}</span>;
  }

  // Handle boolean with visual indicator
  if (valueType === 'boolean') {
    return (
      <span className={`inline-flex items-center gap-1.5 ${className}`}>
        {processedValue ? (
          <>
            <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-green-100 dark:bg-green-900/30">
              <Check className="h-3.5 w-3.5 text-green-600 dark:text-green-400" strokeWidth={3} />
            </span>
            <span className="text-sm font-medium text-green-600 dark:text-green-400">true</span>
          </>
        ) : (
          <>
            <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-red-100 dark:bg-red-900/30">
              <X className="h-3.5 w-3.5 text-red-600 dark:text-red-400" strokeWidth={3} />
            </span>
            <span className="text-sm font-medium text-red-600 dark:text-red-400">false</span>
          </>
        )}
      </span>
    );
  }

  // Handle objects and arrays
  if (valueType === 'object' || valueType === 'array') {
    const handleCopy = async () => {
      try {
        await navigator.clipboard.writeText(JSON.stringify(processedValue, null, 2));
        setCopySuccess(true);
        setTimeout(() => setCopySuccess(false), 2000);
      } catch (err) {
        console.error('Failed to copy:', err);
      }
    };

    // Create a preview of the object/array
    const getPreview = () => {
      if (valueType === 'array') {
        const length = processedValue.length;
        if (length === 0) return '[]';
        
        // Show first few items if they're primitives
        const preview = processedValue.slice(0, 2).map((item: any) => {
          if (typeof item === 'object' && item !== null) {
            return Array.isArray(item) ? '[…]' : '{…}';
          }
          return JSON.stringify(item);
        }).join(', ');
        
        return length > 2 ? `[${preview}, …]` : `[${preview}]`;
      } else {
        // Object
        const keys = Object.keys(processedValue);
        if (keys.length === 0) return '{}';
        
        // Show first few key-value pairs
        const preview = keys.slice(0, 2).map(key => {
          const val = processedValue[key];
          if (typeof val === 'object' && val !== null) {
            return `${key}: ${Array.isArray(val) ? '[…]' : '{…}'}`;
          }
          return `${key}: ${JSON.stringify(val)}`;
        }).join(', ');
        
        return keys.length > 2 ? `{${preview}, …}` : `{${preview}}`;
      }
    };

    const getCountText = () => {
      if (valueType === 'array') {
        const length = processedValue.length;
        return `${length} item${length !== 1 ? 's' : ''}`;
      } else {
        const keyCount = Object.keys(processedValue).length;
        return `${keyCount} key${keyCount !== 1 ? 's' : ''}`;
      }
    };

    // Format JSON for display
    const formatJson = (obj: any, indent = 0): JSX.Element[] => {
      const lines: JSX.Element[] = [];
      const indentStr = '  '.repeat(indent);

      if (Array.isArray(obj)) {
        if (obj.length === 0) {
          lines.push(
            <div key="empty-array" className="text-muted-foreground">
              {indentStr}[]
            </div>
          );
          return lines;
        }

        lines.push(
          <div key="array-open" className="text-muted-foreground">
            {indentStr}[
          </div>
        );

        obj.forEach((item, index) => {
          const isLast = index === obj.length - 1;
          if (typeof item === 'object' && item !== null) {
            lines.push(...formatJson(item, indent + 1));
            if (!isLast) {
              lines.push(
                <div key={`comma-${index}`} className="text-muted-foreground">
                  {','}
                </div>
              );
            }
          } else {
            const itemStr = JSON.stringify(item);
            lines.push(
              <div key={`item-${index}`}>
                <span className="text-muted-foreground">{indentStr}  </span>
                <span className={getValueColorClass(item)}>{itemStr}</span>
                {!isLast && <span className="text-muted-foreground">,</span>}
              </div>
            );
          }
        });

        lines.push(
          <div key="array-close" className="text-muted-foreground">
            {indentStr}]
          </div>
        );
      } else {
        const keys = Object.keys(obj);
        if (keys.length === 0) {
          lines.push(
            <div key="empty-object" className="text-muted-foreground">
              {indentStr}{'{}'}
            </div>
          );
          return lines;
        }

        lines.push(
          <div key="object-open" className="text-muted-foreground">
            {indentStr}{'{'}
          </div>
        );

        keys.forEach((key, index) => {
          const val = obj[key];
          const isLast = index === keys.length - 1;

          if (typeof val === 'object' && val !== null) {
            lines.push(
              <div key={`key-${key}`}>
                <span className="text-muted-foreground">{indentStr}  </span>
                <span className="text-blue-600 dark:text-blue-400">&quot;{key}&quot;</span>
                <span className="text-muted-foreground">: </span>
              </div>
            );
            lines.push(...formatJson(val, indent + 1));
            if (!isLast) {
              lines.push(
                <div key={`comma-${key}`} className="text-muted-foreground">
                  {','}
                </div>
              );
            }
          } else {
            const valStr = JSON.stringify(val);
            lines.push(
              <div key={`prop-${key}`}>
                <span className="text-muted-foreground">{indentStr}  </span>
                <span className="text-blue-600 dark:text-blue-400">&quot;{key}&quot;</span>
                <span className="text-muted-foreground">: </span>
                <span className={getValueColorClass(val)}>{valStr}</span>
                {!isLast && <span className="text-muted-foreground">,</span>}
              </div>
            );
          }
        });

        lines.push(
          <div key="object-close" className="text-muted-foreground">
            {indentStr}{'}'}
          </div>
        );
      }

      return lines;
    };

    const getValueColorClass = (val: any): string => {
      if (val === null) return 'text-muted-foreground italic';
      if (typeof val === 'string') return 'text-green-600 dark:text-green-400';
      if (typeof val === 'number') return 'text-purple-600 dark:text-purple-400';
      if (typeof val === 'boolean') return 'text-orange-600 dark:text-orange-400';
      return 'text-foreground';
    };

    return (
      <>
        <div className={`inline-flex items-center gap-1 ${className}`}>
          <button
            ref={buttonRef}
            onClick={() => setIsExpanded(!isExpanded)}
            className="inline-flex items-center gap-1 hover:bg-muted/50 px-2 py-1 rounded transition-colors"
            title={isExpanded ? 'Collapse' : 'Expand'}
          >
            {isExpanded ? (
              <ChevronDown className="h-3 w-3 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-3 w-3 text-muted-foreground" />
            )}
            <span className="text-xs text-muted-foreground font-mono">
              {getCountText()}
            </span>
          </button>

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  onClick={handleCopy}
                  className="p-1 hover:bg-muted/50 rounded transition-colors"
                >
                  <Copy className="h-3 w-3 text-muted-foreground hover:text-foreground" />
                </button>
              </TooltipTrigger>
              <TooltipContent>
                <p>{copySuccess ? 'Copied!' : 'Copy JSON'}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>

          {!isExpanded && (
            <span className="text-xs font-mono text-muted-foreground max-w-xs truncate">
              {getPreview()}
            </span>
          )}
        </div>

        {/* Render popup in a portal to avoid clipping */}
        {isExpanded && isMounted && createPortal(
          <>
            {/* Backdrop to close on click */}
            <div
              className="fixed inset-0 z-40"
              onClick={() => setIsExpanded(false)}
            />
            {/* Popover content */}
            <div 
              className="fixed z-50 p-3 bg-card border border-border rounded-lg shadow-lg max-h-96 overflow-auto min-w-[300px] max-w-[600px]"
              style={{
                top: `${popupPosition.top}px`,
                left: `${popupPosition.left}px`,
              }}
            >
              <div className="flex items-center justify-between mb-2 pb-2 border-b border-border">
                <span className="text-xs font-semibold text-foreground">
                  {valueType === 'array' ? 'Array' : 'Object'} ({getCountText()})
                </span>
                <button
                  onClick={handleCopy}
                  className="flex items-center gap-1 px-2 py-1 text-xs hover:bg-muted rounded transition-colors"
                >
                  <Copy className="h-3 w-3" />
                  {copySuccess ? 'Copied!' : 'Copy'}
                </button>
              </div>
              <div className="text-xs font-mono whitespace-pre">
                {formatJson(processedValue, 0)}
              </div>
            </div>
          </>,
          document.body
        )}
      </>
    );
  }

  // Fallback for any other types
  return <span className={className}>{String(processedValue)}</span>;
}

