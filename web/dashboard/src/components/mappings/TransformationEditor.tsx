'use client';

import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { ChevronDown, Check, Sparkles } from 'lucide-react';
import type { Transformation } from '@/lib/api/types';

interface TransformationEditorProps {
  currentTransformation: string | null;
  transformations: Transformation[];
  onSave: (transformationName: string) => Promise<void>;
  disabled?: boolean;
}

export function TransformationEditor({
  currentTransformation,
  transformations,
  onSave,
  disabled = false,
}: TransformationEditorProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [dropdownPosition, setDropdownPosition] = useState<{ top: number; left: number; width: number; openUpward: boolean } | null>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Calculate dropdown position
  useEffect(() => {
    if (isOpen && buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      const dropdownHeight = 384; // max-h-96 = 24rem = 384px
      const viewportHeight = window.innerHeight;
      const spaceBelow = viewportHeight - rect.bottom;
      const spaceAbove = rect.top;
      
      // Determine if we should open upward or downward
      const openUpward = spaceBelow < dropdownHeight && spaceAbove > spaceBelow;
      
      setDropdownPosition({
        top: openUpward 
          ? rect.top + window.scrollY - dropdownHeight - 4  // Position above button
          : rect.bottom + window.scrollY + 4,                // Position below button
        left: rect.left + window.scrollX,
        width: 288, // w-72 = 18rem = 288px
        openUpward,
      });
    }
  }, [isOpen]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current && 
        !dropdownRef.current.contains(event.target as Node) &&
        buttonRef.current &&
        !buttonRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [isOpen]);

  const handleSelect = async (transformationName: string) => {
    if (transformationName === currentTransformation) {
      setIsOpen(false);
      return;
    }

    try {
      setIsLoading(true);
      await onSave(transformationName);
      setIsOpen(false);
    } catch (error) {
      console.error('Error saving transformation:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const getCurrentTransformationLabel = () => {
    if (!currentTransformation || currentTransformation === 'direct_mapping') {
      return 'No transformation';
    }

    const transformation = transformations.find(
      (t) => t.transformation_name === currentTransformation
    );
    return transformation?.transformation_name || currentTransformation;
  };

  const getCurrentTransformation = () => {
    if (!currentTransformation || currentTransformation === 'direct_mapping') {
      return null;
    }

    return transformations.find((t) => t.transformation_name === currentTransformation);
  };

  const currentTransformationObj = getCurrentTransformation();

  return (
    <>
      <div className="relative inline-block">
        <button
          ref={buttonRef}
          onClick={() => !disabled && setIsOpen(!isOpen)}
          disabled={disabled || isLoading}
          className={`inline-flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium rounded-md transition-colors ${
            currentTransformation && currentTransformation !== 'direct_mapping'
              ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/40'
              : 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700'
          } ${disabled || isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
          title={currentTransformationObj?.transformation_description || 'Select transformation'}
        >
          <Sparkles className="h-3 w-3" />
          <span className="max-w-[120px] truncate">{getCurrentTransformationLabel()}</span>
          <ChevronDown className="h-3 w-3" />
        </button>
      </div>

      {isOpen && dropdownPosition && typeof document !== 'undefined' && createPortal(
        <div 
          ref={dropdownRef}
          className="fixed bg-white dark:bg-gray-800 border border-border rounded-lg shadow-lg z-[100] max-h-96 overflow-y-auto"
          style={{
            top: `${dropdownPosition.top}px`,
            left: `${dropdownPosition.left}px`,
            width: `${dropdownPosition.width}px`,
          }}
        >
          {/* No transformation option */}
          <button
            onClick={() => handleSelect('direct_mapping')}
            className={`w-full text-left px-3 py-2 text-sm hover:bg-accent transition-colors flex items-center justify-between ${
              !currentTransformation || currentTransformation === 'direct_mapping' ? 'bg-accent/50' : ''
            }`}
          >
            <div>
              <div className="font-medium text-foreground">No transformation</div>
              <div className="text-xs text-muted-foreground">Direct mapping (no changes)</div>
            </div>
            {(!currentTransformation || currentTransformation === 'direct_mapping') && (
              <Check className="h-4 w-4 text-primary" />
            )}
          </button>

          <div className="border-t border-border my-1"></div>

          {/* Transformation options */}
          {transformations.length === 0 ? (
            <div className="px-3 py-8 text-center text-sm text-muted-foreground">
              No transformations available
            </div>
          ) : (
            transformations
              .filter((t) => t.transformation_name !== 'direct_mapping')
              .map((transformation) => (
              <button
                key={transformation.transformation_id}
                onClick={() => handleSelect(transformation.transformation_name)}
                className={`w-full text-left px-3 py-2 text-sm hover:bg-accent transition-colors ${
                  currentTransformation === transformation.transformation_name ? 'bg-accent/50' : ''
                }`}
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-foreground flex items-center gap-1.5">
                      {transformation.transformation_name}
                      {transformation.transformation_builtin && (
                        <span className="px-1.5 py-0.5 text-[10px] font-medium bg-primary/10 text-primary rounded">
                          BUILTIN
                        </span>
                      )}
                    </div>
                    {transformation.transformation_description && (
                      <div className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
                        {transformation.transformation_description}
                      </div>
                    )}
                    {transformation.transformation_type && (
                      <div className="text-[10px] text-muted-foreground mt-1">
                        Type: {transformation.transformation_type}
                      </div>
                    )}
                  </div>
                  {currentTransformation === transformation.transformation_name && (
                    <Check className="h-4 w-4 text-primary flex-shrink-0 mt-0.5" />
                  )}
                </div>
              </button>
            ))
          )}
        </div>,
        document.body
      )}
    </>
  );
}

/**
 * Read-only transformation badge (no editing)
 */
export function TransformationBadge({
  transformationName,
  transformations,
}: {
  transformationName: string | null;
  transformations: Transformation[];
}) {
  if (!transformationName || transformationName === 'direct_mapping') {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400 rounded-md">
        <Sparkles className="h-3 w-3" />
        Direct
      </span>
    );
  }

  const transformation = transformations.find((t) => t.transformation_name === transformationName);

  return (
    <span
      className="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 rounded-md"
      title={transformation?.transformation_description || transformationName}
    >
      <Sparkles className="h-3 w-3" />
      <span className="max-w-[120px] truncate">{transformationName}</span>
    </span>
  );
}

