'use client';

import { ReactNode, useState, createContext, useContext, useEffect, useRef } from 'react';

interface TooltipContextValue {
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
  triggerRef: React.RefObject<HTMLDivElement | null> | null;
}

const TooltipContext = createContext<TooltipContextValue | undefined>(undefined);

export function TooltipProvider({ children }: { children: ReactNode }) {
  return <>{children}</>;
}

export function Tooltip({ children }: { children: ReactNode }) {
  const [isOpen, setIsOpen] = useState(false);
  const triggerRef = useRef<HTMLDivElement>(null);

  return (
    <TooltipContext.Provider value={{ isOpen, setIsOpen, triggerRef }}>
      <div className="relative inline-block">
        {children}
      </div>
    </TooltipContext.Provider>
  );
}

interface TooltipTriggerProps {
  children: ReactNode;
  asChild?: boolean;
}

export function TooltipTrigger({ children, asChild }: TooltipTriggerProps) {
  const context = useContext(TooltipContext);

  if (!context) {
    throw new Error('TooltipTrigger must be used within a Tooltip');
  }

  const handleMouseEnter = () => context.setIsOpen(true);
  const handleMouseLeave = () => context.setIsOpen(false);

  if (asChild) {
    // Clone the child and add event handlers
    const child = children as any;
    return (
      <div
        ref={context.triggerRef}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {child}
      </div>
    );
  }

  return (
    <div
      ref={context.triggerRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      {children}
    </div>
  );
}

export function TooltipContent({ children }: { children: ReactNode }) {
  const context = useContext(TooltipContext);
  const [position, setPosition] = useState({ top: 0, left: 0 });

  if (!context) {
    throw new Error('TooltipContent must be used within a Tooltip');
  }

  useEffect(() => {
    if (context.isOpen && context.triggerRef?.current) {
      const rect = context.triggerRef.current.getBoundingClientRect();
      setPosition({
        top: rect.top - 8, // 8px above trigger
        left: rect.left + rect.width / 2, // Center horizontally
      });
    }
  }, [context.isOpen, context.triggerRef]);

  if (!context.isOpen) {
    return null;
  }

  return (
    <div 
      className="fixed z-50 px-3 py-1.5 text-sm text-white bg-gray-900 dark:bg-gray-800 rounded-md shadow-lg whitespace-nowrap pointer-events-none transform -translate-x-1/2 -translate-y-full"
      style={{
        top: `${position.top}px`,
        left: `${position.left}px`,
      }}
    >
      {children}
      <div className="absolute top-full left-1/2 transform -translate-x-1/2 -mt-1">
        <div className="border-4 border-transparent border-t-gray-900 dark:border-t-gray-800"></div>
      </div>
    </div>
  );
}

