'use client';

import { ReactNode, SelectHTMLAttributes, createContext, useContext, useState } from 'react';
import { ChevronDown } from 'lucide-react';

interface SelectContextValue {
  value: string;
  onChange: (value: string) => void;
}

const SelectContext = createContext<SelectContextValue | undefined>(undefined);

interface SelectProps {
  children: ReactNode;
  value: string;
  onValueChange: (value: string) => void;
}

export function Select({ children, value, onValueChange }: SelectProps) {
  return (
    <SelectContext.Provider value={{ value, onChange: onValueChange }}>
      <div className="relative inline-block">
        {children}
      </div>
    </SelectContext.Provider>
  );
}

interface SelectTriggerProps {
  children: ReactNode;
  className?: string;
}

export function SelectTrigger({ children, className = '' }: SelectTriggerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const context = useContext(SelectContext);

  if (!context) {
    throw new Error('SelectTrigger must be used within a Select');
  }

  return (
    <>
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className={`inline-flex items-center justify-between rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 ${className}`}
      >
        {children}
        <ChevronDown className="h-4 w-4 ml-2 opacity-50" />
      </button>
      {isOpen && (
        <>
          <div 
            className="fixed inset-0 z-40" 
            onClick={() => setIsOpen(false)}
          />
          <SelectContentInternal onClose={() => setIsOpen(false)} />
        </>
      )}
    </>
  );
}

interface SelectValueProps {
  placeholder?: string;
}

export function SelectValue({ placeholder }: SelectValueProps) {
  const context = useContext(SelectContext);

  if (!context) {
    throw new Error('SelectValue must be used within a Select');
  }

  return <span>{context.value || placeholder}</span>;
}

interface SelectContentProps {
  children: ReactNode;
}

let selectContent: ReactNode = null;

export function SelectContent({ children }: SelectContentProps) {
  selectContent = children;
  return null;
}

function SelectContentInternal({ onClose }: { onClose: () => void }) {
  return (
    <div className="absolute z-50 mt-1 min-w-full overflow-auto rounded-md border border-input bg-popover text-popover-foreground shadow-md">
      <div className="p-1">
        {selectContent}
      </div>
    </div>
  );
}

interface SelectItemProps {
  children: ReactNode;
  value: string;
}

export function SelectItem({ children, value }: SelectItemProps) {
  const context = useContext(SelectContext);

  if (!context) {
    throw new Error('SelectItem must be used within a Select');
  }

  return (
    <div
      onClick={() => {
        context.onChange(value);
      }}
      className={`relative flex cursor-pointer select-none items-center rounded-sm px-2 py-1.5 text-sm outline-none hover:bg-accent hover:text-accent-foreground ${
        context.value === value ? 'bg-accent' : ''
      }`}
    >
      {children}
    </div>
  );
}

