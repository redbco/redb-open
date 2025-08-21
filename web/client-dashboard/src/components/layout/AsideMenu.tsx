'use client';

import { ReactNode } from 'react';

interface AsideMenuProps {
  children: ReactNode;
  className?: string;
}

export function AsideMenu({ children, className = '' }: AsideMenuProps) {
  return (
    <aside className={`fixed inset-y-0 left-20 hidden w-80 overflow-y-auto border-r border-border bg-card lg:block ${className}`}>
      <div className="px-4 py-4 sm:px-6 lg:px-6">
        {children}
      </div>
    </aside>
  );
}
