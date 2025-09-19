'use client';

import { useState, useEffect } from 'react';
import { Building2 } from 'lucide-react';
import Image from 'next/image';

interface TenantBrandingProps {
  tenantId: string;
}

interface TenantInfo {
  name: string;
  logo?: string;
  primaryColor?: string;
  displayName?: string;
}

export function TenantBranding({ tenantId }: TenantBrandingProps) {
  const [tenantInfo, setTenantInfo] = useState<TenantInfo | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // TODO: Replace with actual API call to fetch tenant branding
    const fetchTenantInfo = async () => {
      setIsLoading(true);
      
      // Simulate API call
      setTimeout(() => {
        // Mock tenant data based on tenantId
        const mockTenantInfo: TenantInfo = {
          name: tenantId,
          displayName: tenantId.charAt(0).toUpperCase() + tenantId.slice(1),
          // You can add logo URLs and custom colors here
          primaryColor: '#3b82f6', // Default blue
        };
        
        setTenantInfo(mockTenantInfo);
        setIsLoading(false);
      }, 500);
    };

    fetchTenantInfo();
  }, [tenantId]);

  if (isLoading) {
    return (
      <div className="text-center mb-8">
        <div className="animate-pulse">
          <div className="w-16 h-16 bg-muted rounded-full mx-auto mb-4"></div>
          <div className="h-6 bg-muted rounded w-32 mx-auto"></div>
        </div>
      </div>
    );
  }

  if (!tenantInfo) {
    return (
      <div className="text-center mb-8">
        <div className="w-16 h-16 bg-muted rounded-full flex items-center justify-center mx-auto mb-4">
          <Building2 className="h-8 w-8 text-muted-foreground" />
        </div>
        <h2 className="text-xl font-semibold text-foreground">
          {tenantId}
        </h2>
      </div>
    );
  }

  return (
    <div className="text-center mb-8">
      {/* Tenant Logo */}
      <div className="mb-4">
        <Image
          src="/logo.svg"
          width={256}
          height={256}
          alt="reDB"
          className="w-32 h-32 mx-auto rounded-full object-cover border-2 border-border"
        />
      </div>

      {/* Tenant Name */}
      <h2 className="text-xl font-semibold text-foreground">
        {tenantInfo.displayName}
      </h2>
    </div>
  );
}
