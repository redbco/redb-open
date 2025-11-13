import { SystemLayout } from '@/components/layout/SystemLayout';

interface SystemLayoutWrapperProps {
  children: React.ReactNode;
}

export default function SystemLayoutWrapper({ children }: SystemLayoutWrapperProps) {
  return (
    <SystemLayout>
      {children}
    </SystemLayout>
  );
}

