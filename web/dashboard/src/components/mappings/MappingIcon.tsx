import React from 'react';
import { ArrowRightLeft, Database, Table } from 'lucide-react';

interface MappingIconProps {
    type?: string;
    className?: string;
}

export function MappingIcon({ type, className = "w-6 h-6" }: MappingIconProps) {
    const normalizedType = type?.toLowerCase().trim() || '';

    switch (normalizedType) {
        case 'table':
            return <Table className={className} />;
        case 'database':
            return <Database className={className} />;
        default:
            return <ArrowRightLeft className={className} />;
    }
}

