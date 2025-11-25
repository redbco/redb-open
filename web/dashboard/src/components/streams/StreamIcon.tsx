import React from 'react';
import { Activity, Radio, Zap, MessageSquare, Server, Cloud } from 'lucide-react';

interface StreamIconProps {
    platform: string;
    className?: string;
}

export const StreamIcon: React.FC<StreamIconProps> = ({ platform, className = "w-6 h-6" }) => {
    const p = platform?.toLowerCase() || '';

    // Apache Kafka
    if (p.includes('kafka') || p.includes('confluent') || p.includes('redpanda')) {
        return (
            <svg className={className} viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path d="M11.7 1.3c-.4-.2-.8-.2-1.2 0C6.6 3.4 3.4 6.6 1.3 10.5c-.2.4-.2.8 0 1.2 2.1 3.9 5.3 7.1 9.2 9.2.4.2.8.2 1.2 0 3.9-2.1 7.1-5.3 9.2-9.2.2-.4.2-.8 0-1.2-2.1-3.9-5.3-7.1-9.2-9.2zM12 18c-3.3 0-6-2.7-6-6s2.7-6 6-6 6 2.7 6 6-2.7 6-6 6z" fillOpacity="0.2" />
                <path d="M12 6c-3.3 0-6 2.7-6 6s2.7 6 6 6 6-2.7 6-6-2.7-6-6-6zm0 10c-2.2 0-4-1.8-4-4s1.8-4 4-4 4 1.8 4 4-1.8 4-4 4z" />
            </svg>
        );
    }

    // AWS Kinesis
    if (p.includes('kinesis')) {
        return (
            <svg className={className} viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path d="M18.5 4.5L5.5 4.5C4.4 4.5 3.5 5.4 3.5 6.5L3.5 17.5C3.5 18.6 4.4 19.5 5.5 19.5L18.5 19.5C19.6 19.5 20.5 18.6 20.5 17.5L20.5 6.5C20.5 5.4 19.6 4.5 18.5 4.5ZM10.5 15.5L7.5 15.5L7.5 8.5L10.5 8.5L10.5 15.5ZM16.5 15.5L13.5 15.5L13.5 8.5L16.5 8.5L16.5 15.5Z" />
            </svg>
        );
    }

    // GCP Pub/Sub
    if (p.includes('pubsub')) {
        return (
            <svg className={className} viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path d="M4 6h16v2H4zm2 4h12v2H6zm2 4h8v2H8zm2 4h4v2h-4z" />
            </svg>
        );
    }

    // RabbitMQ
    if (p.includes('rabbitmq')) {
        return (
            <svg className={className} viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z" />
            </svg>
        );
    }

    // Azure Event Hubs
    if (p.includes('azure') || p.includes('event')) {
        return <Cloud className={className} />;
    }

    // MQTT
    if (p.includes('mqtt')) {
        return <Radio className={className} />;
    }

    // NATS
    if (p.includes('nats')) {
        return <Zap className={className} />;
    }

    // SQS/SNS
    if (p.includes('sqs') || p.includes('sns')) {
        return <MessageSquare className={className} />;
    }

    // Default
    return <Activity className={className} />;
};
