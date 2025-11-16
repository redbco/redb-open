'use client';

import { useState } from 'react';
import { useConnectStream } from '@/lib/hooks/useStreams';
import { useToast } from '@/components/ui/Toast';
import { X } from 'lucide-react';

interface ConnectStreamDialogProps {
  workspaceName: string;
  onClose: () => void;
  onSuccess?: () => void;
}

const STREAM_PLATFORMS = [
  { value: 'kafka', label: 'Apache Kafka' },
  { value: 'redpanda', label: 'Redpanda (Kafka Compatible)' },
  { value: 'kinesis', label: 'AWS Kinesis' },
  { value: 'pubsub', label: 'Google Cloud Pub/Sub' },
  { value: 'eventhubs', label: 'Azure Event Hubs' },
  { value: 'mqtt', label: 'MQTT Client (Connect to Broker)' },
  { value: 'mqtt_server', label: 'MQTT Broker (Run as Server)' },
];

export function ConnectStreamDialog({ workspaceName, onClose, onSuccess }: ConnectStreamDialogProps) {
  const { connect, isLoading } = useConnectStream(workspaceName);
  const { showToast } = useToast();
  
  const [streamName, setStreamName] = useState('');
  const [description, setDescription] = useState('');
  const [platform, setPlatform] = useState('kafka');
  const [brokers, setBrokers] = useState('');
  const [region, setRegion] = useState('');
  const [topics, setTopics] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [tlsEnabled, setTlsEnabled] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!streamName) {
      showToast({
        type: 'error',
        title: 'Validation Error',
        message: 'Stream name is required',
      });
      return;
    }

    // Build connection config based on platform
    const connectionConfig: Record<string, any> = {};
    
    if (platform === 'kafka' || platform === 'redpanda') {
      if (!brokers) {
        showToast({
          type: 'error',
          title: 'Validation Error',
          message: 'Brokers are required for Kafka/Redpanda',
        });
        return;
      }
      connectionConfig.brokers = brokers.split(',').map(b => b.trim());
      if (username) {
        connectionConfig.username = username;
        connectionConfig.password = password;
      }
      if (tlsEnabled) {
        connectionConfig.tls_enabled = true;
      }
    } else if (platform === 'kinesis') {
      if (!region) {
        showToast({
          type: 'error',
          title: 'Validation Error',
          message: 'Region is required for AWS Kinesis',
        });
        return;
      }
      connectionConfig.region = region;
    } else if (platform === 'pubsub') {
      if (!region) {
        showToast({
          type: 'error',
          title: 'Validation Error',
          message: 'Project ID is required for Google Cloud Pub/Sub',
        });
        return;
      }
      connectionConfig.project_id = region;
    } else if (platform === 'eventhubs') {
      if (!brokers) {
        showToast({
          type: 'error',
          title: 'Validation Error',
          message: 'Namespace is required for Azure Event Hubs',
        });
        return;
      }
      connectionConfig.namespace = brokers;
    } else if (platform === 'mqtt') {
      if (!brokers) {
        showToast({
          type: 'error',
          title: 'Validation Error',
          message: 'Broker URL is required for MQTT client',
        });
        return;
      }
      connectionConfig.broker_url = brokers;
      if (username) {
        connectionConfig.username = username;
        connectionConfig.password = password;
      }
    } else if (platform === 'mqtt_server') {
      // For MQTT server, bind address and port are optional
      if (region) {
        connectionConfig.bind_address = region;
      }
      if (brokers) {
        connectionConfig.port = brokers;
      }
    }

    const monitoredTopics = topics ? topics.split(',').map(t => t.trim()) : [];

    try {
      await connect({
        stream_name: streamName,
        stream_description: description,
        stream_platform: platform,
        connection_config: connectionConfig,
        monitored_topics: monitoredTopics,
      });

      showToast({
        type: 'success',
        title: 'Stream Connected',
        message: `Successfully connected to ${streamName}`,
      });

      if (onSuccess) {
        onSuccess();
      }
      onClose();
    } catch (error) {
      showToast({
        type: 'error',
        title: 'Connection Failed',
        message: error instanceof Error ? error.message : 'Failed to connect stream',
      });
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-card border border-border rounded-lg shadow-lg w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="sticky top-0 bg-card border-b border-border px-6 py-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold text-foreground">Connect Stream</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium text-foreground mb-1">
              Stream Name *
            </label>
            <input
              type="text"
              value={streamName}
              onChange={(e) => setStreamName(e.target.value)}
              className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="my-stream"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-1">
              Description
            </label>
            <input
              type="text"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="Production Kafka cluster"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-foreground mb-1">
              Platform *
            </label>
            <select
              value={platform}
              onChange={(e) => setPlatform(e.target.value)}
              className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
              required
            >
              {STREAM_PLATFORMS.map((p) => (
                <option key={p.value} value={p.value}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>

          {(platform === 'kafka' || platform === 'redpanda') && (
            <>
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Brokers * (comma-separated)
                </label>
                <input
                  type="text"
                  value={brokers}
                  onChange={(e) => setBrokers(e.target.value)}
                  className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  placeholder="kafka1:9092,kafka2:9092"
                  required
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-foreground mb-1">
                    Username (optional)
                  </label>
                  <input
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-1">
                    Password (optional)
                  </label>
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  />
                </div>
              </div>

              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="tls"
                  checked={tlsEnabled}
                  onChange={(e) => setTlsEnabled(e.target.checked)}
                  className="mr-2"
                />
                <label htmlFor="tls" className="text-sm text-foreground">
                  Enable TLS
                </label>
              </div>
            </>
          )}

          {platform === 'kinesis' && (
            <div>
              <label className="block text-sm font-medium text-foreground mb-1">
                AWS Region *
              </label>
              <input
                type="text"
                value={region}
                onChange={(e) => setRegion(e.target.value)}
                className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="us-east-1"
                required
              />
            </div>
          )}

          {platform === 'pubsub' && (
            <div>
              <label className="block text-sm font-medium text-foreground mb-1">
                GCP Project ID *
              </label>
              <input
                type="text"
                value={region}
                onChange={(e) => setRegion(e.target.value)}
                className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="my-project-id"
                required
              />
            </div>
          )}

          {platform === 'eventhubs' && (
            <div>
              <label className="block text-sm font-medium text-foreground mb-1">
                Event Hubs Namespace *
              </label>
              <input
                type="text"
                value={brokers}
                onChange={(e) => setBrokers(e.target.value)}
                className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                placeholder="my-namespace"
                required
              />
            </div>
          )}

          {platform === 'mqtt' && (
            <>
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Broker URL * (e.g., tcp://broker.hivemq.com:1883)
                </label>
                <input
                  type="text"
                  value={brokers}
                  onChange={(e) => setBrokers(e.target.value)}
                  className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  placeholder="tcp://localhost:1883"
                  required
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-foreground mb-1">
                    Username (optional)
                  </label>
                  <input
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-foreground mb-1">
                    Password (optional)
                  </label>
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  />
                </div>
              </div>
            </>
          )}

          {platform === 'mqtt_server' && (
            <>
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Bind Address (optional, default: 0.0.0.0)
                </label>
                <input
                  type="text"
                  value={region}
                  onChange={(e) => setRegion(e.target.value)}
                  className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  placeholder="0.0.0.0"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Port (optional, default: 1883)
                </label>
                <input
                  type="text"
                  value={brokers}
                  onChange={(e) => setBrokers(e.target.value)}
                  className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
                  placeholder="1883"
                />
              </div>
            </>
          )}

          <div>
            <label className="block text-sm font-medium text-foreground mb-1">
              Topics to Monitor (comma-separated, optional)
            </label>
            <input
              type="text"
              value={topics}
              onChange={(e) => setTopics(e.target.value)}
              className="w-full px-3 py-2 border border-input bg-background rounded-md focus:outline-none focus:ring-2 focus:ring-primary"
              placeholder="orders,payments,inventory"
            />
          </div>

          <div className="flex items-center justify-end space-x-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-input bg-background rounded-md hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading}
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50"
            >
              {isLoading ? 'Connecting...' : 'Connect Stream'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

