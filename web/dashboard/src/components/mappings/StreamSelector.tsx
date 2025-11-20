'use client';

import { useState, useEffect } from 'react';
import { ChevronDown } from 'lucide-react';
import { useStreams } from '@/lib/hooks/useResources';

interface StreamSelectorProps {
  workspaceId: string;
  onSelect: (selection: { stream: string; topic: string | null } | null) => void;
  value: { stream: string; topic: string | null } | null;
  allowTopicSelection?: boolean; // If false, only select stream
  label: string;
  disabled?: boolean;
  allowNew?: boolean; // If true, show "Create new stream" option
}

export function StreamSelector({
  workspaceId,
  onSelect,
  value,
  allowTopicSelection = true,
  label,
  disabled = false,
  allowNew = false,
}: StreamSelectorProps) {
  const [selectedStream, setSelectedStream] = useState<string>(value?.stream || '');
  const [selectedTopic, setSelectedTopic] = useState<string>(value?.topic || '');
  const [isCreatingNew, setIsCreatingNew] = useState(false);

  const { streams, isLoading: loadingStreams } = useStreams(workspaceId);

  useEffect(() => {
    if (isCreatingNew) {
      onSelect({ stream: '__new__', topic: null });
    } else if (selectedStream && (!allowTopicSelection || selectedTopic)) {
      onSelect({
        stream: selectedStream,
        topic: allowTopicSelection ? selectedTopic || null : null,
      });
    } else {
      onSelect(null);
    }
  }, [selectedStream, selectedTopic, isCreatingNew, allowTopicSelection, onSelect]);

  const handleStreamChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const streamName = e.target.value;
    if (streamName === '__new__') {
      setIsCreatingNew(true);
      setSelectedStream('');
      setSelectedTopic('');
    } else {
      setIsCreatingNew(false);
      setSelectedStream(streamName);
      setSelectedTopic(''); // Reset topic when stream changes
    }
  };

  if (isCreatingNew) {
    return (
      <div className="space-y-3">
        <div className="text-xs font-medium text-foreground mb-1.5">{label}</div>
        <div className="p-3 border border-primary/30 bg-primary/5 rounded-lg">
          <p className="text-xs text-foreground font-medium">Creating New Stream</p>
          <p className="text-xs text-muted-foreground mt-1">
            A new stream will be created based on your mapping configuration.
          </p>
          <button
            onClick={() => setIsCreatingNew(false)}
            className="text-xs text-primary hover:underline mt-2"
          >
            ← Select existing stream instead
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <div className="text-xs font-medium text-foreground mb-1.5">{label}</div>
      
      {/* Stream Selector */}
      <div>
        <label className="block text-xs text-muted-foreground mb-1.5">
          {allowTopicSelection ? '1. Select Stream' : 'Select Stream'}
        </label>
        <div className="relative">
          <select
            value={selectedStream}
            onChange={handleStreamChange}
            disabled={disabled || loadingStreams}
            className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background appearance-none cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary"
          >
            <option value="">Choose a stream...</option>
            {allowNew && <option value="__new__">+ Create New Stream</option>}
            {streams.map((stream) => (
              <option key={stream.stream_id} value={stream.stream_name}>
                {stream.stream_name} ({stream.stream_platform})
              </option>
            ))}
          </select>
          <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
        </div>
      </div>

      {/* Topic Selector (Placeholder - would need topic API) */}
      {allowTopicSelection && selectedStream && (
        <div>
          <label className="block text-xs text-muted-foreground mb-1.5">
            2. Select Topic
          </label>
          <input
            type="text"
            value={selectedTopic}
            onChange={(e) => setSelectedTopic(e.target.value)}
            placeholder="Enter topic name..."
            disabled={disabled}
            className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-primary"
          />
          <p className="text-xs text-muted-foreground mt-1">
            Topic must exist in the selected stream
          </p>
        </div>
      )}
    </div>
  );
}

