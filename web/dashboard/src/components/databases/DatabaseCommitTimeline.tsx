'use client';

import { useState } from 'react';
import { ChevronDown, ChevronRight, GitCommit } from 'lucide-react';
import Link from 'next/link';
import { formatCommitDate } from '@/lib/formatters';
import type { CommitTimelineEntry } from '@/lib/api/types';

interface DatabaseCommitTimelineProps {
  commitTimeline: CommitTimelineEntry[];
  workspaceId: string;
  repoName?: string;
  branchName?: string;
}

export function DatabaseCommitTimeline({
  commitTimeline,
  workspaceId,
  repoName,
  branchName,
}: DatabaseCommitTimelineProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (!commitTimeline || commitTimeline.length === 0) {
    return null;
  }

  // Sort commits by date, oldest first for left-to-right timeline
  const sortedCommits = [...commitTimeline].sort((a, b) => {
    if (!a.commit_date || !b.commit_date) return 0;
    return new Date(a.commit_date).getTime() - new Date(b.commit_date).getTime();
  });

  // Show only last 5 commits
  const visibleCommits = sortedCommits.slice(-5);
  const hiddenCount = Math.max(0, sortedCommits.length - 5);

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-4 hover:bg-accent/50 transition-colors"
      >
        <div className="flex items-center gap-3">
          {isExpanded ? (
            <ChevronDown className="h-5 w-5 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-5 w-5 text-muted-foreground" />
          )}
          <div className="flex items-center gap-2">
            <GitCommit className="h-5 w-5 text-primary" />
            <h3 className="text-lg font-semibold text-foreground">
              Schema History
            </h3>
            <span className="text-sm text-muted-foreground">
              ({commitTimeline.length} {commitTimeline.length === 1 ? 'commit' : 'commits'})
            </span>
          </div>
        </div>
      </button>

      {/* Timeline Content */}
      {isExpanded && (
        <div className="border-t border-border p-6 bg-gradient-to-b from-accent/20 to-transparent">
          <div className="relative overflow-x-auto">
            <div className="min-w-full w-fit">
              {/* Timeline Container */}
              <div className="relative pt-8 pb-8">
                {/* Horizontal Line */}
                <div className="absolute left-0 right-0 top-[40px] h-0.5 bg-border -translate-y-1/2" />

                {/* Commits */}
                <div className="flex items-start justify-between gap-4 relative w-full">
                  {/* Hidden Commits Indicator */}
                  {hiddenCount > 0 && (
                    <div className="flex flex-col items-center gap-2 min-w-[80px] relative">
                      <div className="relative z-10 h-4 flex items-center justify-center">
                        <div className="bg-muted text-muted-foreground text-xs font-medium px-2 py-0.5 rounded-full border border-border">
                          +{hiddenCount} more
                        </div>
                      </div>
                       {/* Placeholder for alignment with dates */}
                       <div className="h-[60px]" />
                    </div>
                  )}

                  {visibleCommits.map((commit) => {
                    const isHead = commit.is_head;
                    const commitUrl =
                      repoName && branchName
                        ? `/workspaces/${workspaceId}/repositories/${encodeURIComponent(
                            repoName
                          )}/branches/${encodeURIComponent(branchName)}/commits/${encodeURIComponent(
                            commit.commit_code
                          )}`
                        : undefined;

                    const dotContent = (
                      <div className="flex flex-col items-center gap-2 min-w-[200px]">
                        {/* Dot container with fixed height for alignment */}
                        <div className="h-4 flex items-center justify-center relative z-10">
                           <div
                            className={`rounded-full transition-all duration-200 ${
                              isHead
                                ? 'w-5 h-5 bg-primary border-4 border-background shadow-[0_0_0_2px_hsl(var(--primary))] scale-110'
                                : 'w-3 h-3 bg-background border-2 border-primary/50 hover:border-primary hover:scale-125'
                            }`}
                          />
                          {isHead && (
                            <div className="absolute -top-8 left-1/2 -translate-x-1/2 whitespace-nowrap">
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-primary text-primary-foreground shadow-md">
                                HEAD
                              </span>
                            </div>
                          )}
                        </div>

                        {/* Commit Info */}
                        <div className="flex flex-col items-center gap-1 mt-4">
                          <span
                            className={`font-mono text-xs font-semibold ${
                              isHead ? 'text-primary' : 'text-foreground'
                            }`}
                          >
                            {commit.commit_code}
                          </span>
                          <span className="text-xs text-muted-foreground text-center">
                            {formatCommitDate(commit.commit_date)}
                          </span>
                          {commit.commit_message && (
                            <span className="text-xs text-muted-foreground text-center line-clamp-2 max-w-[120px] break-words" title={commit.commit_message}>
                              {commit.commit_message}
                            </span>
                          )}
                        </div>
                      </div>
                    );

                    return commitUrl ? (
                      <Link
                        key={commit.commit_code}
                        href={commitUrl}
                        className="hover:opacity-80 transition-opacity"
                      >
                        {dotContent}
                      </Link>
                    ) : (
                      <div key={commit.commit_code}>{dotContent}</div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
