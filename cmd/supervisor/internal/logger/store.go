package logger

import (
	"context"
	"sync"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
)

type Store struct {
	mu            sync.RWMutex
	entries       []*commonv1.LogEntry
	retentionDays int
	maxEntries    int
}

func NewStore(retentionDays int) *Store {
	return &Store{
		entries:       make([]*commonv1.LogEntry, 0),
		retentionDays: retentionDays,
		maxEntries:    100000, // Configurable limit
	}
}

func (s *Store) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanup()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) Store(entry *commonv1.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, entry)

	// Trim if over limit
	if len(s.entries) > s.maxEntries {
		s.entries = s.entries[len(s.entries)-s.maxEntries:]
	}
}

func (s *Store) Query(serviceName string, level commonv1.LogLevel, since time.Time, limit int) []*commonv1.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*commonv1.LogEntry

	for i := len(s.entries) - 1; i >= 0 && len(results) < limit; i-- {
		entry := s.entries[i]

		// Apply filters
		if serviceName != "" && entry.Service.Name != serviceName {
			continue
		}

		if level != commonv1.LogLevel_LOG_LEVEL_UNSPECIFIED && entry.Level < level {
			continue
		}

		if !since.IsZero() && entry.Timestamp.AsTime().Before(since) {
			break
		}

		results = append(results, entry)
	}

	// Reverse to get chronological order
	for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
		results[i], results[j] = results[j], results[i]
	}

	return results
}

func (s *Store) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.retentionDays <= 0 {
		return
	}

	cutoff := time.Now().AddDate(0, 0, -s.retentionDays)

	// Find first entry to keep
	keepFrom := 0
	for i, entry := range s.entries {
		if entry.Timestamp.AsTime().After(cutoff) {
			keepFrom = i
			break
		}
	}

	if keepFrom > 0 {
		s.entries = s.entries[keepFrom:]
	}
}
