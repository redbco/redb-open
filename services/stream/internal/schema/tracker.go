package schema

import (
	"context"
	"time"
)

// Tracker tracks schema evolution over time
type Tracker struct {
	discoverer *Discoverer
}

// NewTracker creates a new schema tracker
func NewTracker(discoverer *Discoverer) *Tracker {
	return &Tracker{
		discoverer: discoverer,
	}
}

// StartTracking begins tracking schema changes for a topic
func (t *Tracker) StartTracking(ctx context.Context, streamID, topic string) {
	// Start a goroutine to periodically check and update schemas
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Check if schema needs updating
				schema, exists := t.discoverer.GetSchema(streamID, topic)
				if exists && schema.MessagesSampled > 0 {
					// Update resource registry if confidence is high enough
					if schema.Confidence > 0.8 {
						t.discoverer.updateResourceRegistry(context.Background(), schema)
					}
				}
			}
		}
	}()
}
