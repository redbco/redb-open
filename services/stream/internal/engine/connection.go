package engine

import (
	"context"
	"fmt"

	"github.com/redbco/redb-open/pkg/stream/adapter"
	internalconfig "github.com/redbco/redb-open/services/stream/internal/config"
	"github.com/redbco/redb-open/services/stream/internal/state"
)

// connectToStream attempts to establish a connection to a stream
func (e *Engine) connectToStream(ctx context.Context, streamConfig *internalconfig.StreamConfig) {
	if e.logger != nil {
		e.logger.Infof("Attempting to connect to stream: %s (platform: %s)", streamConfig.Name, streamConfig.Platform)
	}

	// Convert to connection config
	connectionConfig := streamConfig.ToConnectionConfig()

	// Get the appropriate adapter
	streamAdapter, err := adapter.GetAdapter(connectionConfig.Platform)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to get adapter for stream %s: %v", streamConfig.ID, err)
		}
		return
	}

	// Establish connection
	conn, err := streamAdapter.Connect(ctx, *connectionConfig)
	if err != nil {
		if e.logger != nil {
			e.logger.Errorf("Failed to connect to stream %s: %v", streamConfig.ID, err)
		}

		// Update status to error
		globalState := state.GetInstance()
		if repo := globalState.GetConfigRepository(); repo != nil {
			repo.UpdateStreamConnectionStatus(ctx, streamConfig.ID, false, fmt.Sprintf("Connection failed: %v", err))
		}
		return
	}

	// Store connection in state
	globalState := state.GetInstance()
	globalState.AddConnection(streamConfig.ID, conn)

	// Update connection status
	if repo := globalState.GetConfigRepository(); repo != nil {
		err = repo.UpdateStreamConnectionStatus(ctx, streamConfig.ID, true, "Connected successfully")
		if err != nil && e.logger != nil {
			e.logger.Warnf("Failed to update stream status for %s: %v", streamConfig.ID, err)
		}
	}

	if e.logger != nil {
		e.logger.Infof("Successfully connected to stream: %s", streamConfig.Name)
	}
}
