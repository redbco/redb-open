package watcher

// ConfigWatcher stub - monitors stream configuration changes
import (
	"context"
	"time"

	"github.com/redbco/redb-open/services/stream/internal/config"
)

type ConfigWatcher struct{ repository *config.Repository }

func NewConfigWatcher(repo *config.Repository) *ConfigWatcher {
	return &ConfigWatcher{repository: repo}
}
func (w *ConfigWatcher) Start(ctx context.Context) { go w.watch(ctx) }
func (w *ConfigWatcher) watch(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
